#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Presto <-> Spark SQL Bi-directional Transpiler Script
-----------------------------------------------------
Hỗ trợ 2 chiều:
- Presto / Trino -> Spark SQL
- Spark SQL -> Presto / Trino
"""

import sys
import os
import re
import argparse

# ==========================================
# 1. BẢO VỆ BIẾN THAM SỐ (JINJA/AIRFLOW/SPARK)
# ==========================================
def protect_variables(sql: str):
    """
    Bảo vệ các biến tham số dạng {{process_date}}, {{ ds }}, ${VAR}, v.v.
    trước khi chuyển vào parser SQL, tránh bị parser hiểu nhầm thành cú pháp struct/row.
    """
    vars_list = []
    pattern = r'(\{\{[\s\S]*?\}\}|\$\{[a-zA-Z0-9_.:\-]+\})'
    def repl(m):
        idx = len(vars_list)
        vars_list.append(m.group(0))
        return f"__TPV_VAR_{idx}__"
    return re.sub(pattern, repl, sql), vars_list

def restore_variables(sql: str, vars_list: list) -> str:
    """
    Khôi phục nguyên vẹn 100% các biến tham số sau khi convert xong.
    """
    if not vars_list:
        return sql
    def repl(m):
        idx = int(m.group(1))
        return vars_list[idx] if idx < len(vars_list) else m.group(0)
    return re.sub(r'(?i)__TPV_VAR_(\d+)__', repl, sql)


# ==========================================
# 2. XỬ LÝ ĐẶC THÙ THEO PHIÊN BẢN PRESTO / TRINO
# ==========================================
def post_process_presto_dialect(sql: str, dialect: str, original_spark_sql: str = "") -> str:
    """
    Xử lý tinh chỉnh đặc thù theo từng phiên bản Presto:
    1. 'presto' (PrestoDB 0.2xx / EMR Presto / Athena v2):
       - PrestoDB KHÔNG hỗ trợ correlated subquery trong LATERAL join:
         'CROSS JOIN LATERAL (SELECT ... FROM UNNEST(...) ...)' -> Gây lỗi trên PrestoDB!
       - Bắt buộc chuẩn hóa thành cú pháp UNNEST trực tiếp:
         'CROSS JOIN UNNEST(...) WITH ORDINALITY AS alias(val, pos)'
    2. 'athena' (AWS Athena Engine):
       - Tương thích tối đa cho cả Athena v2 (Presto 0.217) lẫn Athena v3 (Trino 406+):
         Ưu tiên cú pháp UNNEST trực tiếp không dùng lateral subquery để chạy an toàn trên mọi workgroup.
    3. 'trino' (Trino 330+ / Trino 400+):
       - Hỗ trợ đầy đủ cú pháp LATERAL subquery và chuẩn hóa hàm Trino (như date_format).
    """
    if dialect in ("presto", "athena"):
        # 1. Unwrap subquery bọc UNNEST trong CROSS JOIN LATERAL (trường hợp WITH ORDINALITY)
        pattern_ord = r'(?is)CROSS\s+JOIN\s+LATERAL\s*\(\s*SELECT\s+.*?\s+FROM\s+(UNNEST\s*\(.*?\)\s+WITH\s+ORDINALITY\s+AS\s+[a-zA-Z0-9_]+(?:\s*\([^)]+\))?)\s*\)'
        sql = re.sub(pattern_ord, r'CROSS JOIN \1', sql)

        # 2. Unwrap subquery bọc UNNEST trong CROSS JOIN LATERAL (trường hợp không có WITH ORDINALITY)
        pattern_plain = r'(?is)CROSS\s+JOIN\s+LATERAL\s*\(\s*SELECT\s+.*?\s+FROM\s+(UNNEST\s*\(.*?\)\s+AS\s+[a-zA-Z0-9_]+(?:\s*\([^)]+\))?)\s*\)'
        sql = re.sub(pattern_plain, r'CROSS JOIN \1', sql)

    # Đảm bảo thứ tự cột UNNEST WITH ORDINALITY: cột 1 là value, cột 2 là ordinality
    if original_spark_sql:
        m_spark = re.search(r'(?is)POSEXPLODE\s*\([^)]+\)\s*(?:[a-zA-Z0-9_]+\s+)?AS\s+([a-zA-Z0-9_]+)\s*,\s*([a-zA-Z0-9_]+)', original_spark_sql)
        if m_spark:
            pos_alias, val_alias = m_spark.group(1), m_spark.group(2)
            bad_pat = rf'(?i)(WITH\s+ORDINALITY\s+AS\s+[a-zA-Z0-9_]+\s*\()\s*{pos_alias}\s*,\s*{val_alias}\s*(\))'
            sql = re.sub(bad_pat, rf'\g<1>{val_alias}, {pos_alias}\2', sql)

    if dialect == "trino":
        sql = re.sub(r'(?i)\bformat_datetime\s*\(', 'date_format(', sql)

    return sql


# ==========================================
# 3. TRANSPILE BẰNG SQLGLOT (AST ENGINE)
# ==========================================
def transpile_with_sqlglot(sql_content: str, read_dialect: str, write_dialect: str, presto_dialect: str = "presto") -> str:
    try:
        import sqlglot
        transpiled = sqlglot.transpile(
            sql_content,
            read=read_dialect,
            write=write_dialect,
            pretty=True
        )
        res = ";\n\n".join(transpiled)
        if write_dialect in ("presto", "trino", "athena"):
            res = post_process_presto_dialect(res, presto_dialect, original_spark_sql=sql_content)
        return res
    except ImportError:
        return None
    except Exception as e:
        print(f"[Cảnh báo AST Engine]: Lỗi parse SQL ({e}), chuyển sang Regex Rule Engine...")
        return None


# ==========================================
# 4. RULE-BASED REGEX ENGINE (FALLBACK)
# ==========================================
class RegexRuleConverter:
    def __init__(self, mode="presto2spark", presto_dialect="presto"):
        self.mode = mode
        self.presto_dialect = presto_dialect or "presto"
        if mode == "presto2spark":
            self.rules = [
                # Lateral View Unnest -> Spark Explode
                (r'(?is)CROSS\s+JOIN\s+UNNEST\s*\(\s*([^)]+?)\s*\)\s+WITH\s+ORDINALITY\s+AS\s+([a-zA-Z0-9_]+)\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'LATERAL VIEW POSEXPLODE(\1) \2 AS \4, \3'),
                (r'(?is)CROSS\s+JOIN\s+UNNEST\s*\(\s*([^)]+?)\s*\)\s+AS\s+([a-zA-Z0-9_]+)\s*\(\s*([^)]+?)\s*\)', r'LATERAL VIEW EXPLODE(\1) \2 AS \3'),
                (r'(?is)CROSS\s+JOIN\s+LATERAL\s*\(\s*SELECT\s+.*?\s+FROM\s+UNNEST\s*\(\s*([^)]+?)\s*\)\s+WITH\s+ORDINALITY\s+AS\s+([a-zA-Z0-9_]+)\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)\s*\)', r'LATERAL VIEW POSEXPLODE(\1) \2 AS \4, \3'),
                # Types
                (r'(?i)\bVARCHAR\b', 'STRING'),
                (r'(?i)\bVARBINARY\b', 'BINARY'),
                (r'(?i)\bJSON\b', 'STRING'),
                # Dates
                (r'(?i)\bdate_parse\s*\(\s*([^,]+?)\s*,\s*[\'"]%Y-%m-%d[\'"]\s*\)', r'to_date(\1, "yyyy-MM-dd")'),
                (r'(?i)\bdate_parse\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'to_timestamp(\1, \2)'),
                (r'(?i)\b(?:format_datetime|date_format)\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'date_format(\1, \2)'),
                (r'(?i)\bnow\s*\(\s*\)', 'current_timestamp()'),
                (r'(?i)\bcurrent_date\b(?!\s*\()', 'current_date()'),
                (r'(?i)\bdate_add\s*\(\s*[\'"]day[\'"]\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'date_add(\2, \1)'),
                (r'(?i)\bdate_add\s*\(\s*[\'"]month[\'"]\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'add_months(\2, \1)'),
                (r'(?i)\bdate_diff\s*\(\s*[\'"]day[\'"]\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'datediff(\2, \1)'),
                # JSON
                (r'(?i)\bjson_extract_scalar\s*\(', 'get_json_object('),
                (r'(?i)\bjson_extract\s*\(', 'get_json_object('),
                # String
                (r'(?i)\bstrpos\s*\(', 'instr('),
                (r'(?i)\bcodepoint\s*\(', 'ascii('),
                (r'(?i)\bregexp_like\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'\1 RLIKE \2'),
                # Array/Map
                (r'(?i)\bcardinality\s*\(', 'size('),
                (r'(?i)\bcontains\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'array_contains(\1, \2)'),
                (r'(?i)\barray_join\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'concat_ws(\2, \1)'),
                # Aggregates
                (r'(?i)\bapprox_distinct\s*\(', 'approx_count_distinct('),
                (r'(?i)\barbitrary\s*\(', 'first('),
                # S3 URI
                (r's3://', 's3a://'),
            ]
        else:
            # spark2presto
            date_fmt_func = 'date_format' if self.presto_dialect == 'trino' else 'format_datetime'
            
            # Posexplode / Explode theo dialect
            if self.presto_dialect in ("presto", "athena"):
                # PrestoDB: Cú pháp UNNEST trực tiếp không dùng lateral subquery
                posexplode_rule = (r'(?is)LATERAL\s+VIEW\s+(?:OUTER\s+)?POSEXPLODE\s*\(\s*([^)]+?)\s*\)\s+([a-zA-Z0-9_]+)?\s*AS\s+([^,\s]+)\s*,\s*([^\s,;]+)', r'CROSS JOIN UNNEST(\1) WITH ORDINALITY AS \2(\4, \3)')
            else:
                # Trino: Hỗ trợ subquery trong LATERAL
                posexplode_rule = (r'(?is)LATERAL\s+VIEW\s+(?:OUTER\s+)?POSEXPLODE\s*\(\s*([^)]+?)\s*\)\s+([a-zA-Z0-9_]+)?\s*AS\s+([^,\s]+)\s*,\s*([^\s,;]+)', r'CROSS JOIN LATERAL (SELECT \3 - 1 AS \3, \4 FROM UNNEST(\1) WITH ORDINALITY AS \2(\4, \3))')

            self.rules = [
                posexplode_rule,
                (r'(?is)LATERAL\s+VIEW\s+(?:OUTER\s+)?EXPLODE\s*\(\s*([^)]+?)\s*\)\s+([a-zA-Z0-9_]+)?\s*AS\s+([^\s,;]+)', r'CROSS JOIN UNNEST(\1) AS \2(\3)'),
                # Types
                (r'(?i)\bSTRING\b', 'VARCHAR'),
                # Dates
                (r'(?i)\bto_date\s*\(\s*([^,]+?)\s*,\s*[\'"]yyyy-MM-dd[\'"]\s*\)', r'date_parse(\1, \'%Y-%m-%d\')'),
                (r'(?i)\bto_date\s*\(\s*([^,]+?)\s*\)', r'date_parse(\1, \'%Y-%m-%d\')'),
                (r'(?i)\bto_timestamp\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'date_parse(\1, \2)'),
                (r'(?i)\bdate_format\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', f'{date_fmt_func}(\\1, \\2)'),
                (r'(?i)\bcurrent_timestamp\s*\(\s*\)', 'now()'),
                (r'(?i)\bdate_add\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'date_add(\'day\', \2, \1)'),
                (r'(?i)\badd_months\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'date_add(\'month\', \2, \1)'),
                (r'(?i)\bdatediff\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'date_diff(\'day\', \2, \1)'),
                # JSON
                (r'(?i)\bget_json_object\s*\(', 'json_extract_scalar('),
                # String
                (r'(?i)\binstr\s*\(', 'strpos('),
                # Array/Map
                (r'(?i)\bsize\s*\(', 'cardinality('),
                (r'(?i)\barray_contains\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'contains(\1, \2)'),
                (r'(?i)\bconcat_ws\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'array_join(\2, \1)'),
                # Aggregates
                (r'(?i)\bapprox_count_distinct\s*\(', 'approx_distinct('),
                # S3 URI
                (r's3a://', 's3://'),
            ]

    def convert(self, sql: str) -> str:
        res = sql
        for pattern, replacement in self.rules:
            res = re.sub(pattern, replacement, res)
        # Dọn dẹp AS (val, pos) nếu thiếu table alias
        res = re.sub(r'AS\s+\(([^)]+)\)', r'AS _t0(\1)', res)
        return res


# ==========================================
# 5. PUBLIC API FUNCTIONS
# ==========================================
def convert_sql(sql_code: str, mode: str = "presto2spark", presto_dialect: str = "presto") -> str:
    """
    mode: 'presto2spark' hoac 'spark2presto'
    presto_dialect: 'presto' (PrestoDB 0.2xx), 'trino' (Trino 330+/400+), 'athena' (AWS Athena)
    """
    # 1. Bảo vệ các biến tham số (Jinja {{...}}, Shell ${...})
    protected_sql, vars_list = protect_variables(sql_code)

    if mode == "spark2presto":
        read_d = "spark"
        write_d = presto_dialect or "presto"
    else:
        read_d = presto_dialect or "presto"
        write_d = "spark"

    # 2. Transpile bằng AST Engine
    ast_result = transpile_with_sqlglot(protected_sql, read_d, write_d, presto_dialect=presto_dialect)
    if ast_result:
        return restore_variables(ast_result, vars_list)

    # 3. Fallback sang Regex Rule Engine
    converter = RegexRuleConverter(mode=mode, presto_dialect=presto_dialect)
    fallback_res = converter.convert(protected_sql)
    return restore_variables(fallback_res, vars_list)


def convert_presto_to_spark(sql_code: str, presto_dialect: str = "presto") -> str:
    return convert_sql(sql_code, mode="presto2spark", presto_dialect=presto_dialect)


def convert_spark_to_presto(sql_code: str, presto_dialect: str = "presto") -> str:
    return convert_sql(sql_code, mode="spark2presto", presto_dialect=presto_dialect)


def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    parser = argparse.ArgumentParser(description="Chuyen doi cu phap SQL 2 chieu: Presto <-> SparkSQL")
    parser.add_argument("-q", "--query", type=str, help="Cau lenh SQL can convert truc tiep")
    parser.add_argument("-m", "--mode", choices=["presto2spark", "spark2presto"], default="presto2spark", 
                        help="Chieu chuyen doi: presto2spark (mac dinh) hoac spark2presto")
    parser.add_argument("-p", "--presto-dialect", choices=["presto", "trino", "athena"], default="presto",
                        help="Phien ban Presto/Trino: presto (mac dinh PrestoDB 0.2xx), trino (Trino 330+/400+), athena (AWS Athena)")
    parser.add_argument("-f", "--file", type=str, help="Duong dan file .sql dau vao")
    parser.add_argument("-o", "--output", type=str, help="Duong dan file ket qua")
    parser.add_argument("-d", "--dir", type=str, help="Thu muc chua cac file .sql can convert")
    
    args = parser.parse_args()

    try:
        import sqlglot
        print(f"[Engine]: Dang su dung sqlglot (AST Full Parser) | Che do: {args.mode} | Presto Dialect: {args.presto_dialect}")
    except ImportError:
        print(f"[Engine]: Dang dung Regex Rule Engine fallback | Che do: {args.mode}")

    if args.query:
        converted = convert_sql(args.query, mode=args.mode, presto_dialect=args.presto_dialect)
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(converted)
            print(f"Da luu ket qua vao: {args.output}")
        else:
            print(f"\n--- KET QUA ({args.mode.upper()} - {args.presto_dialect.upper()}) ---")
            print(converted)

    elif args.file:
        if not os.path.exists(args.file):
            print(f"Loi: Khong tim thay file {args.file}")
            sys.exit(1)
        with open(args.file, 'r', encoding='utf-8') as f:
            content = f.read()
        converted = convert_sql(content, mode=args.mode, presto_dialect=args.presto_dialect)
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(converted)
            print(f"Da convert xong: {args.file} -> {args.output}")
        else:
            print(f"\n--- KET QUA ({args.mode.upper()} - {args.presto_dialect.upper()}) ---")
            print(converted)

    elif args.dir:
        if not os.path.exists(args.dir):
            print(f"Loi: Khong tim thay thu muc {args.dir}")
            sys.exit(1)
        out_dir = args.output or os.path.join(args.dir, f"{args.mode}_{args.presto_dialect}_converted")
        os.makedirs(out_dir, exist_ok=True)
        
        for root, _, files in os.walk(args.dir):
            for file in files:
                if file.endswith(('.sql', '.prc')):
                    in_path = os.path.join(root, file)
                    rel_path = os.path.relpath(in_path, args.dir)
                    target_path = os.path.join(out_dir, rel_path)
                    os.makedirs(os.path.dirname(target_path), exist_ok=True)
                    
                    with open(in_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    converted = convert_sql(content, mode=args.mode, presto_dialect=args.presto_dialect)
                    with open(target_path, 'w', encoding='utf-8') as f:
                        f.write(converted)
                    print(f"-> Converted: {rel_path}")
        print(f"\nHoan tat chuyen doi thu muc vao: {out_dir}")

    else:
        sample_spark = """SELECT 
    CAST(cust_id AS STRING) AS cust_id_str,
    GET_JSON_OBJECT(event_payload, '$.device') AS device_type,
    DATEDIFF(current_date(), TO_DATE(log_date, 'yyyy-MM-dd')) AS days_ago,
    SIZE(transaction_ids) AS total_txns,
    ARRAY_CONTAINS(transaction_ids, 'TXN_VIP') AS is_vip,
    APPROX_COUNT_DISTINCT(session_token) AS approx_sessions
FROM customer_activity_logs
WHERE DATE_ADD(current_date(), -7) <= TO_DATE(log_date, 'yyyy-MM-dd')
  AND partition_date = '{{process_date}}'"""
        print("Demo convert Spark SQL -> Presto (Bao ve bien {{process_date}}):")
        print(sample_spark)
        print("\n--- KET QUA PRESTO (PrestoDB 0.2xx) ---")
        print(convert_spark_to_presto(sample_spark, presto_dialect="presto"))
        print("\n--- KET QUA TRINO (PrestoSQL 330+/400+) ---")
        print(convert_spark_to_presto(sample_spark, presto_dialect="trino"))


if __name__ == "__main__":
    main()

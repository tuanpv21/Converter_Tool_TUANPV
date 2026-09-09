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

    # 3. Đảm bảo hàm last_day được đổi thành last_day_of_month trên Presto
    sql = re.sub(r'(?i)\blast_day\s*\(', 'last_day_of_month(', sql)

    # 4. Auto-healing: dọn dẹp lỗi CAST(x AS type, 'fmt') nếu có
    sql = re.sub(r'(?i)\bCAST\s*\(\s*(.*?)\s+AS\s+([a-zA-Z0-9_]+)\s*,\s*[\'"][^\'"]+[\'"]\s*\)', r'CAST(\1 AS \2)', sql)

    if dialect == "trino":
        sql = re.sub(r'(?i)\bformat_datetime\s*\(', 'date_format(', sql)

    return sql


def pre_process_spark_ast(expression):
    from sqlglot import exp
    from sqlglot.optimizer.annotate_types import annotate_types

    # 1. months_between(d1, d2) -> date_diff('month', d2, d1)
    for mb in expression.find_all(exp.MonthsBetween):
        d1 = mb.this
        d2 = mb.expression
        diff = exp.DateDiff(this=d1.copy(), expression=d2.copy(), unit=exp.var('month'))
        mb.replace(diff)
    for anon in expression.find_all(exp.Anonymous):
        if anon.this.upper() == 'MONTHS_BETWEEN':
            args = anon.expressions
            if len(args) >= 2:
                d1 = args[0]
                d2 = args[1]
                diff = exp.DateDiff(this=d1.copy(), expression=d2.copy(), unit=exp.var('month'))
                anon.replace(diff)

    # 2. isnull(x) -> x IS NULL, isnotnull(x) -> NOT (x IS NULL)
    for anon in expression.find_all(exp.Anonymous):
        name = anon.this.upper()
        if name == 'ISNULL' and len(anon.expressions) == 1:
            arg = anon.expressions[0]
            anon.replace(exp.Is(this=arg.copy(), expression=exp.Null()))
        elif name == 'ISNOTNULL' and len(anon.expressions) == 1:
            arg = anon.expressions[0]
            anon.replace(exp.Not(this=exp.Is(this=arg.copy(), expression=exp.Null())))

    # 3. Đồng bộ kiểu dữ liệu (Harmonize CAST) trong CASE WHEN để Presto không báo lỗi incompatible types
    try:
        annotated = annotate_types(expression)
        for case_node in annotated.find_all(exp.Case):
            branches = []
            for if_node in case_node.args.get('ifs', []):
                if if_node.args.get('true'):
                    branches.append(('if', if_node, if_node.args['true']))
            if case_node.args.get('default'):
                branches.append(('default', case_node, case_node.args['default']))
            
            types = []
            for _, _, val_node in branches:
                if val_node.type and not val_node.type.is_type('null', 'unknown'):
                    types.append(val_node.type)
            
            if len(types) <= 1:
                continue
                
            type_keys = set(t.this for t in types)
            if len(type_keys) <= 1:
                continue
                
            has_str = any(t.this in exp.DataType.TEXT_TYPES for t in types)
            has_double = any(t.this in (exp.DataType.Type.DOUBLE, exp.DataType.Type.FLOAT) for t in types)
            has_int = any(t.this in exp.DataType.INTEGER_TYPES for t in types)
            has_date = any(t.this == exp.DataType.Type.DATE for t in types)
            has_ts = any(t.this in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.DATETIME) for t in types)
            
            target_type = None
            if has_str:
                target_type = exp.DataType.build('varchar')
            elif has_double and has_int:
                target_type = exp.DataType.build('double')
            elif has_ts and has_date:
                target_type = exp.DataType.build('timestamp')
                
            if target_type:
                for kind, parent, val_node in branches:
                    if val_node.type and val_node.type.this != target_type.this and not val_node.type.is_type('null'):
                        casted = exp.Cast(this=val_node.copy(), to=target_type.copy())
                        if kind == 'if':
                            parent.set('true', casted)
                        else:
                            parent.set('default', casted)
    except Exception:
        pass

    return expression

def pre_process_presto_ast(expression):
    from sqlglot import exp

    # 1. date_diff('month', d1, d2) -> months_between(d2, d1)
    #    date_diff('day', d1, d2) -> datediff(d2, d1)
    for dd in expression.find_all(exp.DateDiff):
        unit = dd.args.get('unit')
        unit_str = str(unit).lower() if unit else ''
        end_d = dd.this
        start_d = dd.expression
        if 'month' in unit_str:
            mb = exp.Anonymous(this='months_between', expressions=[end_d.copy(), start_d.copy()])
            dd.replace(mb)
        elif 'day' in unit_str:
            diff = exp.Anonymous(this='datediff', expressions=[end_d.copy(), start_d.copy()])
            dd.replace(diff)
            
    # 2. strpos(str, substr) -> instr(str, substr)
    for anon in expression.find_all(exp.Anonymous):
        if anon.this.upper() == 'STRPOS':
            args = anon.expressions
            if len(args) == 2:
                instr_node = exp.Anonymous(this='instr', expressions=[args[0].copy(), args[1].copy()])
                anon.replace(instr_node)
        elif anon.this.upper() == 'LAST_DAY_OF_MONTH':
            anon.replace(exp.Anonymous(this='last_day', expressions=[e.copy() for e in anon.expressions]))

    return expression


def post_process_spark_ast(sql: str, original_presto_sql: str = "") -> str:
    # 1. Fix POSEXPLODE alias in Spark SQL:
    # Presto original: CROSS JOIN UNNEST(arr) WITH ORDINALITY AS t(val, pos)
    # sqlglot spark: LATERAL VIEW POSEXPLODE(arr) t AS val
    # Fix: LATERAL VIEW POSEXPLODE(arr) t AS pos, val
    if original_presto_sql:
        m = re.search(r'(?is)WITH\s+ORDINALITY\s+AS\s+([a-zA-Z0-9_]+)\s*\(\s*([^,\s]+)\s*,\s*([^)\s]+)\s*\)', original_presto_sql)
        if m:
            t_alias, val_alias, pos_alias = m.group(1), m.group(2), m.group(3)
            bad_spark_pat = rf'(?i)(LATERAL\s+VIEW\s+POSEXPLODE\s*\([^)]+\)\s+{t_alias}\s+AS\s+){val_alias}\b'
            sql = re.sub(bad_spark_pat, rf'\g<1>{pos_alias}, {val_alias}', sql)

    # 2. Fix INTERVAL '1' DAY -> INTERVAL 1 DAY
    sql = re.sub(r'(?i)\bINTERVAL\s+[\'"](\d+)[\'"]\s+([a-zA-Z]+)\b', r'INTERVAL \1 \2', sql)
    
    return sql


def replace_balanced_calls(sql: str, func_name: str, replacer_func) -> str:
    """
    Tìm và thay thế hàm func_name(...) chính xác theo cặp ngoặc đơn cân bằng,
    hỗ trợ hàm lồng nhau vô hạn tầng mà không bị cắt nhầm ngoặc.
    Thực hiện thay thế từ phải sang trái (innermost & rightmost first).
    """
    pattern = re.compile(rf'\b{func_name}\s*\(', re.IGNORECASE)
    calls = []
    for m in pattern.finditer(sql):
        start_idx = m.start()
        open_paren = m.end() - 1
        
        depth = 0
        in_sq = False
        in_dq = False
        args = []
        curr = []
        i = open_paren + 1
        n = len(sql)
        while i < n:
            c = sql[i]
            if c == "'" and not in_dq:
                if in_sq and i + 1 < n and sql[i+1] == "'":
                    curr.append("''")
                    i += 2
                    continue
                in_sq = not in_sq
                curr.append(c)
            elif c == '"' and not in_sq:
                in_dq = not in_dq
                curr.append(c)
            elif not in_sq and not in_dq:
                if c == '(':
                    depth += 1
                    curr.append(c)
                elif c == ')':
                    if depth == 0:
                        args.append("".join(curr).strip())
                        calls.append((start_idx, i + 1, args))
                        break
                    else:
                        depth -= 1
                        curr.append(c)
                elif c == ',' and depth == 0:
                    args.append("".join(curr).strip())
                    curr = []
                else:
                    curr.append(c)
            else:
                curr.append(c)
            i += 1
            
    for start, end, args in reversed(calls):
        new_val = replacer_func(args)
        if new_val is not None:
            sql = sql[:start] + new_val + sql[end:]
            
    return sql


# ==========================================
# 3. TRANSPILE BẰNG SQLGLOT (AST ENGINE)
# ==========================================
def transpile_with_sqlglot(sql_content: str, read_dialect: str, write_dialect: str, presto_dialect: str = "presto") -> str:
    try:
        import sqlglot
        if read_dialect == "spark" and write_dialect in ("presto", "trino", "athena"):
            parsed_list = sqlglot.parse(sql_content, read="spark")
            out_list = []
            for parsed in parsed_list:
                if parsed:
                    processed = pre_process_spark_ast(parsed)
                    out_list.append(processed.sql(write_dialect, pretty=True))
            res = ";\n\n".join(out_list)
            res = post_process_presto_dialect(res, presto_dialect, original_spark_sql=sql_content)
            return res
        else:
            # Presto -> Spark
            parsed_list = sqlglot.parse(sql_content, read=read_dialect)
            out_list = []
            for parsed in parsed_list:
                if parsed:
                    processed = pre_process_presto_ast(parsed)
                    out_list.append(processed.sql(write_dialect, pretty=True))
            res = ";\n\n".join(out_list)
            res = post_process_spark_ast(res, original_presto_sql=sql_content)
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
                (r'(?i)\bdate_add\s*\(\s*[\'"]day[\'"]\s*,\s*([^,]+?)\s*,\s*((?:[^()]+|\([^()]*\))+?)\s*\)', r'date_add(\2, \1)'),
                (r'(?i)\bdate_add\s*\(\s*[\'"]month[\'"]\s*,\s*([^,]+?)\s*,\s*((?:[^()]+|\([^()]*\))+?)\s*\)', r'add_months(\2, \1)'),
                (r'(?i)\bdate_diff\s*\(\s*[\'"]day[\'"]\s*,\s*([^,]+?)\s*,\s*((?:[^()]+|\([^()]*\))+?)\s*\)', r'datediff(\2, \1)'),
                (r'(?i)\bdate_diff\s*\(\s*[\'"]month[\'"]\s*,\s*([^,]+?)\s*,\s*((?:[^()]+|\([^()]*\))+?)\s*\)', r'months_between(\2, \1)'),
                (r'(?i)\bcurrent_date\b(?!\s*\()', 'current_date()'),
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
                (r'(?i)\blast_day_of_month\s*\(', 'last_day('),
                # S3 URI
                (r's3://', 's3a://'),
            ]
        else:
            # spark2presto
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
                (r'(?i)\bcurrent_timestamp\s*\(\s*\)', 'now()'),
                (r'(?i)\blast_day\s*\(', 'last_day_of_month('),
                (r'(?i)\bisnull\s*\(\s*([^)]+?)\s*\)', r'(\1 IS NULL)'),
                (r'(?i)\bisnotnull\s*\(\s*([^)]+?)\s*\)', r'(\1 IS NOT NULL)'),
                (r'(?i)\bnvl\s*\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)', r'coalesce(\1, \2)'),
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

        def java_to_strftime_fmt(fmt: str) -> str:
            if not fmt or '%' in fmt:
                return fmt
            out = fmt
            out = out.replace('yyyy', '%Y').replace('yy', '%y')
            out = out.replace('MM', '%m')
            out = out.replace('dd', '%d')
            out = out.replace('HH', '%H').replace('hh', '%I')
            out = out.replace('mm', '%i')
            out = out.replace('ss', '%s')
            out = out.replace('SSS', '%f')
            return out

        if self.mode == "presto2spark":
            # 1. Xử lý các hàm lồng nhau bằng balanced parentheses parser trước
            def date_diff_rep(args):
                if len(args) == 3:
                    unit = args[0].replace("'", "").replace('"', '').strip().lower()
                    d1, d2 = args[1].strip(), args[2].strip()
                    if unit == 'day':
                        return f"datediff({d2}, {d1})"
                    elif unit == 'month':
                        return f"months_between({d2}, {d1})"
                    elif unit == 'year':
                        return f"(months_between({d2}, {d1}) / 12)"
                    elif unit in ('hour', 'minute', 'second'):
                        return f"(unix_timestamp({d2}) - unix_timestamp({d1}))"
                elif len(args) == 2:
                    return f"datediff({args[1].strip()}, {args[0].strip()})"
                return None

            def date_add_rep(args):
                if len(args) == 3:
                    unit = args[0].replace("'", "").replace('"', '').strip().lower()
                    n, d = args[1].strip(), args[2].strip()
                    if unit == 'day':
                        return f"date_add({d}, {n})"
                    elif unit == 'month':
                        return f"add_months({d}, {n})"
                    elif unit == 'year':
                        return f"add_months({d}, ({n}) * 12)"
                return None

            def date_parse_rep(args):
                if len(args) >= 2:
                    s, fmt = args[0].strip(), args[1].strip()
                    fmt_clean = fmt.replace('%Y', 'yyyy').replace('%m', 'MM').replace('%d', 'dd') \
                                   .replace('%H', 'HH').replace('%i', 'mm').replace('%s', 'ss')
                    if fmt_clean in ("'yyyy-MM-dd'", '"yyyy-MM-dd"'):
                        return f"to_date({s}, {fmt_clean})"
                    return f"to_timestamp({s}, {fmt_clean})"
                elif len(args) == 1:
                    return f"to_date({args[0].strip()})"
                return None

            def date_format_rep(args):
                if len(args) >= 2:
                    s, fmt = args[0].strip(), args[1].strip()
                    fmt_clean = fmt.replace('%Y', 'yyyy').replace('%m', 'MM').replace('%d', 'dd') \
                                   .replace('%H', 'HH').replace('%i', 'mm').replace('%s', 'ss')
                    return f"date_format({s}, {fmt_clean})"
                return None

            res = replace_balanced_calls(res, 'date_parse', date_parse_rep)
            res = replace_balanced_calls(res, 'date_diff', date_diff_rep)
            res = replace_balanced_calls(res, 'date_add', date_add_rep)
            res = replace_balanced_calls(res, 'date_format', date_format_rep)
            res = replace_balanced_calls(res, 'format_datetime', date_format_rep)

            # 2. Xử lý INTERVAL chuẩn Spark: INTERVAL '1' DAY -> INTERVAL 1 DAY
            res = re.sub(r'(?i)\bINTERVAL\s+[\'"](\d+)[\'"]\s+([a-zA-Z]+)\b', r'INTERVAL \1 \2', res)

        elif self.mode == "spark2presto":
            date_fmt_func = 'date_format' if self.presto_dialect == 'trino' else 'format_datetime'

            # Auto-healing: dọn dẹp lỗi CAST(x AS type, 'fmt') nếu có
            res = re.sub(r'(?i)\bCAST\s*\(\s*(.*?)\s+AS\s+([a-zA-Z0-9_]+)\s*,\s*[\'"][^\'"]+[\'"]\s*\)', r'CAST(\1 AS \2)', res)

            def to_date_rep(args):
                if len(args) >= 2:
                    s, fmt = args[0].strip(), args[1].strip()
                    fmt_clean = java_to_strftime_fmt(fmt)
                    return f"CAST(date_parse({s}, {fmt_clean}) AS DATE)"
                elif len(args) == 1:
                    return f"CAST({args[0].strip()} AS DATE)"
                return None

            def to_timestamp_rep(args):
                if len(args) >= 2:
                    s, fmt = args[0].strip(), args[1].strip()
                    fmt_clean = java_to_strftime_fmt(fmt)
                    return f"date_parse({s}, {fmt_clean})"
                elif len(args) == 1:
                    return f"CAST({args[0].strip()} AS TIMESTAMP)"
                return None

            def last_day_rep(args):
                if len(args) == 1:
                    return f"last_day_of_month({args[0].strip()})"
                return None

            def date_format_rep(args):
                if len(args) >= 2:
                    s, fmt = args[0].strip(), args[1].strip()
                    fmt_clean = java_to_strftime_fmt(fmt)
                    s_lower = s.lower()
                    if s.strip().upper().startswith("CAST(") and s.strip().upper().endswith("AS TIMESTAMP)"):
                        target_s = s
                    elif any(k in s_lower for k in ('last_day_of_month', 'last_day', 'current_date', 'as date')):
                        target_s = f"CAST({s} AS TIMESTAMP)"
                    else:
                        target_s = s
                    return f"{date_fmt_func}({target_s}, {fmt_clean})"
                return None

            def date_parse_rep(args):
                if len(args) >= 2:
                    s, fmt = args[0].strip(), args[1].strip()
                    fmt_clean = java_to_strftime_fmt(fmt)
                    return f"date_parse({s}, {fmt_clean})"
                return None

            def date_add_rep(args):
                if len(args) == 2:
                    d, n = args[0].strip(), args[1].strip()
                    return f"date_add('day', {n}, {d})"
                return None

            def date_sub_rep(args):
                if len(args) == 2:
                    d, n = args[0].strip(), args[1].strip()
                    return f"date_add('day', -({n}), {d})"
                return None

            def add_months_rep(args):
                if len(args) == 2:
                    d, n = args[0].strip(), args[1].strip()
                    return f"date_add('month', {n}, {d})"
                return None

            def months_between_rep(args):
                if len(args) == 2:
                    d2, d1 = args[0].strip(), args[1].strip()
                    return f"date_diff('month', {d1}, {d2})"
                return None

            def datediff_rep(args):
                if len(args) == 2:
                    d2, d1 = args[0].strip(), args[1].strip()
                    return f"date_diff('day', {d1}, {d2})"
                return None

            res = replace_balanced_calls(res, 'to_date', to_date_rep)
            res = replace_balanced_calls(res, 'to_timestamp', to_timestamp_rep)
            res = replace_balanced_calls(res, 'last_day', last_day_rep)
            res = replace_balanced_calls(res, 'date_format', date_format_rep)
            res = replace_balanced_calls(res, 'format_datetime', date_format_rep)
            res = replace_balanced_calls(res, 'date_parse', date_parse_rep)
            res = replace_balanced_calls(res, 'date_add', date_add_rep)
            res = replace_balanced_calls(res, 'date_sub', date_sub_rep)
            res = replace_balanced_calls(res, 'add_months', add_months_rep)
            res = replace_balanced_calls(res, 'months_between', months_between_rep)
            res = replace_balanced_calls(res, 'datediff', datediff_rep)

        for pattern, replacement in self.rules:
            res = re.sub(pattern, replacement, res)

        # Dọn dẹp AS (val, pos) nếu thiếu table alias
        res = re.sub(r'AS\s+\(([^)]+)\)', r'AS _t0(\1)', res)

        # Đồng bộ CAST cho các nhánh CASE WHEN khi giữ nguyên định dạng
        if self.mode == "spark2presto":
            def harmonize_case_block(match):
                block = match.group(0)
                branches = re.findall(r'(?i)\b(then|else)\s+([^\s]+)', block)
                has_str = any(re.match(r"^['\"].*['\"]$", v.strip()) for _, v in branches)
                has_num = any(re.match(r"^\d+(?:\.\d+)?$", v.strip()) for _, v in branches)
                if has_str and has_num:
                    return re.sub(r'(?i)\b(then|else)\s+(\d+(?:\.\d+)?)', r'\1 CAST(\2 AS VARCHAR)', block)
                return block
            res = re.sub(r'(?is)\bCASE\b.*?\bEND\b', harmonize_case_block, res)

        return res


# ==========================================
# 5. PUBLIC API FUNCTIONS
# ==========================================
def convert_sql(sql_code: str, mode: str = "presto2spark", presto_dialect: str = "presto", keep_format: bool = True) -> str:
    """
    mode: 'presto2spark' hoac 'spark2presto'
    presto_dialect: 'presto' (PrestoDB 0.2xx), 'trino' (Trino 330+/400+), 'athena' (AWS Athena)
    keep_format: True de giu nguyen dinh dang goc khong format lai dong / khoang trang
    """
    # 1. Bảo vệ các biến tham số (Jinja {{...}}, Shell ${...})
    protected_sql, vars_list = protect_variables(sql_code)

    if keep_format:
        converter = RegexRuleConverter(mode=mode, presto_dialect=presto_dialect)
        fallback_res = converter.convert(protected_sql)
        return restore_variables(fallback_res, vars_list)

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

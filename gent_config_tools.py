#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================================================
MODULE: gent_config_tools.py
TÁC GIẢ: TUANPV - Data Platform & Automation Engineer
MÔ TẢ:
    Bộ công cụ tự động hóa sinh cấu hình báo cáo ngân hàng (Circular Config Generator),
    sinh luồng dữ liệu pipeline (Dataflow C_PRE_SOURCE_CODE) và chuyển đổi dữ liệu Excel/CSV
    sang câu lệnh SQL INSERT đa nền tảng (Spark SQL, Presto, Oracle, MySQL, Postgres).

DANH SÁCH 3 BỘ CÔNG CỤ CHÍNH:
    1. CircularConfigGenerator:
       - Sinh 3 câu lệnh INSERT cấu hình mẫu biểu Thông tư: C_GROUP_ITEM, C_ITEM_ORG, C_SOURCE_CODE.
       - Hỗ trợ các cơ chế JOIN thứ tự chỉ tiêu ORDER_ID (cột trực tiếp, bảng tạm JOIN, CASE WHEN).
    
    2. DataflowPipelineGenerator:
       - Sinh cấu hình thực thi Pipeline vào bảng prod_gold_ssd.sbv_report.c_pre_source_code.
       - Tự động convert cú pháp SQL từ Presto sang Spark SQL (nhờ module convert_presto_to_spark).
       - Kỳ chạy lại (rerun_term): Tự động gán NULL nếu rỗng, không sinh câu lệnh COMMIT dư thừa.
    
    3. ExcelToSqlInserter:
       - Đọc DDL CREATE TABLE và dữ liệu Excel (.xlsx, .xls, .csv, hoặc văn bản dạng bảng TSV).
       - Tự động nhận diện và convert kiểu dữ liệu:
           + INT / SMALLINT: Cắt đuôi .0, loại bỏ dấu phẩy nghìn, xử lý số khoa học (1.5E+05 -> 150000).
           + BIGINT: Giữ nguyên độ chính xác số ID lớn, số tài khoản, timestamp ms, loại bỏ .0.
           + VARCHAR: Tự động bọc '...', escape dấu nháy đơn (' -> ''), bảo toàn số 0 ở đầu ('00112233').
           + DECIMAL: Giữ nguyên số lẻ thập phân, làm sạch dấu phẩy phân cách nghìn.
           + DATE / TIMESTAMP: Tự động chuẩn hóa định dạng (dd/MM/yyyy, yyyy-MM-dd).
       - Hỗ trợ sinh INSERT dạng Batch (VALUES (...), (...)) hoặc Single INSERT.
========================================================================================
"""

import os
import sys
import re
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Tuple, Optional, Any, Union
from dataclasses import dataclass, field

# Đảm bảo in tiếng Việt có dấu mượt mà trên console Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Tích hợp bộ chuyển đổi cú pháp Presto <-> Spark SQL có sẵn trong cùng thư mục
try:
    from convert_presto_to_spark import convert_sql
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    try:
        from convert_presto_to_spark import convert_sql
    except ImportError:
        def convert_sql(sql: str, mode: str = "presto2spark", **kwargs) -> str:
            # Fallback đơn giản nếu không tìm thấy file convert_presto_to_spark.py
            return sql


# ========================================================================================
# PHẦN 0: CÁC HÀM TIỆN ÍCH CHUNG (COMMON UTILITIES)
# ========================================================================================

def escape_sql_string(val: Any) -> str:
    """
    Escape ký tự nháy đơn trong chuỗi SQL để chống lỗi cú pháp (Syntax Error / SQL Injection).
    Trong chuẩn SQL, dấu nháy đơn ' bên trong chuỗi được biểu diễn bằng 2 dấu nháy đơn ''.
    Ví dụ: "Công ty O'Reilly" -> "Công ty O''Reilly"
    """
    if val is None:
        return ""
    return str(val).replace("'", "''")


def clean_number_string(val: str) -> str:
    """
    Làm sạch chuỗi số:
    - Loại bỏ dấu phẩy ngăn cách hàng nghìn (ví dụ: '150,000,000' -> '150000000').
    - Loại bỏ khoảng trắng thừa.
    """
    if not val:
        return ""
    return str(val).replace(",", "").replace(" ", "").strip()


# ========================================================================================
# PHẦN 1: TOOL GENT CONFIG FILE CIRCULAR (MSB TONE)
# ========================================================================================

@dataclass
class CircularColumn:
    """
    Đại diện cho cấu hình của 1 cột trên mẫu biểu báo cáo.
    """
    col_location: str          # Tên cột Excel (ví dụ: 'A', 'B', 'C',...)
    item_name: str             # Tên chỉ tiêu từ template
    source_col: str = ""       # Cột trong bảng kết quả nguồn (ví dụ: 'A.BALANCE_AMT')
    row_excel: int = 1         # Dòng bắt đầu trên Excel (trong DB sẽ lưu = row_excel - 1)
    data_type: str = "NUMBER"  # Kiểu dữ liệu: NUMBER, VARCHAR, DATE,...
    unit: str = "NULL"         # Đơn vị làm tròn (ví dụ: 1000000 cho triệu đồng, hoặc NULL)
    decimal_position: Optional[Union[int, str]] = 0  # Số chữ số thập phân: 0, 2, 4,... hoặc None / 'NULL'


@dataclass
class OrderItem:
    """
    Đại diện cho 1 dòng trong bảng mapping thứ tự chỉ tiêu ORDER_ID.
    """
    stt: str                   # STT hiển thị trên báo cáo (ví dụ: '1', '2.a', 'I',...)
    order_id: int              # Thứ tự số nguyên để ORDER BY
    item_name: str             # Tên chỉ tiêu tương ứng


class CircularConfigGenerator:
    """
    Trình sinh mã SQL cấu hình báo cáo Thông tư:
    - Bảng C_GROUP_ITEM: Thông tin chung của mẫu biểu
    - Bảng C_ITEM_ORG: Cấu hình toạ độ từng cột/dòng trên file Excel template (hỗ trợ decimal_position)
    - Bảng C_SOURCE_CODE: Câu lệnh truy vấn SQL đổ dữ liệu vào từng cột (lấy bảng map chỉ tiêu làm gốc LEFT JOIN)
    - Bảng thứ tự ORDER: Bảng cấu hình map chỉ tiêu (hỗ trợ điền catalog/schema tùy ý)
    """

    def __init__(
        self,
        group_code: str = "C00384",
        sheet_no: str = "1",
        group_name: str = "BÁO CÁO HOẠT ĐỘNG BẢO LÃNH",
        term_code: str = "M",
        is_rerun: int = 1,
        target_schema: str = "prod_gold_ssd.sbv_report.",
        result_table: str = "{{silver}}.{{silver_schema}}.c_pre_mbnt_cv2401 A",
        branch_mode: str = "total",  # 'total' (Fix VN0010001) hoặc 'branch' (A.BRANCH_ID)
        branch_col: str = "A.BRANCH_ID",
        order_mode: str = "direct_col",  # 'direct_col', 'join_subquery', 'case_stt'
        order_col: str = "A.ORDER_ID",
        order_table: Optional[str] = None,  # Cho phép điền catalog & schema bảng map chỉ tiêu
        stt_col: str = "A.STT",
        where_clause: str = "A.TRANS_TYPE = 'BUY' AND a.import_date = {{process_date}}",
        user_modify: str = "TUANPV",
        pre_source_id: str = "NULL",
        is_used: int = 1,
        source_order: str = "NULL"
    ):
        self.group_code = group_code.strip().upper()
        self.sheet_no = sheet_no.strip()
        self.sbv_group_code = f"{self.group_code}{self.sheet_no}"
        self.group_name = group_name.strip()
        self.term_code = term_code.strip().upper()
        self.is_rerun = is_rerun
        self.target_schema = target_schema.strip()
        self.result_table = result_table.strip()
        self.branch_mode = branch_mode
        self.branch_col = branch_col.strip()
        self.order_mode = order_mode
        self.order_col = order_col.strip()
        self.order_table = order_table.strip() if order_table and order_table.strip() else None
        self.stt_col = stt_col.strip()
        self.where_clause = where_clause.strip()
        self.user_modify = user_modify.strip()
        self.pre_source_id = pre_source_id
        self.is_used = is_used
        self.source_order = source_order

        # Tên diễn giải kỳ hạn
        term_map = {"M": "Hàng Tháng", "D": "Hàng Ngày", "Q": "Hàng Quý", "Y": "Hàng Năm", "H": "Nửa Năm"}
        self.term_name = term_map.get(self.term_code, f"Kỳ {self.term_code}")

        self.columns: List[CircularColumn] = []
        self.order_items: List[OrderItem] = []

    def add_column(self, col: CircularColumn) -> 'CircularConfigGenerator':
        """Thêm 1 cột cấu hình vào danh sách."""
        self.columns.append(col)
        return self

    def add_order_item(self, item: OrderItem) -> 'CircularConfigGenerator':
        """Thêm 1 dòng thứ tự chỉ tiêu ORDER_ID vào danh sách."""
        self.order_items.append(item)
        return self

    def generate_sql(self) -> str:
        """
        Sinh toàn bộ script SQL cấu hình hoàn chỉnh.
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_table_name = f"c_report_order_{self.group_code.lower()}"
        full_order_table = self.order_table or f"{self.target_schema}{order_table_name}"

        # Xác định biểu thức chi nhánh (BRANCH_ID)
        branch_expr = f"'{escape_sql_string(self.branch_col)}'" if self.branch_mode == "total" else self.branch_col
        if self.branch_mode == "total":
            branch_expr = "'VN0010001'"

        # Xác định cơ chế JOIN thứ tự ORDER_ID
        if self.order_mode == "join_subquery":
            # Lấy bảng map chỉ tiêu làm bảng gốc (LEFT JOIN) để bảo toàn 100% tất cả các hàng chỉ tiêu
            # ngay cả khi các chi nhánh hoặc trường tính toán không có đủ dữ liệu
            if self.where_clause:
                from_join_expr = f"{full_order_table} ORD LEFT JOIN {self.result_table} ON ORD.stt = {self.stt_col} AND ({self.where_clause})"
            else:
                from_join_expr = f"{full_order_table} ORD LEFT JOIN {self.result_table} ON ORD.stt = {self.stt_col}"
            order_expr = "ORD.order_id"
            final_where = ""
        elif self.order_mode == "case_stt":
            from_join_expr = self.result_table
            if self.order_items:
                case_clauses = " ".join([f"WHEN '{escape_sql_string(o.stt)}' THEN {o.order_id}" for o in self.order_items])
                order_expr = f"CASE {self.stt_col} {case_clauses} ELSE 9999 END"
            else:
                order_expr = f"CASE WHEN {self.stt_col} = '1' THEN 1 ELSE 999 END"
            final_where = self.where_clause
        else:
            # direct_col: Có sẵn cột trong bảng kết quả
            from_join_expr = self.result_table
            order_expr = self.order_col
            final_where = self.where_clause

        lines = []
        lines.append("-- =========================================================================================")
        lines.append("-- SCRIPT TỰ ĐỘNG SINH CẤU HÌNH BÁO CÁO (MSB CIRCULAR CONFIG GENERATOR) - TUANPV")
        lines.append(f"-- MÃ BÁO CÁO: {self.group_code} | SHEET: {self.sheet_no} ({self.sbv_group_code}) | KỲ HẠN: {self.term_code} ({self.term_name})")
        lines.append(f"-- CATALOG & SCHEMA ĐÍCH: {self.target_schema or '(Mặc định)'}")
        if self.order_mode == "join_subquery":
            lines.append(f"-- BẢNG THỨ TỰ ORDER (BẢNG GỐC): {full_order_table}")
        lines.append(f"-- THỜI GIAN SINH: {now_str}")
        lines.append("-- =========================================================================================\n")

        # 1. BẢNG THỨ TỰ NẾU DÙNG JOIN_SUBQUERY
        if self.order_mode == "join_subquery" and self.order_items:
            lines.append("-- =========================================================================================")
            lines.append(f"-- 1. TẠO BẢNG THỨ TỰ CHỈ TIÊU: {full_order_table} (BẢNG GỐC LEFT JOIN KẾT QUẢ)")
            lines.append("-- =========================================================================================")
            lines.append(f"DROP TABLE IF EXISTS {full_order_table};\n")
            lines.append(f"CREATE TABLE {full_order_table} (")
            lines.append("    stt VARCHAR(50),")
            lines.append("    order_id INT,")
            lines.append("    item_name VARCHAR(500)")
            lines.append(");\n")
            for ord_item in self.order_items:
                lines.append(
                    f"INSERT INTO {full_order_table} (stt, order_id, item_name) "
                    f"VALUES ('{escape_sql_string(ord_item.stt)}', {ord_item.order_id}, '{escape_sql_string(ord_item.item_name)}');"
                )
            lines.append("\n")

        # 2. DELETE DỮ LIỆU CŨ TRÁNH TRÙNG LẶP
        lines.append("-- =========================================================================================")
        lines.append("-- 2. XÓA CẤU HÌNH CŨ THEO KEY CHUNG CỦA BÁO CÁO (TRÁNH TRÙNG LẶP DỮ LIỆU)")
        lines.append("-- =========================================================================================")
        lines.append(f"DELETE FROM {self.target_schema}C_GROUP_ITEM WHERE group_item_code = '{escape_sql_string(self.group_code)}';")
        lines.append(f"DELETE FROM {self.target_schema}C_ITEM_ORG WHERE group_item_code = '{escape_sql_string(self.group_code)}';")
        lines.append(f"DELETE FROM {self.target_schema}C_SOURCE_CODE WHERE org_item_code LIKE '{escape_sql_string(self.group_code)}_%';\n")

        # 3. INSERT BẢNG C_GROUP_ITEM
        lines.append("-- =========================================================================================")
        lines.append(f"-- 3. INSERT VÀO BẢNG: {self.target_schema}C_GROUP_ITEM (Thông tin chung mẫu biểu)")
        lines.append("-- =========================================================================================")
        lines.append(f"INSERT INTO {self.target_schema}C_GROUP_ITEM (")
        lines.append("    item_code_org, group_item_code, sbv_group_item_code, sbv_group_item_code_org,")
        lines.append("    group_item_name, term_code, term_name, is_rerun")
        lines.append(") VALUES (")
        lines.append(
            f"    NULL, '{escape_sql_string(self.group_code)}', '{escape_sql_string(self.sbv_group_code)}', "
            f"'{escape_sql_string(self.group_code)}', '{escape_sql_string(self.group_name)}', "
            f"'{escape_sql_string(self.term_code)}', '{escape_sql_string(self.term_name)}', {self.is_rerun}"
        )
        lines.append(");\n\n")

        # 4. INSERT BẢNG C_ITEM_ORG
        lines.append("-- =========================================================================================")
        lines.append(f"-- 4. INSERT VÀO BẢNG: {self.target_schema}C_ITEM_ORG (Cấu hình từng cột trên Template)")
        lines.append("-- =========================================================================================")
        for col in self.columns:
            org_item_code = f"{self.group_code}_{col.col_location}"
            row_db = col.row_excel - 1 if col.row_excel > 0 else 0
            unit_val = col.unit.strip() if col.unit and col.unit.strip().upper() != "NULL" else "NULL"
            if col.decimal_position is not None and str(col.decimal_position).strip().upper() not in ("NULL", "NONE", ""):
                try:
                    dec_pos_val = str(int(col.decimal_position))
                except (ValueError, TypeError):
                    dec_pos_val = "NULL"
            else:
                dec_pos_val = "NULL"

            lines.append(f"INSERT INTO {self.target_schema}C_ITEM_ORG (")
            lines.append("    org_item_code, group_item_code, sbv_org_item_code, symbol, item_type,")
            lines.append("    item_name, item_level, col_location, row_location, term_code,")
            lines.append("    sbv_term_code, general_report, branch_report, data_type, unit,")
            lines.append("    unit_code, decimal_position, date_modify, user_modify, item_type_1, group_code")
            lines.append(") VALUES (")
            lines.append(
                f"    '{escape_sql_string(org_item_code)}', '{escape_sql_string(self.group_code)}', '{escape_sql_string(self.sbv_group_code)}', NULL, 'DR',\n"
                f"    '{escape_sql_string(col.item_name)}', NULL, '{escape_sql_string(col.col_location)}', {row_db}, NULL,\n"
                f"    NULL, NULL, 0, '{escape_sql_string(col.data_type)}', {unit_val},\n"
                f"    NULL, {dec_pos_val}, CURRENT_DATE, '{escape_sql_string(self.user_modify)}', 'FORM', NULL"
            )
            lines.append(");")
        lines.append("\n")

        # 5. INSERT BẢNG C_SOURCE_CODE
        lines.append("-- =========================================================================================")
        lines.append(f"-- 5. INSERT VÀO BẢNG: {self.target_schema}C_SOURCE_CODE (Nguồn SQL đổ vào từng cột)")
        lines.append("-- =========================================================================================")
        for col in self.columns:
            org_item_code = f"{self.group_code}_{col.col_location}"
            source_col_expr = col.source_col if col.source_col else f"A.COL_{col.col_location}"

            raw_query = f"SELECT '{org_item_code}' AS ORG_ITEM_CODE, {source_col_expr} AS ITEM_VALUE, {branch_expr} AS BRANCH_ID, {order_expr} AS ORG_ORDER_ID FROM {from_join_expr}"
            if final_where:
                raw_query += f" WHERE {final_where}"

            escaped_query = escape_sql_string(raw_query)

            lines.append(f"INSERT INTO {self.target_schema}C_SOURCE_CODE (")
            lines.append("    org_item_code, use_procedure, sql_code, pre_source_code_id, is_used, order_id")
            lines.append(") VALUES (")
            lines.append(
                f"    '{escape_sql_string(org_item_code)}', NULL, '{escaped_query}', "
                f"{self.pre_source_id}, {self.is_used}, {self.source_order}"
            )
            lines.append(");")

        lines.append(f"\n-- ======================= HOÀN TẤT SINH CẤU HÌNH CHO {self.group_code} =======================\n")
        return "\n".join(lines)


# ========================================================================================
# PHẦN 2: TOOL GENT DATA FLOW C_PRE_SOURCE_CODE CIRCULAR
# ========================================================================================

@dataclass
class PipelineStep:
    """
    Đại diện cho 1 bước trong luồng xử lý dữ liệu (Pipeline Step).
    """
    order_id: int
    action: str = "INSERT"  # DELETE, INSERT, TRUNCATE, MERGE, CREATE, UPDATE, EXECUTE
    table_name: str = ""
    description: str = ""
    sql_code: str = ""      # Câu lệnh SQL của bước này


class DataflowPipelineGenerator:
    """
    Trình sinh mã SQL cấu hình Data Flow vào bảng c_pre_source_code.
    Tự động chuyển đổi các câu lệnh Presto SQL sang Spark SQL,
    xử lý rerun_term (để trống -> NULL), không sinh lệnh COMMIT.
    """

    def __init__(
        self,
        report_code: str = "c_pre_sao_ke_lending",
        grp_job: str = "C_PRE_STAT",
        term_code: str = "D",
        rerun_term: Optional[str] = None,
        is_used: int = 1,
        gen_delete: bool = True,
        auto_convert_spark: bool = True
    ):
        self.report_code = report_code.strip()
        self.grp_job = grp_job.strip()
        self.term_code = term_code.strip()
        self.rerun_term = rerun_term.strip() if rerun_term and rerun_term.strip() else None
        self.is_used = is_used
        self.gen_delete = gen_delete
        self.auto_convert_spark = auto_convert_spark
        self.steps: List[PipelineStep] = []

    def add_step(self, step: PipelineStep) -> 'DataflowPipelineGenerator':
        """Thêm 1 bước thực thi vào pipeline."""
        if self.auto_convert_spark and step.sql_code:
            # Tự động chuyển đổi cú pháp SQL sang Spark SQL
            step.sql_code = convert_sql(step.sql_code, mode="presto2spark", keep_format=True)
        self.steps.append(step)
        return self

    def parse_presto_script(self, raw_script: str) -> 'DataflowPipelineGenerator':
        """
        Tự động phân tích 1 đoạn kịch bản SQL Presto nhiều câu lệnh thành các bước (Steps).
        Quy tắc: Bóc tách các câu DELETE / INSERT INTO / TRUNCATE / MERGE.
        """
        # Loại bỏ các block comment /* ... */
        clean_sql = re.sub(r'/\*[\s\S]*?\*/', '', raw_script)
        
        # Tách từng câu lệnh theo dấu chấm phẩy kết thúc dòng
        raw_stmts = [s.strip() for s in clean_sql.split(';') if s.strip()]

        current_order = 10
        for stmt in raw_stmts:
            # Bỏ qua các câu lệnh COMMIT/ROLLBACK hoặc rỗng
            if re.match(r'(?i)^\s*(commit|rollback)\s*$', stmt):
                continue

            action = "INSERT"
            table_name = self.report_code
            desc = f"Thực hiện bước {current_order}"

            m_del = re.search(r'(?i)\bDELETE\s+FROM\s+([a-zA-Z0-9_\.]+)', stmt)
            m_ins = re.search(r'(?i)\bINSERT\s+INTO\s+([a-zA-Z0-9_\.]+)', stmt)
            m_trunc = re.search(r'(?i)\bTRUNCATE\s+TABLE\s+([a-zA-Z0-9_\.]+)', stmt)

            if m_del:
                action = "DELETE"
                table_name = m_del.group(1).split('.')[-1].lower()
                desc = f"Xóa dữ liệu cũ bảng {table_name}"
            elif m_ins:
                action = "INSERT"
                table_name = m_ins.group(1).split('.')[-1].lower()
                desc = f"Đổ dữ liệu vào bảng {table_name}"
            elif m_trunc:
                action = "TRUNCATE"
                table_name = m_trunc.group(1).split('.')[-1].lower()
                desc = f"Làm trống bảng {table_name}"

            step_sql = stmt
            if self.auto_convert_spark:
                step_sql = convert_sql(stmt, mode="presto2spark", keep_format=True)

            self.steps.append(PipelineStep(
                order_id=current_order,
                action=action,
                table_name=table_name,
                description=desc,
                sql_code=step_sql
            ))
            current_order += 10

        return self

    def generate_sql(self) -> str:
        """
        Sinh câu lệnh INSERT INTO prod_gold_ssd.sbv_report.c_pre_source_code.
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rerun_term_val = f"'{escape_sql_string(self.rerun_term)}'" if self.rerun_term else "NULL"

        lines = []
        lines.append("-- =========================================================================================")
        lines.append("-- SCRIPT TỰ ĐỘNG SINH: TOOL GENT DATA FLOW C_PRE_SOURCE_CODE CIRCULAR - TUANPV")
        lines.append("-- BẢNG CẤU HÌNH: prod_gold_ssd.sbv_report.c_pre_source_code")
        lines.append(f"-- MÃ BÁO CÁO: {self.report_code} | GROUP: {self.grp_job} | KỲ HẠN: {self.term_code} | KỲ CHẠY LẠI: {self.rerun_term or 'NULL'}")
        lines.append(f"-- SỐ LƯỢNG BƯỚC THỰC THI: {len(self.steps)} | ENGINE: SPARK SQL")
        lines.append(f"-- THỜI GIAN SINH: {now_str}")
        lines.append("-- =========================================================================================\n")

        # 1. DELETE CẤU HÌNH CŨ
        if self.gen_delete and self.steps:
            order_ids = [str(s.order_id) for s in self.steps]
            lines.append("-- 1. XÓA CẤU HÌNH CŨ CỦA BÁO CÁO THEO KEY (sbv_group_item_code_org VÀ order_id)")
            if len(order_ids) == 1:
                lines.append(
                    f"DELETE FROM prod_gold_ssd.sbv_report.c_pre_source_code \n"
                    f"WHERE sbv_group_item_code_org = '{escape_sql_string(self.report_code)}'\n"
                    f"  AND order_id = {order_ids[0]};\n"
                )
            else:
                lines.append(
                    f"DELETE FROM prod_gold_ssd.sbv_report.c_pre_source_code \n"
                    f"WHERE sbv_group_item_code_org = '{escape_sql_string(self.report_code)}'\n"
                    f"  AND order_id IN ({', '.join(order_ids)});\n"
                )

        # 2. INSERT CẤU HÌNH CÁC BƯỚC
        lines.append("-- 2. NẠP CẤU HÌNH CÁC BƯỚC THỰC THI (ĐÃ CHUYỂN ĐỔI SANG SPARK SQL)")
        lines.append("INSERT INTO prod_gold_ssd.sbv_report.c_pre_source_code (")
        lines.append('    table_name, "action", grp_job, sbv_group_item_code_org, sql_code,')
        lines.append("    description_sql_code, term_code, order_id, rerun_term, is_used")
        lines.append(") VALUES")

        for idx, step in enumerate(self.steps):
            is_last = (idx == len(self.steps) - 1)
            comma_or_semicolon = ";" if is_last else ","
            escaped_sql = escape_sql_string(step.sql_code)
            lines.append("(")
            lines.append(f"    '{escape_sql_string(step.table_name)}',")
            lines.append(f"    '{escape_sql_string(step.action)}',")
            lines.append(f"    '{escape_sql_string(self.grp_job)}',")
            lines.append(f"    '{escape_sql_string(self.report_code)}',")
            lines.append(f"    '{escaped_sql}',")
            lines.append(f"    '{escape_sql_string(step.description)}',")
            lines.append(f"    '{escape_sql_string(self.term_code)}',")
            lines.append(f"    {step.order_id},")
            lines.append(f"    {rerun_term_val},")
            lines.append(f"    {self.is_used}")
            lines.append(f"){comma_or_semicolon}")

        lines.append("\n-- ======================= HOÀN TẤT SINH DATA FLOW =======================\n")
        return "\n".join(lines)


# ========================================================================================
# PHẦN 3: TOOL INSERT TABLE FROM EXCEL
# ========================================================================================

@dataclass
class ColumnMeta:
    """
    Thông tin cấu trúc của một cột bóc tách từ câu DDL CREATE TABLE.
    """
    name: str              # Tên cột (ví dụ: 'account_no')
    raw_type: str          # Kiểu dữ liệu gốc trong DDL (ví dụ: 'VARCHAR(50)')
    category: str          # Nhóm kiểu chuẩn hóa: INT, BIGINT, VARCHAR, DECIMAL, DATE, TIMESTAMP, BOOLEAN


def categorize_data_type(raw_type: str) -> str:
    """
    Quy đổi kiểu dữ liệu SQL thô về 1 trong các nhóm xử lý chuẩn:
    - INT: INT, INTEGER, SMALLINT, TINYINT
    - BIGINT: BIGINT, INT8, LONG
    - DECIMAL: DECIMAL, NUMERIC, NUMBER, FLOAT, DOUBLE, REAL
    - VARCHAR: VARCHAR, VARCHAR2, CHAR, NVARCHAR, STRING, TEXT, CLOB
    - DATE: DATE
    - TIMESTAMP: TIMESTAMP, DATETIME
    - BOOLEAN: BOOLEAN, BOOL
    """
    t = raw_type.upper().strip()
    if re.search(r'\b(BIGINT|INT8|LONG)\b', t):
        return "BIGINT"
    if re.search(r'\b(INT|INTEGER|SMALLINT|TINYINT)\b', t):
        return "INT"
    if re.search(r'\b(DECIMAL|NUMERIC|NUMBER|FLOAT|DOUBLE|REAL)\b', t):
        return "DECIMAL"
    if re.search(r'\b(DATE)\b', t):
        return "DATE"
    if re.search(r'\b(TIMESTAMP|DATETIME)\b', t):
        return "TIMESTAMP"
    if re.search(r'\b(BOOLEAN|BOOL)\b', t):
        return "BOOLEAN"
    return "VARCHAR"


def parse_create_table_ddl(ddl: str) -> Tuple[str, List[ColumnMeta]]:
    """
    Bóc tách tên bảng và danh sách các cột từ câu DDL `CREATE TABLE ... (...)`.
    
    Returns:
        (table_name, list_of_column_meta)
    """
    # 1. Bóc tách tên bảng
    table_match = re.search(r'(?i)CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_\.]+)', ddl)
    table_name = table_match.group(1) if table_match else "my_table"

    # 2. Lấy nội dung bên trong cặp ngoặc đơn đầu tiên của CREATE TABLE
    paren_start = ddl.find('(')
    paren_end = ddl.rfind(')')
    if paren_start == -1 or paren_end == -1 or paren_end <= paren_start:
        return table_name, []

    body = ddl[paren_start + 1:paren_end]

    # 3. Tách từng định nghĩa cột
    columns: List[ColumnMeta] = []
    lines = body.split('\n')
    for line in lines:
        line_clean = line.strip()
        # Bỏ qua dòng trống hoặc chú thích
        if not line_clean or line_clean.startswith('--') or line_clean.startswith('/*'):
            continue
        # Bỏ qua các ràng buộc bảng (PRIMARY KEY, CONSTRAINT, INDEX,...)
        if re.match(r'(?i)^(PRIMARY\s+KEY|FOREIGN\s+KEY|CONSTRAINT|INDEX|KEY|UNIQUE|CHECK)\b', line_clean):
            continue

        # Cắt dấu phẩy ở cuối dòng
        if line_clean.endswith(','):
            line_clean = line_clean[:-1].strip()

        # Regex trích xuất tên cột và kiểu dữ liệu (hỗ trợ DECIMAL(18, 2), VARCHAR(50),...)
        col_match = re.match(r'^[`"\[]?([a-zA-Z0-9_]+)[`"\]]?\s+([a-zA-Z0-9_]+(?:\s*\([^)]+\))?)', line_clean)
        if col_match:
            c_name = col_match.group(1)
            c_type = col_match.group(2)
            c_cat = categorize_data_type(c_type)
            columns.append(ColumnMeta(name=c_name, raw_type=c_type, category=c_cat))

    return table_name, columns


def format_sql_cell(val: Any, category: str, dialect: str = "spark", empty_as_null: bool = True) -> str:
    """
    HÀM CỐT LÕI (CORE CONVERTER): Tự động format từng ô dữ liệu sang cú pháp SQL chuẩn.
    
    Tham số:
        val: Giá trị từ ô Excel/CSV (chuỗi, số, float, nan,...)
        category: Nhóm kiểu dữ liệu (INT, BIGINT, VARCHAR, DECIMAL, DATE, TIMESTAMP, BOOLEAN)
        dialect: Engine SQL đích ('spark', 'presto', 'oracle', 'mysql', 'postgres')
        empty_as_null: Nếu True, chuỗi rỗng / NaN / None sẽ xuất ra từ khóa 'NULL'
    """
    # 0. Xử lý giá trị NULL / Rỗng
    if val is None:
        return "NULL"
    
    # Nếu là kiểu float nan của pandas/numpy
    s_val = str(val).strip()
    if s_val == "" or s_val.lower() in ("nan", "none", "null"):
        return "NULL" if empty_as_null else "''"

    clean_s = clean_number_string(s_val)

    # 1. KIỂU INT (INTEGER / SMALLINT / TINYINT)
    if category == "INT":
        # Cắt đuôi .0 hoặc .00 của Excel (ví dụ: '20260921.0' -> '20260921')
        clean_s = re.sub(r'\.0+$', '', clean_s)
        try:
            # Xử lý cả dạng khoa học 1.5E+05 -> 150000 hoặc số float làm tròn
            num = round(float(clean_s))
            return str(num)
        except ValueError:
            return "NULL" if empty_as_null else "0"

    # 2. KIỂU BIGINT (Dành cho mã ID lớn, timestamp ms, số tiền cực lớn)
    if category == "BIGINT":
        clean_s = re.sub(r'\.0+$', '', clean_s)
        try:
            # Dùng Decimal để tránh mất độ chính xác của số lớn (> 64-bit hoặc vượt giới hạn float)
            d = Decimal(clean_s)
            return str(int(round(d)))
        except Exception:
            return "NULL" if empty_as_null else "0"

    # 3. KIỂU DECIMAL / NUMBER / FLOAT / DOUBLE
    if category == "DECIMAL":
        try:
            d = Decimal(clean_s)
            # Chuẩn hóa về chuỗi số thập phân trần (không nháy)
            return f"{d:f}"
        except Exception:
            return "NULL" if empty_as_null else "0.0"

    # 4. KIỂU VARCHAR / TEXT / STRING
    if category == "VARCHAR":
        # Nếu Excel vô tình đổi số tài khoản dài thành dạng khoa học (1.23456789E+11), khôi phục lại chuỗi số
        if re.match(r'^[+-]?\d+(?:\.\d+)?[eE][+-]?\d+$', s_val):
            try:
                big_num = int(round(Decimal(s_val)))
                return f"'{big_num}'"
            except Exception:
                pass
        
        # Luôn bọc trong dấu nháy đơn, tự động escape nháy đơn bên trong (' -> '')
        # Bảo toàn nguyên vẹn số 0 ở đầu (ví dụ: '00112233')
        return f"'{escape_sql_string(s_val)}'"

    # 5. KIỂU DATE
    if category == "DATE":
        std_date = s_val
        # Định dạng DD/MM/YYYY hoặc DD-MM-YYYY -> YYYY-MM-DD
        dmy_m = re.match(r'^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$', s_val)
        if dmy_m:
            d, m, y = dmy_m.group(1).zfill(2), dmy_m.group(2).zfill(2), dmy_m.group(3)
            std_date = f"{y}-{m}-{d}"
        # Định dạng YYYYMMDD -> YYYY-MM-DD
        elif re.match(r'^\d{8}$', s_val):
            std_date = f"{s_val[0:4]}-{s_val[4:6]}-{s_val[6:8]}"

        if dialect == "oracle":
            return f"TO_DATE('{std_date}', 'YYYY-MM-DD')"
        elif dialect == "spark":
            return f"DATE '{std_date}'"
        else:
            return f"'{std_date}'"

    # 6. KIỂU TIMESTAMP / DATETIME
    if category == "TIMESTAMP":
        if dialect == "oracle":
            return f"TO_TIMESTAMP('{s_val}', 'YYYY-MM-DD HH24:MI:SS')"
        elif dialect == "spark":
            return f"TIMESTAMP '{s_val}'"
        else:
            return f"'{s_val}'"

    # 7. KIỂU BOOLEAN
    if category == "BOOLEAN":
        lower_v = s_val.lower()
        if lower_v in ("true", "1", "t", "yes"):
            return "TRUE"
        if lower_v in ("false", "0", "f", "no"):
            return "FALSE"

    # Mặc định an toàn: Escape và bọc nháy đơn
    return f"'{escape_sql_string(s_val)}'"


class ExcelToSqlInserter:
    """
    Trình chuyển đổi dữ liệu từ Excel / CSV sang câu lệnh SQL INSERT.
    """

    def __init__(
        self,
        ddl_text: str,
        dialect: str = "spark",          # 'spark', 'presto', 'oracle', 'mysql', 'postgres'
        mode: str = "batch",             # 'batch' (INSERT nhiều dòng) hoặc 'single' (từng câu riêng)
        batch_size: int = 100,           # Số lượng dòng trong 1 lệnh INSERT batch
        empty_as_null: bool = True,
        has_header: bool = True
    ):
        self.ddl_text = ddl_text
        self.dialect = dialect.lower()
        self.mode = mode.lower()
        self.batch_size = max(1, batch_size)
        self.empty_as_null = empty_as_null
        self.has_header = has_header

        # Bóc tách DDL
        self.table_name, self.columns = parse_create_table_ddl(self.ddl_text)

    def generate_from_rows(self, data_rows: List[List[Any]], headers: Optional[List[str]] = None) -> str:
        """
        Sinh câu lệnh INSERT từ danh sách các dòng dữ liệu (dạng ma trận 2D).
        """
        if not self.columns:
            raise ValueError("Không tìm thấy định nghĩa cột nào trong câu lệnh CREATE TABLE DDL!")

        if not data_rows:
            return "-- KHÔNG CÓ DỮ LIỆU ĐỂ SINH CÂU LỆNH INSERT."

        # Xây dựng ánh xạ cột (Mapping) giữa DDL và Data Header
        col_names = [c.name for c in self.columns]
        mapping: Dict[str, Optional[int]] = {}

        if headers:
            clean_headers = [str(h).strip().lower() for h in headers]
            for col in self.columns:
                target_col = col.name.strip().lower()
                try:
                    idx = clean_headers.index(target_col)
                    mapping[col.name] = idx
                except ValueError:
                    mapping[col.name] = None
        else:
            # Map theo thứ tự cột từ trái sang phải
            for idx, col in enumerate(self.columns):
                mapping[col.name] = idx if idx < len(data_rows[0]) else None

        col_names_str = ", ".join(col_names)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = []
        lines.append("-- =========================================================================================")
        lines.append("-- SCRIPT TỰ ĐỘNG SINH: TOOL INSERT TABLE FROM EXCEL - TUANPV")
        lines.append(f"-- BẢNG ĐÍCH: {self.table_name}")
        lines.append(f"-- SỐ CỘT: {len(self.columns)} | TỔNG SỐ DÒNG DỮ LIỆU: {len(data_rows)}")
        lines.append(f"-- ENGINE SQL: {self.dialect.upper()} | CHẾ ĐỘ: {self.mode.upper()}")
        lines.append(f"-- THỜI GIAN SINH: {now_str}")
        lines.append("-- =========================================================================================\n")

        if self.mode == "single":
            for row in data_rows:
                vals = []
                for col in self.columns:
                    mapped_idx = mapping.get(col.name)
                    raw_val = row[mapped_idx] if mapped_idx is not None and mapped_idx < len(row) else None
                    vals.append(format_sql_cell(raw_val, col.category, self.dialect, self.empty_as_null))
                lines.append(f"INSERT INTO {self.table_name} ({col_names_str}) VALUES ({', '.join(vals)});")
        else:
            # Batch mode: INSERT INTO table (cols) VALUES (...), (...);
            for i in range(0, len(data_rows), self.batch_size):
                chunk = data_rows[i:i + self.batch_size]
                lines.append(f"INSERT INTO {self.table_name} (\n    {col_names_str}\n) VALUES")
                for c_idx, row in enumerate(chunk):
                    is_last = (c_idx == len(chunk) - 1)
                    vals = []
                    for col in self.columns:
                        mapped_idx = mapping.get(col.name)
                        raw_val = row[mapped_idx] if mapped_idx is not None and mapped_idx < len(row) else None
                        vals.append(format_sql_cell(raw_val, col.category, self.dialect, self.empty_as_null))
                    comma_or_semicolon = ";" if is_last else ","
                    lines.append(f"    ({', '.join(vals)}){comma_or_semicolon}")
                lines.append("")

        lines.append(f"-- ======================= HOÀN TẤT SINH DỮ LIỆU CHO {self.table_name} =======================\n")
        return "\n".join(lines)

    def generate_from_excel_file(self, file_path: str, sheet_name: Optional[str] = None) -> str:
        """
        Đọc trực tiếp file Excel (.xlsx, .xls) hoặc file CSV và sinh lệnh INSERT SQL.
        Yêu cầu thư viện: openpyxl hoặc pandas (được nạp tự động).
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

        # Trường hợp đọc file CSV
        if file_path.lower().endswith(".csv"):
            import csv
            with open(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
                reader = csv.reader(f)
                all_rows = list(reader)
                if not all_rows:
                    return "-- File CSV rỗng."
                headers = all_rows[0] if self.has_header else None
                data_rows = all_rows[1:] if self.has_header else all_rows
                return self.generate_from_rows(data_rows, headers=headers)

        # Trường hợp đọc file Excel (.xlsx, .xls)
        try:
            import openpyxl
            wb = openpyxl.load_workbook(file_path, data_only=True)
            ws = wb[sheet_name] if sheet_name and sheet_name in wb.sheetnames else wb.active
            all_rows = []
            for row in ws.iter_rows(values_only=True):
                # Bỏ qua dòng trống hoàn toàn
                if any(c is not None and str(c).strip() != "" for c in row):
                    all_rows.append(list(row))
            if not all_rows:
                return "-- File Excel không có dữ liệu."
            headers = [str(h) for h in all_rows[0]] if self.has_header else None
            data_rows = all_rows[1:] if self.has_header else all_rows
            return self.generate_from_rows(data_rows, headers=headers)
        except ImportError:
            # Fallback dùng pandas nếu có
            try:
                import pandas as pd
                df = pd.read_excel(file_path, sheet_name=sheet_name or 0)
                headers = list(df.columns) if self.has_header else None
                data_rows = df.values.tolist()
                return self.generate_from_rows(data_rows, headers=headers)
            except Exception as e:
                raise RuntimeError(f"Cần cài đặt openpyxl hoặc pandas để đọc file Excel: pip install openpyxl ({e})")

    def generate_from_tsv_text(self, tsv_text: str) -> str:
        """
        Đọc dữ liệu copy/paste từ Excel (dạng chuỗi phân tách bằng Tab '\\t' hoặc dấu phẩy ',').
        """
        lines = [line for line in tsv_text.strip().split('\n') if line.strip()]
        if not lines:
            return "-- Không có dữ liệu."

        # Tự động nhận diện dấu phân cách: Tab '\t' hoặc phẩy ','
        first_line = lines[0]
        delimiter = '\t' if '\t' in first_line else ','

        matrix = []
        for line in lines:
            parts = [p.strip() for p in line.split(delimiter)]
            matrix.append(parts)

        headers = matrix[0] if self.has_header else None
        data_rows = matrix[1:] if self.has_header else matrix
        return self.generate_from_rows(data_rows, headers=headers)


# ========================================================================================
# PHẦN 4: HÀM MAIN & CHẠY THỬ NGHIỆM DEMO (CLI RUNNER)
# ========================================================================================

def run_demo():
    """
    Chạy thử nghiệm minh họa trực tiếp cả 3 công cụ khi thực thi file này:
    python gent_config_tools.py
    """
    print("=" * 80)
    print("DEMO BỘ CÔNG CỤ TỰ ĐỘNG HÓA CẤU HÌNH & DỮ LIỆU SQL - TUANPV")
    print("=" * 80)

    # -------------------------------------------------------------
    # DEMO 1: TOOL GENT CONFIG FILE CIRCULAR
    # -------------------------------------------------------------
    print("\n[DEMO 1] SINH CẤU HÌNH THÔNG TƯ CHO MẪU BIỂU C00384 (CIRCULAR CONFIG)")
    cir_gen = CircularConfigGenerator(
        group_code="C00384",
        sheet_no="1",
        group_name="BÁO CÁO HOẠT ĐỘNG BẢO LÃNH",
        term_code="M",
        is_rerun=1,
        target_schema="prod_gold_ssd.sbv_report.",
        result_table="{{silver}}.{{silver_schema}}.c_pre_mbnt_cv2401 A",
        branch_mode="total",
        order_mode="join_subquery",
        where_clause="A.TRANS_TYPE = 'BUY' AND a.import_date = {{process_date}}"
    )
    cir_gen.add_column(CircularColumn(col_location="A", item_name="STT", source_col="A.STT", row_excel=20, data_type="VARCHAR"))
    cir_gen.add_column(CircularColumn(col_location="B", item_name="Tên Khách Hàng", source_col="A.CUST_NAME", row_excel=20, data_type="VARCHAR"))
    cir_gen.add_column(CircularColumn(col_location="C", item_name="Số Dư Cam Kết", source_col="A.COMMITTED_AMT", row_excel=20, data_type="NUMBER", unit="1000000"))
    cir_gen.add_order_item(OrderItem(stt="1", order_id=1, item_name="Bảo lãnh trong nước"))
    cir_gen.add_order_item(OrderItem(stt="2", order_id=2, item_name="Bảo lãnh nước ngoài"))

    sql_cir = cir_gen.generate_sql()
    print("-> Đã sinh thành công script cấu hình Thông tư:")
    print("\n".join(sql_cir.split("\n")[:30]))  # In 30 dòng đầu demo
    print("... (Xem đầy đủ trong file output)\n")

    # -------------------------------------------------------------
    # DEMO 2: TOOL GENT DATA FLOW C_PRE_SOURCE_CODE
    # -------------------------------------------------------------
    print("-" * 80)
    print("[DEMO 2] SINH LUỒNG DATA FLOW PIPELINE C_PRE_SOURCE_CODE (SPARK SQL)")
    presto_sample = """
    DELETE FROM prod_gold_ssd.sbv_report.c_pre_sao_ke_lending WHERE report_date = {{process_date}};
    INSERT INTO prod_gold_ssd.sbv_report.c_pre_sao_ke_lending
    SELECT 
        cust_id,
        date_diff('day', date_parse(open_date, '%Y-%m-%d'), current_date) as days_active,
        cardinality(txn_ids) as total_txns
    FROM silver.lending_daily;
    """
    pipe_gen = DataflowPipelineGenerator(
        report_code="c_pre_sao_ke_lending",
        grp_job="C_PRE_STAT",
        term_code="D",
        rerun_term=None,  # Để trống -> Tự động sinh NULL chuẩn xác
        gen_delete=True,
        auto_convert_spark=True
    )
    pipe_gen.parse_presto_script(presto_sample)
    sql_pipe = pipe_gen.generate_sql()
    print("-> Đã sinh thành công script Dataflow Pipeline:")
    print(sql_pipe)

    # -------------------------------------------------------------
    # DEMO 3: TOOL INSERT TABLE FROM EXCEL
    # -------------------------------------------------------------
    print("-" * 80)
    print("[DEMO 3] TỰ ĐỘNG CONVERT & SINH INSERT TỪ EXCEL/TSV VÀ DDL")
    sample_ddl = """
    CREATE TABLE prod_silver_ssd.t24.fnc_cic_eod_calc (
        branch_id VARCHAR(20),
        customer_cif VARCHAR(50),
        account_no VARCHAR(50),
        total_balance DECIMAL(18, 2),
        created_date DATE,
        etl_date INT
    ) USING parquet;
    """

    sample_excel_tsv = """branch_id\tcustomer_cif\taccount_no\ttotal_balance\tcreated_date\tetl_date
VN0010001\tCIF00008899\t0011002233001\t150,000,000.00\t2026-09-21\t20260921.0
VN0010002\tCông ty O'Reilly\t0011002233002\t285,450,000.50\t21/09/2026\t20260921
VN0020001\tCIF00009122\t0022003344001\t75,000,000\t20260921\t20260921.00
"""

    inserter = ExcelToSqlInserter(
        ddl_text=sample_ddl,
        dialect="spark",
        mode="batch",
        batch_size=50,
        empty_as_null=True,
        has_header=True
    )
    sql_insert = inserter.generate_from_tsv_text(sample_excel_tsv)
    print("-> Đã sinh thành công script INSERT dữ liệu:")
    print(sql_insert)
    print("=" * 80)
    print("HOÀN TẤT DEMO 100%! BẠN CÓ THỂ IMPORT CÁC CLASS TRÊN VÀO CODE PYTHON CỦA MÌNH.")
    print("=" * 80)


if __name__ == "__main__":
    run_demo()

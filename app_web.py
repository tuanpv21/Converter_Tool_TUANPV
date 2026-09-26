#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web UI Tool: Presto <-> Spark SQL Bi-directional Transpiler with Authentication
Tích hợp:
- Xác thực đăng nhập (Login / Logout / Change Password)
- Quản lý User bằng SQLite cục bộ + Hỗ trợ Cloud Secrets (ADMIN_USERNAME, ADMIN_PASSWORD)
- Bảo vệ API /api/convert bằng Session Token HttpOnly
- Giao diện Dark Theme hiện đại cho Ngân hàng
"""

import http.server
import socketserver
import json
import webbrowser
import os
import sys
import sqlite3
import hashlib
import secrets
import time
from http import cookies

try:
    from convert_presto_to_spark import convert_sql
except ImportError:
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from convert_presto_to_spark import convert_sql

# ==========================================
# DOCX DYNAMIC TEMPLATE ENGINE & PROCESSOR
# ==========================================
import unicodedata
import zipfile
import re
import io
from docxtpl import DocxTemplate, RichText

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
os.makedirs(TEMPLATES_DIR, exist_ok=True)

FIELD_LABELS_MAP = {
    "system_name": "Tên hệ thống",
    "system_code": "Mã hệ thống",
    "project_name": "Tên dự án",
    "author": "Người thực hiện",
    "reviewer": "Người kiểm duyệt",
    "approver": "Người phê duyệt",
    "deployer": "Người triển khai",
    "department": "Đơn vị / Khối",
    "doc_date": "Ngày tạo tài liệu",
    "deploy_date": "Ngày triển khai",
    "deploy_time": "Thời gian triển khai",
    "version": "Phiên bản",
    "release_version": "Phiên bản phát hành",
    "target_environment": "Môi trường triển khai",
    "downtime": "Thời gian gián đoạn (Downtime)",
    "contact_phone": "Số điện thoại liên hệ",
    "scope": "Phạm vi tài liệu",
    "objective": "Mục tiêu",
    "architecture_overview": "Kiến trúc tổng thể",
    "tech_stack": "Công nghệ sử dụng",
    "data_flow": "Luồng dữ liệu (Data Flow)",
    "environment_requirements": "Yêu cầu môi trường & Cấu hình",
    "security_notes": "Yêu cầu bảo mật & Tuân thủ",
    "prerequisites": "Điều kiện tiên quyết",
    "release_notes": "Nội dung phát hành",
    "deploy_steps": "Các bước triển khai",
    "verification_steps": "Kiểm tra sau triển khai",
    "rollback_trigger": "Điều kiện kích hoạt Rollback",
    "rollback_steps": "Các bước khôi phục (Rollback)",
    "backup_plan": "Kế hoạch sao lưu dự phòng",
    "ten_du_an": "Tên dự án",
    "nguoi_phu_trach": "Người phụ trách",
    "noi_dung_tong_quan": "Nội dung tổng quan",
}

def to_clean_identifier(text: str) -> str:
    """Chuyển đổi chuỗi tiếng Việt hoặc có dấu cách thành định dạng identifier snake_case hợp lệ cho Jinja2"""
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    text = text.replace('đ', 'd').replace('Đ', 'D')
    text = re.sub(r'[^a-zA-Z0-9_]+', '_', text).strip('_')
    return text.lower()

class SmartRichText(RichText):
    """RichText tự động chèn <w:br/> khi gặp dấu xuống dòng \n hoặc \r"""
    def add(self, text, **kwargs):
        if isinstance(text, str) and ("\n" in text or "\r" in text):
            lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
            for idx, line in enumerate(lines):
                if line:
                    super(SmartRichText, self).add(line, **kwargs)
                if idx < len(lines) - 1:
                    self.xml += "<w:r><w:br/></w:r>"
            return
        super(SmartRichText, self).add(text, **kwargs)

class AutoCleanDocxTemplate(DocxTemplate):
    """
    Tự động chuẩn hóa các thẻ Jinja2 chứa dấu cách hoặc tiếng Việt
    như {{ nội dung thiết lập }} hay {{Quản trị dữ liệu}}
    thành cú pháp hợp lệ mà Jinja2 hiểu được mà không gây lỗi TemplateSyntaxError.
    Đồng thời lưu lại nhãn nguyên bản để hiển thị thân thiện trên giao diện Form.
    """
    def __init__(self, *args, **kwargs):
        self.var_labels = {}
        super().__init__(*args, **kwargs)

    def patch_xml(self, src_xml):
        src_xml = super().patch_xml(src_xml)
        
        # 1. Chuẩn hóa thẻ {{ ... }}
        def fix_tag(match):
            inner = match.group(1).strip()
            # Bỏ qua các từ khóa jinja đặc biệt
            if not any(inner.startswith(kw) for kw in ['for ', 'if ', 'set ', 'include ', 'elif ', 'else', 'endif', 'endfor']):
                prefix = ''
                if inner.startswith('r '):
                    prefix = 'r '
                    inner = inner[2:].strip()
                
                # Nếu là thuộc tính đối tượng: item.thuoc_tinh
                if '.' in inner:
                    parts = inner.split('.')
                    clean_parts = [to_clean_identifier(p) for p in parts]
                    clean = '.'.join(clean_parts)
                    return '{{ ' + prefix + clean + ' }}'
                
                clean = to_clean_identifier(inner)
                if clean:
                    if clean != inner and clean not in self.var_labels:
                        self.var_labels[clean] = inner
                    return '{{ ' + prefix + clean + ' }}'
            return match.group(0)

        src_xml = re.sub(r'\{\{\s*([^{}]+?)\s*\}\}', fix_tag, src_xml)

        # 2. Chuẩn hóa thẻ vòng lặp {% ... %}
        def fix_directive(match):
            inner = match.group(1).strip()
            m_for = re.match(r'^(tr\s+|tc\s+)?for\s+([a-zA-Z_]\w*)\s+in\s+(.+)$', inner)
            if m_for:
                prefix = m_for.group(1) or ''
                item_var = m_for.group(2)
                raw_list = m_for.group(3).strip()
                clean_list = to_clean_identifier(raw_list)
                if clean_list != raw_list and clean_list not in self.var_labels:
                    self.var_labels[clean_list] = raw_list
                return f'{{% {prefix}for {item_var} in {clean_list} %}}'
            return match.group(0)

        src_xml = re.sub(r'\{%\s*(.+?)\s*%\}', fix_directive, src_xml)
        return src_xml

def prettify_name(var_name: str, original_label: str = None) -> str:
    if original_label and original_label.strip():
        return original_label.strip()
    low = var_name.lower().strip()
    if low in FIELD_LABELS_MAP:
        return FIELD_LABELS_MAP[low]
    words = var_name.replace("_", " ").strip().split()
    return " ".join(w.capitalize() for w in words) if words else var_name

def is_textarea_hint(var_name: str) -> bool:
    low = var_name.lower()
    multiline_keywords = [
        "overview", "desc", "content", "plan", "step", "note", "check", 
        "spec", "design", "req", "detail", "huong_dan", "noi_dung", "mo_ta", 
        "ghi_chu", "danh_sach", "scope", "objective", "architecture", 
        "tong_quan", "muc_tieu", "pham_vi", "luong_du_lieu", "bao_mat"
    ]
    return any(kw in low for kw in multiline_keywords)

def _process_data_for_render(data):
    if isinstance(data, dict):
        processed = {}
        for k, v in data.items():
            if isinstance(v, str):
                if "\n" in v or "\r" in v:
                    rt = SmartRichText()
                    rt.add(v)
                    processed[k] = rt
                else:
                    processed[k] = v
            elif isinstance(v, list):
                processed[k] = [_process_data_for_render(item) for item in v]
            elif isinstance(v, dict):
                processed[k] = _process_data_for_render(v)
            else:
                processed[k] = v
        return processed
    elif isinstance(data, list):
        return [_process_data_for_render(item) for item in data]
    return data

def list_available_templates():
    if not os.path.exists(TEMPLATES_DIR):
        os.makedirs(TEMPLATES_DIR, exist_ok=True)
    res = []
    for f in sorted(os.listdir(TEMPLATES_DIR)):
        if f.lower().endswith(".docx") and not f.startswith("~$") and not f.startswith("."):
            full_path = os.path.join(TEMPLATES_DIR, f)
            try:
                size_kb = round(os.path.getsize(full_path) / 1024, 1)
            except Exception:
                size_kb = 0.0
            display_name = f.replace("_", " ").replace(".docx", "")
            if "deployment_guide" in f:
                display_name = "📘 Mẫu Tài Liệu Triển Khai (Deployment Guide)"
            elif "technical_spec" in f:
                display_name = "📙 Mẫu Đặc Tả Kỹ Thuật (Technical Spec)"
            elif "bang_tu_no_dong" in f or "Vi_du" in f:
                display_name = "📗 Mẫu Bảng Tự Nở Dòng (Table Auto-Expand)"
            res.append({
                "filename": f,
                "display_name": display_name,
                "size_kb": size_kb
            })
    return res

def inspect_template_content(file_bytes: bytes) -> dict:
    tpl = AutoCleanDocxTemplate(io.BytesIO(file_bytes))
    try:
        raw_vars = tpl.get_undeclared_template_variables()
    except Exception:
        raw_vars = set()

    # Đọc các file XML để tìm kiếm cấu trúc bảng lặp
    xml_texts = []
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
            for name in z.namelist():
                if name.endswith(".xml"):
                    raw = z.read(name).decode("utf-8", errors="ignore")
                    text = re.sub(r"<[^>]+>", "", raw)
                    xml_texts.append(text)
    except Exception:
        pass

    combined_text = "\n".join(xml_texts)
    loop_pattern = re.compile(
        r"{%\s*(?:tr\s+|tc\s+)?for\s+([a-zA-Z_]\w*)\s+in\s+([a-zA-Z_]\w*)\s*%}(.*?){%\s*(?:tr\s+|tc\s+)?endfor\s*%}",
        re.DOTALL
    )

    loop_fields = []
    loop_vars = set()
    item_vars = set()

    for match in loop_pattern.finditer(combined_text):
        item_var, loop_var, body = match.groups()
        loop_vars.add(loop_var)
        item_vars.add(item_var)

        prop_matches = re.findall(rf"{{\s*{item_var}\.([a-zA-Z_]\w*)\s*}}", body)
        seen_props = []
        for p in prop_matches:
            if p not in seen_props:
                seen_props.append(p)

        fields = [
            {"name": prop, "label": prettify_name(prop, tpl.var_labels.get(prop))}
            for prop in seen_props
        ]
        loop_fields.append({
            "loop_var": loop_var,
            "item_var": item_var,
            "label": prettify_name(loop_var, tpl.var_labels.get(loop_var)),
            "fields": fields
        })

    scalar_fields = []
    for var in sorted(raw_vars):
        if var in loop_vars or var in item_vars or var == "loop":
            continue
        orig_label = tpl.var_labels.get(var)
        scalar_fields.append({
            "name": var,
            "label": prettify_name(var, orig_label),
            "type": "textarea" if is_textarea_hint(var) else "text",
            "value": ""
        })

    return {
        "scalar_fields": scalar_fields,
        "loop_fields": loop_fields
    }

def render_dynamic_template(template_bytes: bytes, data: dict) -> io.BytesIO:
    tpl = AutoCleanDocxTemplate(io.BytesIO(template_bytes))
    processed_data = _process_data_for_render(data)
    enriched_data = dict(processed_data)
    for k, v in list(processed_data.items()):
        clean_k = to_clean_identifier(k)
        if clean_k not in enriched_data:
            enriched_data[clean_k] = v
    tpl.render(enriched_data)
    out = io.BytesIO()
    tpl.save(out)
    out.seek(0)
    return out

def render_deployment_document(data: dict) -> io.BytesIO:
    tpl_path = os.path.join(TEMPLATES_DIR, "template_deployment_guide.docx")
    with open(tpl_path, "rb") as f:
        tpl_bytes = f.read()
    return render_dynamic_template(tpl_bytes, data)

def render_technical_document(data: dict) -> io.BytesIO:
    tpl_path = os.path.join(TEMPLATES_DIR, "template_technical_spec.docx")
    with open(tpl_path, "rb") as f:
        tpl_bytes = f.read()
    return render_dynamic_template(tpl_bytes, data)

def get_sample_deployment_data() -> dict:
    return {
        "project_name": "Nâng cấp Hệ thống Thanh toán & Chuyển tiền Liên ngân hàng",
        "system_code": "PAYMENT_GW_2026",
        "release_version": "v3.4.0-RC2",
        "deployer": "Nguyễn Văn Tuấn (DevOps Lead)",
        "approver": "Trần Thị Lan (Head of IT Ops)",
        "deploy_time": "23:00 - 26/09/2026",
        "target_environment": "Production - Cluster PROD-HN01",
        "downtime": "15 phút (Maintenance Window)",
        "contact_phone": "0988.xxx.xxx (Hotline On-call)",
        "prerequisites": "1. Hoàn tất backup database PostgreSQL và snapshot VM\n2. Thông báo gián đoạn dịch vụ tới các chi nhánh\n3. Kiểm tra kết nối mạng giữa các node và gateway",
        "release_notes": "1. Tối ưu hiệu năng truy vấn giao dịch Presto / Spark SQL\n2. Sửa lỗi timeout kết nối cổng thanh toán ngoài\n3. Bổ sung tính năng sinh tài liệu kỹ thuật tự động",
        "deploy_steps": [
            {
                "time": "23:00 - 23:15",
                "action_detail": "Chuyển hướng lưu lượng sang DR Site và dừng dịch vụ trên PROD-HN01",
                "executor": "Nguyễn Văn Tuấn (DevOps)",
                "expected_result": "Không còn connection active vào gateway"
            },
            {
                "time": "23:15 - 23:30",
                "action_detail": "Thực hiện apply manifest k8s và chạy database schema migration",
                "executor": "Trần Thị Lan (DBA Lead)",
                "expected_result": "Database migration exit code 0, không có table lock"
            },
            {
                "time": "23:30 - 23:45",
                "action_detail": "Khởi động phiên bản mới v3.4.0 và kiểm tra health check API",
                "executor": "Nguyễn Văn Tuấn (DevOps)",
                "expected_result": "Pods Ready 3/3, endpoint /healthz trả về 200 OK"
            }
        ],
        "verification_steps": "1. Kiểm tra endpoint /healthz và /metrics trả về status 200 OK\n2. Thực hiện 05 giao dịch mẫu giả lập thành công\n3. Kiểm tra log hệ thống không phát sinh error/critical",
        "rollback_trigger": "Phát sinh lỗi kết nối Core Banking > 3% hoặc tỷ lệ thất bại giao dịch vượt 0.5% sau 10 phút triển khai",
        "rollback_steps": "1. Khôi phục container về phiên bản v3.3.9 qua Helm rollback\n2. Restore dữ liệu từ bản backup trước triển khai\n3. Khởi động lại dịch vụ và thông báo cho đội trực vận hành",
        "backup_plan": "Sao lưu toàn bộ database và snapshot storage trước 22:30 cùng ngày"
    }

def get_sample_technical_data() -> dict:
    return {
        "project_name": "Hệ thống Phân tích Dữ liệu Giao dịch Tài chính Đa nguồn",
        "system_code": "DATA_ANALYTICS_PLATFORM",
        "version": "v2.1.0",
        "doc_date": "26/09/2026",
        "department": "Khối Công nghệ & Chuyển đổi Số",
        "author": "Phùng Văn Tuấn (Solutions Architect)",
        "reviewer": "Lê Hoàng Long (Chief Technology Officer)",
        "scope": "Phạm vi tài liệu mô tả kiến trúc kỹ thuật, luồng xử lý dữ liệu và đặc tả thành phần của hệ thống phân tích giao dịch ngân hàng quy mô lớn.",
        "objective": "Cung cấp nền tảng xử lý dữ liệu gần thời gian thực (near real-time) hỗ trợ đối soát giao dịch và phát hiện gian lận tự động.",
        "architecture_overview": "Hệ thống kiến trúc theo mô hình Microservices kết hợp Event-Driven:\n- Ingestion Layer: Apache Kafka thu nhận log giao dịch từ Core Banking\n- Processing Layer: Apache Spark & Presto xử lý tính toán phân tán\n- Storage Layer: HDFS / MinIO lưu trữ Data Lake, PostgreSQL lưu metadata\n- API Gateway & Web UI: Nền tảng phân tích và sinh báo cáo tự động",
        "tech_stack": "- Ngôn ngữ & Framework: Python 3.10, PySpark, FastAPI\n- Phân tích & Xử lý: Apache Spark 3.4, Presto/Trino\n- Lưu trữ: MinIO S3-compatible, PostgreSQL 15, Redis Cache\n- Giám sát: Prometheus, Grafana, OpenTelemetry",
        "data_flow": "1. Core Banking gửi sự kiện giao dịch qua Kafka Topic\n2. Spark Streaming tiêu thụ dữ liệu, làm sạch và chuẩn hóa schema\n3. Ghi dữ liệu vào Data Lake (Parquet/Iceberg format)\n4. Web Dashboard truy vấn qua Presto engine để hiển thị số liệu",
        "environment_requirements": "- CPU: Tối thiểu 16 Cores (Production khuyến nghị 64 Cores)\n- RAM: Tối thiểu 32GB RAM (Khuyến nghị 128GB cho Spark Workers)\n- Storage: 2TB NVMe SSD cho caching và WAL logs\n- Hệ điều hành: RHEL 8.8 / Ubuntu 22.04 LTS",
        "security_notes": "- Toàn bộ kết nối nội bộ bắt buộc mã hóa mTLS\n- Dữ liệu định danh khách hàng (PII) được băm một chiều (SHA-256 + Salt) hoặc mã hóa AES-256\n- Phân quyền người dùng theo vai trò RBAC và lưu vết đầy đủ trong Audit Log",
        "components": [
            {"name": "API Service", "description": "Tiếp nhận request từ UI và phân quyền JWT", "io": "REST / JSON"},
            {"name": "Transpiler Core", "description": "Chuyển đổi cú pháp Presto <-> Spark SQL", "io": "SQL In / Out"},
            {"name": "Doc Generator", "description": "Tự động sinh tài liệu Word theo mẫu động", "io": "Word (.docx)"}
        ]
    }

# Gán doc_service trỏ vào chính module hiện tại để tương thích hoàn toàn
doc_service = sys.modules[__name__]


PORT = int(os.environ.get("PORT", 7860))
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auth.db")

# Tài khoản mặc định từ biến môi trường (Ưu tiên khi deploy lên Cloud)
DEFAULT_ADMIN_USER = os.environ.get("ADMIN_USERNAME", "admin")
DEFAULT_ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "VualidonMSB")

# ==========================================
# 1. DATABASE & AUTHENTICATION HELPERS
# ==========================================
def hash_password(password: str, salt: str = None) -> tuple:
    if not salt:
        salt = secrets.token_hex(16)
    hashed = hashlib.sha256((password + salt).encode('utf-8')).hexdigest()
    return hashed, salt

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Bảng users
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            created_at TEXT
        )
    ''')
    # Bảng sessions
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL
        )
    ''')
    
    # Tạo user admin mặc định nếu chưa có
    cur.execute('SELECT username FROM users WHERE username = ?', (DEFAULT_ADMIN_USER,))
    if not cur.fetchone():
        h, s = hash_password(DEFAULT_ADMIN_PASS)
        cur.execute(
            'INSERT INTO users (username, password_hash, salt, role, created_at) VALUES (?, ?, ?, ?, ?)',
            (DEFAULT_ADMIN_USER, h, s, 'admin', time.strftime('%Y-%m-%d %H:%M:%S'))
        )
    conn.commit()
    conn.close()

def verify_credentials(username, password) -> bool:
    # 1. Kiểm tra khớp với biến môi trường Cloud Secrets trước
    if username == DEFAULT_ADMIN_USER and password == DEFAULT_ADMIN_PASS:
        return True
    
    # 2. Kiểm tra trong SQLite DB
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT password_hash, salt FROM users WHERE username = ?', (username,))
    row = cur.fetchone()
    conn.close()
    
    if not row:
        return False
    
    stored_hash, salt = row
    calculated_hash, _ = hash_password(password, salt)
    return secrets.compare_digest(stored_hash, calculated_hash)

def create_session(username) -> str:
    session_id = secrets.token_urlsafe(32)
    now = time.time()
    expires_at = now + (7 * 24 * 3600) # Hạn 7 ngày
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # Dọn dẹp session cũ hết hạn
    cur.execute('DELETE FROM sessions WHERE expires_at < ?', (now,))
    cur.execute('INSERT INTO sessions (session_id, username, created_at, expires_at) VALUES (?, ?, ?, ?)',
                (session_id, username, now, expires_at))
    conn.commit()
    conn.close()
    return session_id

def validate_session(session_id) -> str:
    if not session_id:
        return None
    now = time.time()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('SELECT username FROM sessions WHERE session_id = ? AND expires_at > ?', (session_id, now))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def destroy_session(session_id):
    if not session_id:
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('DELETE FROM sessions WHERE session_id = ?', (session_id,))
    conn.commit()
    conn.close()

def update_user_password(username, new_password):
    h, s = hash_password(new_password)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('UPDATE users SET password_hash = ?, salt = ? WHERE username = ?', (h, s, username))
    conn.commit()
    conn.close()

# ==========================================
# 2. HTML INTERFACE WITH LOGIN & DASHBOARD
# ==========================================
HTML_PAGE = """<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Presto ⇄ Spark SQL Transpiler - TUANPV</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #0b1329; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
    
    /* Header */
    header { background: #131f37; border-bottom: 1px solid #1e293b; padding: 12px 24px; display: flex; justify-content: space-between; align-items: center; }
    .logo { font-size: 17px; font-weight: 700; color: #38bdf8; display: flex; align-items: center; gap: 8px; }
    .badge { background: #0284c7; color: #ffffff; font-size: 11px; padding: 3px 8px; border-radius: 9999px; font-weight: 600; }
    .user-pill { display: flex; align-items: center; gap: 10px; font-size: 13px; color: #94a3b8; }
    .user-pill strong { color: #f8fafc; }
    
    /* Buttons */
    button { font-family: inherit; font-size: 13px; font-weight: 600; padding: 8px 16px; border-radius: 6px; border: none; cursor: pointer; transition: all 0.15s ease; display: inline-flex; align-items: center; gap: 6px; }
    .btn-primary { background: #2563eb; color: white; }
    .btn-primary:hover { background: #1d4ed8; }
    .btn-switch { background: #0284c7; color: white; }
    .btn-switch:hover { background: #0369a1; }
    .btn-secondary { background: #1e293b; color: #cbd5e1; border: 1px solid #334155; }
    .btn-secondary:hover { background: #334155; }
    .btn-danger { background: #ef4444; color: white; }
    .btn-danger:hover { background: #dc2626; }
    .btn-copy { background: #059669; color: white; }
    .btn-copy:hover { background: #047857; }

    /* App Container */
    .main-container { flex: 1; display: flex; flex-direction: column; padding: 16px 24px; gap: 14px; }
    .toolbar { display: flex; gap: 10px; align-items: center; justify-content: space-between; flex-wrap: wrap; }
    .btn-group { display: flex; gap: 8px; align-items: center; }
    .panels { display: flex; gap: 16px; flex: 1; min-height: 520px; }
    .panel { flex: 1; display: flex; flex-direction: column; background: #131f37; border-radius: 8px; border: 1px solid #1e293b; overflow: hidden; }
    .panel-header { background: #0f172a; padding: 10px 16px; font-size: 13px; font-weight: 600; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #1e293b; }
    .panel-header.left { color: #38bdf8; }
    .panel-header.right { color: #34d399; }
    textarea { flex: 1; width: 100%; border: none; outline: none; background: #090e1a; color: #f8fafc; font-family: "Consolas", "Courier New", monospace; font-size: 13px; line-height: 1.5; padding: 16px; resize: none; tab-size: 4; }
    .status-bar { font-size: 12px; color: #94a3b8; padding: 4px 6px; }

    /* Modal & Login Form */
    .modal-overlay { position: fixed; inset: 0; background: rgba(3, 7, 18, 0.85); backdrop-filter: blur(4px); display: flex; align-items: center; justify-content: center; z-index: 999; }
    .auth-card { background: #131f37; border: 1px solid #334155; border-radius: 12px; padding: 32px; width: 100%; max-width: 400px; box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5); }
    .auth-title { font-size: 20px; font-weight: 700; color: #f8fafc; margin-bottom: 6px; text-align: center; }
    .auth-sub { font-size: 13px; color: #94a3b8; margin-bottom: 24px; text-align: center; }
    .form-group { margin-bottom: 16px; }
    .form-label { display: block; font-size: 12px; font-weight: 600; color: #cbd5e1; margin-bottom: 6px; }
    .form-input { width: 100%; padding: 10px 14px; background: #090e1a; border: 1px solid #334155; border-radius: 6px; color: white; font-size: 14px; outline: none; transition: border-color 0.15s; }
    .form-input:focus { border-color: #38bdf8; }
    .auth-error { background: #7f1d1d; color: #fecaca; padding: 10px; border-radius: 6px; font-size: 12px; margin-bottom: 16px; display: none; }

    .toast { position: fixed; bottom: 20px; right: 20px; background: #10b981; color: white; padding: 10px 18px; border-radius: 6px; font-size: 13px; font-weight: 600; opacity: 0; transition: opacity 0.2s ease; pointer-events: none; z-index: 1000; }
    .toast.show { opacity: 1; }
  </style>
</head>
<body>

  <!-- LOGIN MODAL -->
  <div id="login-modal" class="modal-overlay">
    <div class="auth-card">
      <div style="text-align: center; margin-bottom: 12px; font-size: 32px;">🔐</div>
      <h2 class="auth-title">Xác Thực Hệ Thống</h2>
      <p class="auth-sub">Vui lòng đăng nhập để truy cập SQL Transpiler</p>
      
      <div id="login-error" class="auth-error"></div>

      <form id="login-form" onsubmit="handleLogin(event)">
        <div class="form-group">
          <label class="form-label">Tên đăng nhập (Username)</label>
          <input type="text" id="login-username" class="form-input" required placeholder="Nhập username..." />
        </div>
        <div class="form-group">
          <label class="form-label">Mật khẩu (Password)</label>
          <input type="password" id="login-password" class="form-input" required placeholder="Nhập mật khẩu..." />
        </div>
        <button type="submit" class="btn-primary" style="width: 100%; justify-content: center; padding: 11px; margin-top: 8px;">
          Đăng Nhập ➔
        </button>
      </form>
    </div>
  </div>

  <!-- CHANGE PASSWORD MODAL -->
  <div id="pwd-modal" class="modal-overlay" style="display: none;">
    <div class="auth-card">
      <h2 class="auth-title">Đổi Mật Khẩu</h2>
      <p class="auth-sub">Cập nhật mật khẩu tài khoản của bạn</p>
      <div id="pwd-error" class="auth-error"></div>
      <form onsubmit="handleChangePassword(event)">
        <div class="form-group">
          <label class="form-label">Mật khẩu hiện tại</label>
          <input type="password" id="old-pwd" class="form-input" required />
        </div>
        <div class="form-group">
          <label class="form-label">Mật khẩu mới</label>
          <input type="password" id="new-pwd" class="form-input" required minlength="6" />
        </div>
        <div style="display: flex; gap: 8px; margin-top: 16px;">
          <button type="submit" class="btn-primary" style="flex: 1; justify-content: center;">Lưu Mật Khẩu</button>
          <button type="button" class="btn-secondary" onclick="closePwdModal()">Hủy</button>
        </div>
      </form>
    </div>
  </div>

  <!-- APP HEADER -->
  <header>
    <div class="logo">
      <span>⚡ Presto ⇄ Spark SQL Transpiler 2 Chiều</span>
      <span class="badge">TUANPV</span>
    </div>
    <div class="user-pill">
      <span>👤 Đăng nhập: <strong id="current-user">...</strong></span>
      <button class="btn-secondary" style="padding: 4px 10px; font-size: 11px;" onclick="openPwdModal()">🔑 Đổi MK</button>
      <button class="btn-danger" style="padding: 4px 10px; font-size: 11px;" onclick="handleLogout()">🚪 Đăng xuất</button>
    </div>
  </header>

  <!-- APP CONTENT -->
  <div class="main-container">
    <div class="toolbar">
      <div class="btn-group">
        <button id="btn-convert" class="btn-primary" onclick="convertSQL()">🚀 Chuyển đổi sang Spark SQL (Ctrl + Enter)</button>
        <button id="btn-toggle-mode" class="btn-switch" onclick="toggleMode()">⇄ Đổi chiều: Presto ➔ Spark SQL</button>

        <!-- Presto Version Dropdown -->
        <div style="display: inline-flex; align-items: center; gap: 6px; background: #131f37; border: 1px solid #334155; padding: 4px 10px; border-radius: 6px;">
          <span style="font-size: 12px; font-weight: 600; color: #94a3b8;">Phiên bản Presto:</span>
          <select id="presto-version" onchange="onPrestoVersionChange()" style="background: #090e1a; color: #38bdf8; border: 1px solid #0284c7; border-radius: 4px; padding: 4px 8px; font-size: 12px; font-weight: 600; outline: none; cursor: pointer;">
            <option value="presto" selected>PrestoDB (0.2xx / EMR / Athena v2)</option>
            <option value="trino">Trino (PrestoSQL 330+ / Trino 400+ / Athena v3)</option>
            <option value="athena">AWS Athena Engine</option>
          </select>
        </div>

        <button class="btn-secondary" onclick="swapContent()">⇆ Đổi chỗ nội dung</button>
        <button class="btn-secondary" onclick="loadSample()">📄 Mẫu thử</button>
        <button class="btn-secondary" onclick="clearAll()">🗑️ Xóa trắng</button>
      </div>
      <div class="status-bar" id="status-bar">Sẵn sàng</div>
    </div>

    <div class="panels">
      <!-- PANEL INPUT -->
      <div class="panel">
        <div class="panel-header left">
          <span id="title-left">📥 INPUT: Presto SQL</span>
          <span id="sub-left" style="font-size: 11px; color: #64748b;">Nguồn: Presto / Trino / Athena</span>
        </div>
        <textarea id="sql-input" placeholder="Nhập hoặc dán câu lệnh SQL vào đây..."></textarea>
      </div>

      <!-- PANEL OUTPUT -->
      <div class="panel">
        <div class="panel-header right">
          <span id="title-right">📤 OUTPUT: Spark SQL</span>
          <button class="btn-copy" onclick="copyResult()">📋 Sao chép</button>
        </div>
        <textarea id="sql-output" readonly placeholder="Kết quả chuyển đổi sẽ hiển thị ở đây..."></textarea>
      </div>
    </div>
  </div>

  <div id="toast" class="toast">Thông báo</div>

  <script>
    let currentMode = 'presto2spark';

    const samplePresto = `-- Vi du truy van Presto tren S3 (Bao ve bien tham so {{process_date}})
SELECT 
    cast(cust_id as varchar) as cust_id_str,
    json_extract_scalar(event_payload, '$.device_info.os') as os_type,
    date_diff('day', date_parse(sign_up_date, '%Y-%m-%d'), current_date) as active_days,
    cardinality(transaction_ids) as total_txns,
    contains(transaction_ids, 'TXN_VIP_999') as is_vip_txn,
    array_join(tag_names, ', ') as tags_str,
    approx_distinct(session_token) as approx_sessions
FROM customer_activity_logs
WHERE date_add('day', -30, current_date) <= date_parse(log_date, '%Y-%m-%d')
  AND partition_date = '{{process_date}}'
GROUP BY 1, 2, 3, 4, 5, 6, 7;`;

    const sampleSpark = `-- Vi du truy van Spark SQL tren S3 (Bao ve bien tham so {{process_date}})
SELECT 
    cast(cust_id as string) as cust_id_str,
    get_json_object(event_payload, '$.device_info.os') as os_type,
    datediff(current_date(), to_date(sign_up_date, 'yyyy-MM-dd')) as active_days,
    size(transaction_ids) as total_txns,
    array_contains(transaction_ids, 'TXN_VIP_999') as is_vip_txn,
    concat_ws(', ', tag_names) as tags_str,
    approx_count_distinct(session_token) as approx_sessions
FROM customer_activity_logs
WHERE date_add(current_date(), -30) <= to_date(log_date, 'yyyy-MM-dd')
  AND partition_date = '{{process_date}}'
GROUP BY 1, 2, 3, 4, 5, 6, 7;`;

    // 1. CHECK SESSION KHI MO TRANG
    async function checkAuth() {
      try {
        const res = await fetch('/api/me');
        const data = await res.json();
        if (data.logged_in) {
          document.getElementById('login-modal').style.display = 'none';
          document.getElementById('current-user').innerText = data.username;
        } else {
          document.getElementById('login-modal').style.display = 'flex';
        }
      } catch (err) {
        document.getElementById('login-modal').style.display = 'flex';
      }
    }

    // 2. XU LY DANG NHAP
    async function handleLogin(e) {
      e.preventDefault();
      const u = document.getElementById('login-username').value.trim();
      const p = document.getElementById('login-password').value;
      const errBox = document.getElementById('login-error');
      errBox.style.display = 'none';

      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username: u, password: p })
        });
        const data = await res.json();
        if (data.status === 'ok') {
          document.getElementById('login-modal').style.display = 'none';
          document.getElementById('current-user').innerText = data.username;
          showToast("Đăng nhập thành công!");
          loadSample();
        } else {
          errBox.innerText = data.error || "Sai tên đăng nhập hoặc mật khẩu!";
          errBox.style.display = 'block';
        }
      } catch (err) {
        errBox.innerText = "Lỗi kết nối máy chủ!";
        errBox.style.display = 'block';
      }
    }

    // 3. XU LY DANG XUAT
    async function handleLogout() {
      await fetch('/api/logout', { method: 'POST' });
      document.getElementById('login-modal').style.display = 'flex';
      document.getElementById('login-username').value = '';
      document.getElementById('login-password').value = '';
      showToast("Đã đăng xuất");
    }

    // 4. DOI MAT KHAU
    function openPwdModal() {
      document.getElementById('pwd-modal').style.display = 'flex';
      document.getElementById('pwd-error').style.display = 'none';
    }
    function closePwdModal() {
      document.getElementById('pwd-modal').style.display = 'none';
    }
    async function handleChangePassword(e) {
      e.preventDefault();
      const oldPwd = document.getElementById('old-pwd').value;
      const newPwd = document.getElementById('new-pwd').value;
      const errBox = document.getElementById('pwd-error');
      errBox.style.display = 'none';

      try {
        const res = await fetch('/api/change-password', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ old_password: oldPwd, new_password: newPwd })
        });
        const data = await res.json();
        if (data.status === 'ok') {
          closePwdModal();
          showToast("Đổi mật khẩu thành công!");
        } else {
          errBox.innerText = data.error;
          errBox.style.display = 'block';
        }
      } catch (err) {
        errBox.innerText = "Lỗi kết nối máy chủ!";
        errBox.style.display = 'block';
      }
    }

    // 5. CHUYEN DOI SQL
    function getPrestoDialect() {
      const select = document.getElementById('presto-version');
      return select ? select.value : 'presto';
    }

    function getPrestoDisplayName() {
      const dialect = getPrestoDialect();
      if (dialect === 'trino') return 'Trino';
      if (dialect === 'athena') return 'Athena';
      return 'Presto';
    }

    function onPrestoVersionChange() {
      updateUI();
      const input = document.getElementById('sql-input').value.trim();
      if (input) {
        convertSQL();
      }
    }

    function updateUI() {
      const prestoName = getPrestoDisplayName();
      if (currentMode === 'presto2spark') {
        document.getElementById('btn-toggle-mode').innerText = `⇄ Đổi chiều: ${prestoName} ➔ Spark SQL`;
        document.getElementById('btn-toggle-mode').style.background = "#0284c7";
        document.getElementById('btn-convert').innerText = "🚀 Chuyển đổi sang Spark SQL (Ctrl + Enter)";
        document.getElementById('btn-convert').style.background = "#2563eb";
        document.getElementById('title-left').innerText = `📥 INPUT: ${prestoName} SQL`;
        document.getElementById('sub-left').innerText = `Nguồn: ${prestoName}`;
        document.getElementById('title-right').innerText = "📤 OUTPUT: Spark SQL";
      } else {
        document.getElementById('btn-toggle-mode').innerText = `⇄ Đổi chiều: Spark SQL ➔ ${prestoName}`;
        document.getElementById('btn-toggle-mode').style.background = "#059669";
        document.getElementById('btn-convert').innerText = `🚀 Chuyển đổi sang ${prestoName} SQL (Ctrl + Enter)`;
        document.getElementById('btn-convert').style.background = "#059669";
        document.getElementById('title-left').innerText = "📥 INPUT: Spark SQL";
        document.getElementById('sub-left').innerText = "Nguồn: Apache Spark SQL";
        document.getElementById('title-right').innerText = `📤 OUTPUT: ${prestoName} SQL`;
      }
    }

    function toggleMode() {
      currentMode = currentMode === 'presto2spark' ? 'spark2presto' : 'presto2spark';
      updateUI();
      document.getElementById('status-bar').innerText = "Đã đổi chiều sang: " + currentMode.toUpperCase();
    }

    function swapContent() {
      const inVal = document.getElementById('sql-input').value;
      const outVal = document.getElementById('sql-output').value;
      document.getElementById('sql-input').value = outVal;
      document.getElementById('sql-output').value = inVal;
      toggleMode();
      document.getElementById('status-bar').innerText = "Đã đảo vị trí nội dung và chuyển hướng chuyển đổi!";
    }

    function loadSample() {
      document.getElementById('sql-input').value = currentMode === 'presto2spark' ? samplePresto : sampleSpark;
      document.getElementById('sql-output').value = '';
      document.getElementById('status-bar').innerText = "Đã nạp câu truy vấn mẫu.";
    }

    function clearAll() {
      document.getElementById('sql-input').value = '';
      document.getElementById('sql-output').value = '';
      document.getElementById('status-bar').innerText = "Đã xóa trắng.";
    }

    async function convertSQL() {
      const input = document.getElementById('sql-input').value.trim();
      if (!input) {
        alert("Vui lòng nhập câu lệnh SQL cần chuyển đổi!");
        return;
      }
      document.getElementById('status-bar').innerText = "Đang chuyển đổi...";

      const prestoDialect = getPrestoDialect();

      try {
        const res = await fetch('/api/convert', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ query: input, mode: currentMode, presto_dialect: prestoDialect })
        });
        if (res.status === 401) {
          checkAuth();
          return;
        }
        const data = await res.json();
        if (data.status === 'ok') {
          document.getElementById('sql-output').value = data.result;
          document.getElementById('status-bar').innerText = `✅ Chuyển đổi thành công [${prestoDialect.toUpperCase()}]!`;
        } else {
          document.getElementById('status-bar').innerText = "❌ Lỗi: " + data.error;
          alert("Lỗi chuyển đổi: " + data.error);
        }
      } catch (err) {
        document.getElementById('status-bar').innerText = "❌ Lỗi kết nối API";
      }
    }

    function copyResult() {
      const out = document.getElementById('sql-output').value;
      if (!out) return;
      navigator.clipboard.writeText(out);
      showToast("Đã sao chép vào Clipboard!");
    }

    function showToast(msg) {
      const toast = document.getElementById('toast');
      toast.innerText = msg;
      toast.classList.add('show');
      setTimeout(() => toast.classList.remove('show'), 2000);
    }

    document.addEventListener('keydown', (e) => {
      if (e.ctrlKey && e.key === 'Enter') {
        convertSQL();
      }
    });

    window.onload = () => {
      updateUI();
      checkAuth();
    };
  </script>
</body>
</html>
"""

# ==========================================
# 3. HTTP REQUEST HANDLER WITH AUTH
# ==========================================
class AuthRequestHandler(http.server.BaseHTTPRequestHandler):
    def get_session_token(self):
        cookie_header = self.headers.get("Cookie")
        if not cookie_header:
            return None
        c = cookies.SimpleCookie(cookie_header)
        if "session_token" in c:
            return c["session_token"].value
        return None

    def get_authenticated_user(self):
        token = self.get_session_token()
        return validate_session(token)

    def send_json(self, status_code, data, set_cookie_token=None, delete_cookie=False):
        resp_bytes = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(resp_bytes)))
        
        if set_cookie_token:
            # Cookie 7 ngày, HttpOnly để chống XSS
            c = cookies.SimpleCookie()
            c["session_token"] = set_cookie_token
            c["session_token"]["path"] = "/"
            c["session_token"]["httponly"] = True
            c["session_token"]["max-age"] = 7 * 24 * 3600
            for morsel in c.values():
                self.send_header("Set-Cookie", morsel.OutputString())
        elif delete_cookie:
            c = cookies.SimpleCookie()
            c["session_token"] = ""
            c["session_token"]["path"] = "/"
            c["session_token"]["max-age"] = 0
            for morsel in c.values():
                self.send_header("Set-Cookie", morsel.OutputString())

        self.end_headers()
        self.wfile.write(resp_bytes)

    def do_GET(self):
        if self.path == "/api/me":
            user = self.get_authenticated_user()
            if user:
                self.send_json(200, {"logged_in": True, "username": user})
            else:
                self.send_json(200, {"logged_in": False})
            return

        if self.path == "/api/list-templates":
            templates_list = doc_service.list_available_templates()
            self.send_json(200, {"status": "ok", "templates": templates_list})
            return

        if self.path.startswith("/api/download-template"):
            import urllib.parse
            parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            tpl_name = query.get("name", [""])[0]
            safe_name = os.path.basename(tpl_name)
            tpl_path = os.path.join(doc_service.TEMPLATES_DIR, safe_name)
            if safe_name and os.path.exists(tpl_path) and safe_name.lower().endswith(".docx"):
                with open(tpl_path, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                self.send_header("Content-Disposition", f'attachment; filename="{safe_name}"')
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)
                return
            else:
                self.send_json(404, {"status": "error", "error": "Template không tồn tại"})
                return

        if self.path.startswith("/api/sample-doc"):
            import urllib.parse
            parsed = urllib.parse.urlparse(self.path)
            query = urllib.parse.parse_qs(parsed.query)
            doc_type = query.get("type", ["tech"])[0]
            if doc_type == "deploy":
                sample_data = doc_service.get_sample_deployment_data()
            else:
                sample_data = doc_service.get_sample_technical_data()
            self.send_json(200, {"status": "ok", "data": sample_data})
            return

        # Trang chính HTML (Tự động tải index.html mới nhất kèm Cache-Busting)
        index_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
        if os.path.exists(index_file):
            with open(index_file, "r", encoding="utf-8") as f:
                html_bytes = f.read().encode("utf-8")
        else:
            html_bytes = HTML_PAGE.encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.send_header("Content-Length", str(len(html_bytes)))
        self.end_headers()
        self.wfile.write(html_bytes)

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        
        try:
            req_data = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            req_data = {}

        # 1. API Đăng nhập
        if self.path == "/api/login":
            u = req_data.get("username", "").strip() or DEFAULT_ADMIN_USER
            p = req_data.get("password", "")
            if verify_credentials(u, p):
                token = create_session(u)
                self.send_json(200, {"status": "ok", "username": u}, set_cookie_token=token)
            else:
                self.send_json(401, {"status": "error", "error": "Sai mật khẩu hoặc tên đăng nhập"})
            return

        # 2. API Đăng xuất
        if self.path == "/api/logout":
            token = self.get_session_token()
            destroy_session(token)
            self.send_json(200, {"status": "ok"}, delete_cookie=True)
            return

        # 3. API Chuyển đổi SQL (Cho phép truy cập trực tiếp không cần mật khẩu)
        if self.path == "/api/convert":
            query = req_data.get("query", "")
            mode = req_data.get("mode", "presto2spark")
            presto_dialect = req_data.get("presto_dialect", "presto")
            keep_format = req_data.get("keep_format", True)
            try:
                converted = convert_sql(query, mode=mode, presto_dialect=presto_dialect, keep_format=keep_format)
                self.send_json(200, {"status": "ok", "result": converted})
            except Exception as e:
                self.send_json(400, {"status": "error", "error": str(e)})
            return

        # 3.1. API Xuất File Word Tài Liệu Kỹ Thuật & Triển Khai (DocGen)
        if self.path == "/api/generate-doc":
            try:
                doc_type = req_data.get("type", "tech")
                data = req_data.get("data", {})
                if doc_type == "deploy":
                    doc_bytes = doc_service.render_deployment_document(data)
                    prefix = "Tai_Lieu_Trien_Khai"
                    code = data.get("system_code") or data.get("release_version") or "Release"
                else:
                    doc_bytes = doc_service.render_technical_document(data)
                    prefix = "Tai_Lieu_Ky_Thuat"
                    code = data.get("system_code") or data.get("version") or "TDD"

                clean_code = "".join(c for c in code if c.isalnum() or c in ("-", "_"))
                filename = f"{prefix}_{clean_code}.docx"

                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.send_header("Content-Length", str(len(doc_bytes.getvalue())))
                self.end_headers()
                self.wfile.write(doc_bytes.getvalue())
                return
            except Exception as e:
                self.send_json(500, {"status": "error", "error": str(e)})
                return

        # 3.2. API Phân tích động các biến trong file Word mẫu (Universal Inspect)
        if self.path == "/api/inspect-template":
            try:
                import base64
                tpl_name = req_data.get("template_name")
                b64_data = req_data.get("base64_data")
                if b64_data:
                    if "," in b64_data:
                        b64_data = b64_data.split(",", 1)[1]
                    raw_bytes = base64.b64decode(b64_data)
                    filename = req_data.get("filename") or f"custom_template_{int(time.time())}.docx"
                    safe_filename = "".join(c for c in filename if c.isalnum() or c in ("-", "_", "."))
                    save_path = os.path.join(doc_service.TEMPLATES_DIR, safe_filename)
                    with open(save_path, "wb") as f:
                        f.write(raw_bytes)
                elif tpl_name:
                    safe_filename = os.path.basename(tpl_name)
                    save_path = os.path.join(doc_service.TEMPLATES_DIR, safe_filename)
                    with open(save_path, "rb") as f:
                        raw_bytes = f.read()
                else:
                    self.send_json(400, {"status": "error", "error": "Thiếu template_name hoặc base64_data"})
                    return

                inspect_result = doc_service.inspect_template_content(raw_bytes)
                self.send_json(200, {
                    "status": "ok",
                    "filename": safe_filename,
                    "scalar_fields": inspect_result["scalar_fields"],
                    "loop_fields": inspect_result["loop_fields"]
                })
                return
            except Exception as e:
                self.send_json(500, {"status": "error", "error": str(e)})
                return

        # 3.3. API Render Động Mọi Loại Tài Liệu Word
        if self.path == "/api/render-dynamic-doc":
            try:
                import base64
                tpl_name = req_data.get("template_name")
                b64_data = req_data.get("base64_template")
                data = req_data.get("data", {})
                output_name = req_data.get("output_filename") or "Tai_Lieu_Xuat.docx"

                if b64_data:
                    if "," in b64_data:
                        b64_data = b64_data.split(",", 1)[1]
                    raw_bytes = base64.b64decode(b64_data)
                elif tpl_name:
                    safe_filename = os.path.basename(tpl_name)
                    save_path = os.path.join(doc_service.TEMPLATES_DIR, safe_filename)
                    with open(save_path, "rb") as f:
                        raw_bytes = f.read()
                else:
                    self.send_json(400, {"status": "error", "error": "Thiếu template"})
                    return

                doc_bytes = doc_service.render_dynamic_template(raw_bytes, data)
                safe_out_name = "".join(c for c in output_name if c.isalnum() or c in ("-", "_", "."))
                if not safe_out_name.lower().endswith(".docx"):
                    safe_out_name += ".docx"

                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                self.send_header("Content-Disposition", f'attachment; filename="{safe_out_name}"')
                self.send_header("Content-Length", str(len(doc_bytes.getvalue())))
                self.end_headers()
                self.wfile.write(doc_bytes.getvalue())
                return
            except Exception as e:
                self.send_json(500, {"status": "error", "error": str(e)})
                return

        # 4. Yêu cầu đăng nhập cho các API quản trị tài khoản
        user = self.get_authenticated_user()
        if not user:
            self.send_json(401, {"status": "error", "error": "Yêu cầu đăng nhập để sử dụng tính năng này"})
            return

        # 5. API Đổi mật khẩu
        if self.path == "/api/change-password":
            old_p = req_data.get("old_password", "")
            new_p = req_data.get("new_password", "")
            if not verify_credentials(user, old_p):
                self.send_json(400, {"status": "error", "error": "Mật khẩu hiện tại không đúng"})
                return
            if len(new_p) < 6:
                self.send_json(400, {"status": "error", "error": "Mật khẩu mới tối thiểu 6 ký tự"})
                return
            update_user_password(user, new_p)
            self.send_json(200, {"status": "ok"})
            return

        self.send_json(404, {"status": "error", "error": "Endpoint không tồn tại"})

    def log_message(self, format, *args):
        # Tắt logging nội dung query để bảo mật
        return

def main():
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    init_db()
    print("=" * 65)
    print("Presto <-> Spark SQL Transpiler with Authentication")
    print(f"Tai khoan mac dinh: {DEFAULT_ADMIN_USER} | Mat khau: {DEFAULT_ADMIN_PASS}")
    print(f"Dang mo trinh duyet tai: http://localhost:{PORT}")
    print("=" * 65)

    if not os.environ.get("PORT"):
        try:
            webbrowser.open(f"http://localhost:{PORT}")
        except Exception:
            pass

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("0.0.0.0", PORT), AuthRequestHandler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nĐã tắt Web Server.")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
doc_service.py: Dịch vụ quét biến động và sinh tài liệu Word (DocxTemplate engine)
Hỗ trợ:
- Quét tự động mọi trường Jinja2 {{ var }} (scalars) và bảng biểu lặp {%tr for item in list %} (loops)
- Nhận dạng tự động kiểu dữ liệu (text thường vs textarea cho đoạn văn bản dài)
- Tự động chuyển đổi các đoạn có dấu xuống dòng \n thành RichText OpenXML với <w:br/>
- Cung cấp dữ liệu mẫu chuẩn ngân hàng cho tài liệu kỹ thuật & tài liệu triển khai
"""

import os
import io
import re
import zipfile
from docxtpl import DocxTemplate, RichText

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
os.makedirs(TEMPLATES_DIR, exist_ok=True)

# Bảng nhãn tiếng Việt thân thiện cho các trường biến phổ biến
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


class SmartRichText(RichText):
    """
    Kế thừa RichText của docxtpl để tự động chèn ngắt dòng <w:br/> trong OpenXML
    cho bất kỳ chuỗi văn bản nào chứa ký tự xuống dòng \n hoặc \r.
    """
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


def prettify_name(var_name: str) -> str:
    """Chuyển đổi tên biến mã nguồn thành tên hiển thị tiếng Việt rõ ràng"""
    low = var_name.lower().strip()
    if low in FIELD_LABELS_MAP:
        return FIELD_LABELS_MAP[low]
    # Tự động thay thế gạch dưới và viết hoa chữ cái đầu
    words = var_name.replace("_", " ").strip().split()
    return " ".join(w.capitalize() for w in words) if words else var_name


def is_textarea_hint(var_name: str) -> bool:
    """Dự đoán trường biến có phải là trường văn bản nhiều dòng (textarea) không"""
    low = var_name.lower()
    multiline_keywords = [
        "overview", "desc", "content", "plan", "step", "note", "check", 
        "spec", "design", "req", "detail", "huong_dan", "noi_dung", "mo_ta", 
        "ghi_chu", "danh_sach", "scope", "objective", "architecture", 
        "tong_quan", "muc_tieu", "pham_vi", "luong_du_lieu", "bao_mat"
    ]
    return any(kw in low for kw in multiline_keywords)


def _process_data_for_render(data):
    """
    Duyệt đệ quy dữ liệu: tự động bọc chuỗi có xuống dòng bằng SmartRichText
    để Word hiển thị chính xác các đoạn văn bản nhiều dòng.
    """
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
    """Liệt kê danh sách tất cả các file mẫu Word có sẵn trong thư mục templates"""
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
    """
    Quét động file Word (.docx):
    1. Trích xuất tất cả các biến đơn {{ ten_bien }}
    2. Nhận diện các bảng lặp {%tr for item in danh_sach %} và thuộc tính {{ item.thuoc_tinh }}
    3. Tự động phân loại trường text / textarea và gán nhãn tiếng Việt
    """
    tpl = DocxTemplate(io.BytesIO(file_bytes))
    try:
        raw_vars = tpl.get_undeclared_template_variables()
    except Exception:
        raw_vars = set()

    # Đọc toàn bộ nội dung XML trong file zip của docx
    xml_texts = []
    with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
        for name in z.namelist():
            if name.endswith(".xml"):
                raw = z.read(name).decode("utf-8", errors="ignore")
                text = re.sub(r"<[^>]+>", "", raw)
                xml_texts.append(text)

    combined_text = "\n".join(xml_texts)

    # Tìm các vòng lặp: {% [tr|tc] for <item_var> in <loop_var> %} ... {% [tr|tc] endfor %}
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
            {"name": prop, "label": prettify_name(prop)}
            for prop in seen_props
        ]
        loop_fields.append({
            "loop_var": loop_var,
            "item_var": item_var,
            "label": prettify_name(loop_var),
            "fields": fields
        })

    # Lọc các biến đơn (scalars)
    scalar_fields = []
    for var in sorted(raw_vars):
        if var in loop_vars or var in item_vars or var == "loop":
            continue
        scalar_fields.append({
            "name": var,
            "label": prettify_name(var),
            "type": "textarea" if is_textarea_hint(var) else "text",
            "value": ""
        })

    return {
        "scalar_fields": scalar_fields,
        "loop_fields": loop_fields
    }


def render_dynamic_template(template_bytes: bytes, data: dict) -> io.BytesIO:
    """
    Render dữ liệu vào file Word động:
    Tự động chuẩn hóa văn bản nhiều dòng và sinh file .docx chuẩn OpenXML
    """
    tpl = DocxTemplate(io.BytesIO(template_bytes))
    processed_data = _process_data_for_render(data)
    tpl.render(processed_data)
    out = io.BytesIO()
    tpl.save(out)
    out.seek(0)
    return out


def render_deployment_document(data: dict) -> io.BytesIO:
    """Render tài liệu triển khai theo mẫu chuẩn"""
    tpl_path = os.path.join(TEMPLATES_DIR, "template_deployment_guide.docx")
    with open(tpl_path, "rb") as f:
        tpl_bytes = f.read()
    return render_dynamic_template(tpl_bytes, data)


def render_technical_document(data: dict) -> io.BytesIO:
    """Render tài liệu đặc tả kỹ thuật theo mẫu chuẩn"""
    tpl_path = os.path.join(TEMPLATES_DIR, "template_technical_spec.docx")
    with open(tpl_path, "rb") as f:
        tpl_bytes = f.read()
    return render_dynamic_template(tpl_bytes, data)


def get_sample_deployment_data() -> dict:
    """Dữ liệu mẫu cho tài liệu triển khai hệ thống"""
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
    """Dữ liệu mẫu cho tài liệu đặc tả kiến trúc kỹ thuật"""
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

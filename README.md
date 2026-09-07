---
title: Presto Spark SQL Transpiler & Data Flow Automation
emoji: ⚡
colorFrom: blue
colorTo: indigo
sdk: static
pinned: false
---

# ⚡ SQL TRANSPILER & DATA PIPELINE AUTOMATION TOOLKIT
### Hỗ trợ Truy vấn S3 Data Lake (Presto / Trino ⇄ Apache Spark SQL) & Tự động hóa Luồng Dữ liệu Báo cáo

---

## 1. Giới thiệu Tổng quan

Bộ công cụ chuyên dụng cho Kỹ sư Dữ liệu (Data Engineers) và Chuyên viên Báo cáo Dữ liệu (BI/DWH/Data Analyst) trong môi trường Big Data & Ngân hàng:
1. **Chuyển đổi cú pháp SQL 2 chiều:** Presto / Trino / Athena ⇄ Apache Spark SQL với độ chính xác cao nhờ AST Parser (`sqlglot`) và Regex Transpiler.
2. **Tool Gent Data Flow `c_pre_source_code` circular:** Tự động phân tích script Presto nhiều bước (`DELETE`, `INSERT`...), tự động convert hàm sang Spark SQL và sinh toàn bộ câu lệnh cấu hình nạp vào bảng điều phối luồng `prod_gold_ssd.sbv_report.c_pre_source_code`.
3. **Tool Cấu hình Báo cáo Thông tư 35 (5 Bảng):** Tự động sinh trọn bộ script SQL (`C_REPORT_ORDER_DETAIL`, `C_ORG_ITEM_ORDER`, `C_ITEM_LOCATION`, `C_FORM_DESIGN_INFO`, `C_SOURCE_CODE`) theo metadata và danh sách cột báo cáo.

---

## 2. Các Tính năng Cốt lõi

### 2.1. ⚡ Chuyển đổi SQL 2 chiều (Presto ⇄ Spark SQL)
- **Hỗ trợ đa dạng Dialect:**
  - `PrestoDB (0.2xx)` (EMR Presto, Athena v2, Presto Foundation).
  - `Trino / PrestoSQL (330+ / 400+)` (Athena v3).
  - `AWS Athena Engine`.
- **Bảo toàn 100% Biến tham số (Parameter Preservation):**
  - Giữ nguyên vẹn các biến Jinja/Airflow (`{{process_date}}`, `{{ ds }}`, `{{ params.x }}`) và Shell/Spark (`${VAR}`, `${hiveconf:...}`) không bị parser làm sai lệch.
- **Quy tắc chuyển đổi Big Data chuyên sâu:**
  - JSON function: `json_extract_scalar` ⇄ `get_json_object`.
  - Date/Time: `date_parse`, `date_add`, `date_diff` ⇄ `to_date`, `date_add`, `datediff`.
  - Array/Collection: `cardinality`, `contains`, `array_join` ⇄ `size`, `array_contains`, `concat_ws`.
  - Kiểu dữ liệu: `VARCHAR` ⇄ `STRING`, `VARBINARY` ⇄ `BINARY`.

### 2.2. 🔄 Tool Gent Data Flow `c_pre_source_code` Circular
- **Phân tách script tự động:** Nhận diện từng bước câu lệnh SQL từ script Presto nhiều bước (phân tách bởi dấu `;` hoặc comment tag).
- **Trích xuất thông minh:**
  - **Mô tả bước (`description_sql_code`):** Tự động lấy từ comment tag phía trên câu lệnh (vd: `-- [BƯỚC 1]: Xóa dữ liệu kỳ cũ`).
  - **Hành động (`action`):** Tự động nhận diện `DELETE`, `INSERT`, `TRUNCATE`, `UPDATE`, `MERGE`...
  - **Bảng tác động (`table_name`):** Tự động bóc tách tên bảng thuần từ SQL (`INSERT INTO table`, `DELETE FROM table`).
  - **Convert Spark SQL (`sql_code`):** Tự động chuyển đổi các hàm Presto sang cú pháp Spark SQL để Workflow Engine thực thi.
- **Tự động sinh lệnh DELETE an toàn theo Key:**
  - Tự động sinh câu lệnh DELETE cấu hình cũ theo đúng cặp khóa: `sbv_group_item_code_org` và `order_id` (tránh xóa nhầm các bước khác).
- **Lưới quản lý tương tác (Interactive Grid):** Cho phép chỉnh sửa trực tiếp, thêm bước thủ công, xóa bước, copy hoặc tải file `.sql`.

### 2.3. 📋 Tool Sinh Cấu hình Báo cáo Thông tư 35 (5 Bảng)
- Cấu hình Metadata báo cáo: Group Code, Tên bảng dữ liệu, Chu kỳ, Loại tiền, Trạng thái...
- Nhập danh sách cột linh hoạt (tự động điền nhanh 10, 20, 30 cột).
- Sinh trọn bộ câu lệnh `INSERT INTO` 5 bảng cấu hình Metadata lõi:
  1. `C_REPORT_ORDER_DETAIL`
  2. `C_ORG_ITEM_ORDER`
  3. `C_ITEM_LOCATION`
  4. `C_FORM_DESIGN_INFO`
  5. `C_SOURCE_CODE`

---

## 3. Cấu trúc Thư mục

```text
Converter_Tool/
├── convert_presto_to_spark.py    # Core Engine xử lý chuyển đổi SQL (CLI & AST & Regex)
├── app_gui.py                    # Giao diện ứng dụng máy tính (Tkinter Desktop App)
├── app_web.py                    # Web Server Python tích hợp SQLite Auth
├── index.html                    # Giao diện người dùng Web (Đầy đủ 3 Module)
├── Chay_Tool_Desktop.bat         # 1-Click mở Desktop App
├── Chay_Tool_Web.bat             # 1-Click mở Web App trên trình duyệt
├── Dockerfile                    # File build Docker container
├── docker-compose.yml            # File chạy container với Docker Compose
├── requirements.txt              # Thư viện phụ thuộc (sqlglot)
├── auth.db                       # Cơ sở dữ liệu SQLite quản lý Users & Sessions
├── README.md                     # Tài liệu giới thiệu & triển khai
└── HUONG_DAN_SU_DUNG.md          # Tài liệu hướng dẫn sử dụng chi tiết từng bước
```

---

## 4. Hướng dẫn Khởi động Nhanh

### 4.1. Khởi động 1-Click trên Windows
- **Chạy Web App:** Nhấp đúp chuột vào file `Chay_Tool_Web.bat` (tự động chạy server và mở trình duyệt tại `http://localhost:7860`).
- **Chạy Desktop App:** Nhấp đúp chuột vào file `Chay_Tool_Desktop.bat`.

### 4.2. Khởi động bằng Lệnh Python
```bash
cd "Converter_Tool"
pip install -r requirements.txt
python app_web.py
```
Truy cập trình duyệt: `http://localhost:7860`

---

## 5. Triển khai Docker & Hugging Face Spaces

### 5.1. Triển khai lên Hugging Face Spaces
Dự án hỗ trợ chạy trực tiếp trên Hugging Face Spaces:
```bash
# Thêm remote HF (nếu chưa có)
git remote add hf https://huggingface.co/spaces/Tuanpv21/presto-spark-converter

# Push lên Hugging Face
git add .
git commit -m "deploy: update web tools and documentation"
git push hf main
```

### 5.2. Chạy với Docker / Docker Compose
```bash
# Build và chạy với Docker Compose
docker compose up -d

# Hoặc build trực tiếp bằng Dockerfile
docker build -t sql-transpiler:latest .
docker run -d -p 7860:7860 --name sql-app sql-transpiler:latest
```

---

## 6. Tài liệu Hướng dẫn Sử dụng Chi tiết

👉 Vui lòng xem tài liệu chi tiết: **[HUONG_DAN_SU_DUNG.md](HUONG_DAN_SU_DUNG.md)** để nắm rõ từng bước thao tác với:
- Cách viết và sử dụng Comment Tag trong SQL nhiều bước.
- Ý nghĩa các trường: Thứ tự đầu, Bước nhảy, `rerun_term`, `term_code`.
- Cách sử dụng và tùy biến sinh cấu hình cho Thông tư 35.

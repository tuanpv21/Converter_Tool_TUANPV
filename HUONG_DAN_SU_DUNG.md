# 📖 HƯỚNG DẪN SỬ DỤNG BỘ CÔNG CỤ DATA PIPELINE & SQL TOOLKIT

---

## MỤC LỤC
1. [Giới thiệu Chung](#1-giới-thiệu-chung)
2. [Module 1: Chuyển đổi Cú pháp SQL (Presto ⇄ Spark SQL)](#2-module-1-chuyển-đổi-cú-pháp-sql-presto--spark-sql)
3. [Module 2: Tool Gent Data Flow `c_pre_source_code` Circular](#3-module-2-tool-gent-data-flow-c_pre_source_code-circular)
   - 3.1. [Các tham số Metadata quan trọng](#31-các-tham-số-metadata-quan-trọng)
   - 3.2. [Ý nghĩa "Thứ tự đầu / Bước nhảy" (Order Start / Order Step)](#32-ý-nghĩa-thứ-tự-đầu--bước-nhảy)
   - 3.3. [Cách viết Comment Tag để tự động gán Mô tả](#33-cách-viết-comment-tag-để-tự-động-gán-mô-tả)
   - 3.4. [Cơ chế sinh câu lệnh DELETE cấu hình cũ an toàn](#34-cơ-chế-sinh-câu-lệnh-delete-cấu-hình-cũ-an-toàn)
   - 3.5. [Quy trình thao tác chuẩn từng bước](#35-quy-trình-thao-tác-chuẩn-từng-bước)
4. [Module 3: Tool Sinh Cấu hình Báo cáo Thông tư 35 (5 Bảng)](#4-module-3-tool-sinh-cấu-hình-báo-cáo-thông-tư-35-5-bảng)
5. [Một số Lưu ý & Mẹo làm việc hiệu quả](#5-một-số-lưu-ý--mẹo-làm-việc-hiệu-quả)

---

## 1. Giới thiệu Chung

Bộ công cụ được xây dựng nhằm giải quyết trực tiếp bài toán thực tế của Data Engineer / BI / Data Analyst:
- Phát triển (dev) và kiểm thử nhanh câu lệnh SQL trên engine **Presto / Trino**.
- Đưa logic vào hệ thống tự động hóa / workflow engine chạy bằng **Apache Spark**.
- Cấu hình thông tin luồng dữ liệu vào các bảng điều phối (`c_pre_source_code`) và bảng siêu dữ liệu báo cáo (Thông tư 35).

Truy cập hệ thống:
- **Local (Máy tính cá nhân):** Chạy file `Chay_Tool_Web.bat` hoặc truy cập `http://localhost:7860`.
- **Hugging Face Spaces:** Truy cập đường dẫn Space được triển khai trên cloud.

---

## 2. Module 1: Chuyển đổi Cú pháp SQL (Presto ⇄ Spark SQL)

### Tính năng:
- Hỗ trợ chuyển đổi qua lại giữa **Presto** và **Spark SQL**.
- Chọn Dialect Presto: `PrestoDB`, `Trino`, `AWS Athena`.
- Tự động bảo vệ các biến tham số Jinja / Airflow (`{{process_date}}`, `{{ ds }}`) và biến Shell `${VAR}`.

### Các bước sử dụng:
1. Nhấp chọn tab **"⚡ Chuyển Đổi Presto ⇄ Spark"**.
2. Chọn chiều chuyển đổi (Presto ➔ Spark SQL hoặc ngược lại).
3. Dán câu lệnh SQL nguồn vào ô bên trái.
4. Nhấn nút **"⚡ Chuyển Đổi (AST Transpile)"**.
5. Xem kết quả ở ô bên phải, bấm **"📋 Sao chép"** hoặc **"💾 Tải về"**.

---

## 3. Module 2: Tool Gent Data Flow `c_pre_source_code` Circular

Đây là công cụ giúp tự động hóa việc đưa các script SQL chạy báo cáo nhiều bước vào bảng cấu hình luồng `prod_gold_ssd.sbv_report.c_pre_source_code`.

### 3.1. Các tham số Metadata quan trọng

| Tên trường | Tên cột trong DB | Ý nghĩa & Giá trị mẫu |
| :--- | :--- | :--- |
| **Group Báo Cáo** | `grp_job` | Tên nhóm tiến trình/job của báo cáo (Mặc định: `C_PRE_STAT`). |
| **Mã Báo Cáo** | `sbv_group_item_code_org` | Mã định danh duy nhất của báo cáo (VD: `c_pre_sao_ke_lending`). |
| **Kỳ Báo Cáo** | `term_code` | Tần suất chạy báo cáo: `D` (Hàng Ngày), `M` (Hàng Tháng), `Q` (Quý), `Y` (Năm). |
| **Kỳ Chạy Lại** | `rerun_term` | Cấu hình cho phép chạy lại báo cáo khi có thay đổi dữ liệu (Mặc định: `TRUOC_CIC`). |
| **Sử Dụng** | `is_used` | `1` (Có kích hoạt chạy trong luồng), `0` (Tạm dừng chạy). |

### 3.2. Ý nghĩa "Thứ tự đầu / Bước nhảy"
Cột **`order_id`** quy định thứ tự các câu lệnh chạy tuần tự trong luồng:
- **Thứ tự đầu (`Order Start`):** Số thứ tự của câu lệnh đầu tiên (Mặc định: `1`).
- **Bước nhảy (`Order Step`):** Khoảng cách cộng thêm cho bước kế tiếp:
  - Nếu bước nhảy là `1`: Thứ tự các bước sẽ là `1, 2, 3, 4...`
  - Nếu bước nhảy là `10`: Thứ tự các bước sẽ là `10, 20, 30, 40...` (Khuyên dùng khi dự định sau này có thể chèn thêm bước phụ như `15` ở giữa).

### 3.3. Cách viết Comment Tag để tự động gán Mô tả
Trong khung nhập liệu script, bạn chỉ cần viết chú thích phía trên mỗi câu lệnh SQL. Công cụ sẽ tự động nhận diện và điền vào cột `description_sql_code`:

#### Cách 1: Đánh số bước dạng `[...]` (Khuyên dùng):
```sql
-- [BƯỚC 1]: Xóa dữ liệu cũ kỳ báo cáo
DELETE FROM prod_gold_ssd.sbv_report.f_sao_ke_lending WHERE report_date = '{{process_date}}';

-- [BƯỚC 2]: Tổng hợp dư nợ cho vay theo từng hợp đồng
INSERT INTO prod_gold_ssd.sbv_report.f_sao_ke_lending
SELECT ... ;
```
👉 Tool tự cắt bỏ chữ `[BƯỚC 1]:` và gán mô tả là: `Xóa dữ liệu cũ kỳ báo cáo`.

#### Cách 2: Viết chú thích dạng `Bước X:` hoặc comment thông thường:
```sql
-- Bước 1: Xóa dữ liệu bảng tạm
DELETE FROM tmp_report_data;

-- Nạp dữ liệu mới vào bảng chính
INSERT INTO main_report_data SELECT ...;
```

### 3.4. Cơ chế sinh câu lệnh DELETE cấu hình cũ an toàn
Hệ thống cung cấp tùy chọn:
`☑ Sinh câu lệnh DELETE cấu hình cũ theo key sbv_group_item_code_org và order_id`

- **Khi chạy:** Tool sẽ sinh câu lệnh `DELETE` lọc chính xác theo `sbv_group_item_code_org` và danh sách các `order_id` bạn nạp lần này:
  ```sql
  DELETE FROM prod_gold_ssd.sbv_report.c_pre_source_code 
  WHERE sbv_group_item_code_org = 'c_pre_sao_ke_lending'
    AND order_id IN (1, 2, 3);
  ```
- **Lợi ích:** Chỉ dọn đúng các bước bạn cập nhật mà không xóa mất các bước khác nếu báo cáo có nhiều phần cấu hình riêng rẽ.

### 3.5. Quy trình thao tác chuẩn từng bước
1. Chọn tab **"🔄 Tool gent data flow c_pre_source_code circular"**.
2. Kiểm tra/nhập thông tin Metadata tại **Section 1**.
3. Dán toàn bộ script Presto nhiều bước vào **Section 2**.
4. Bấm nút **"⚡ Phân Tích & Convert sang Spark"**.
5. Xem lại danh sách các bước tại **Section 3 (Interactive Grid)**: Bạn có thể sửa trực tiếp mô tả, thứ tự, câu lệnh SQL hoặc bấm ➕ Thêm Bước / 🗑 Xóa.
6. Bấm **"⚡ Sinh Script Cấu Hình (INSERT c_pre_source_code)"** tại **Section 4**.
7. Bấm **"📋 Sao chép Script SQL"** hoặc **"💾 Tải file .SQL"** để lấy script chạy nạp cấu hình.

---

## 4. Module 3: Tool Sinh Cấu hình Báo cáo Thông tư 35 (5 Bảng)

Dùng cho việc khai báo metadata của mẫu biểu báo cáo SBV theo chuẩn Thông tư 35.

### Quy trình thao tác:
1. Chọn tab **"📋 Tool Gent Cấu Hình Báo Cáo TT35 (5 Bảng)"**.
2. **Khai báo Metadata:** Nhập Mã Báo cáo (`cir-group-code`), Tên bảng dữ liệu (`cir-order-table`), Kỳ báo cáo, Loại tiền tệ...
3. **Khai báo Cột dữ liệu:**
   - Dùng các nút **"+10 Cột"**, **"+20 Cột"** để sinh nhanh.
   - Nhập Tên cột, Tên cột dữ liệu nguồn (`source_col`), Tên cột thứ tự sắp xếp (`order_id_col`), Kiểu dữ liệu...
4. Bấm **"⚡ Sinh Câu Lệnh INSERT (5 Bảng)"**.
5. Hệ thống sẽ sinh trọn bộ script SQL cho 5 bảng:
   - `C_REPORT_ORDER_DETAIL`
   - `C_ORG_ITEM_ORDER`
   - `C_ITEM_LOCATION`
   - `C_FORM_DESIGN_INFO`
   - `C_SOURCE_CODE`
6. Sao chép hoặc tải file `.sql` để chạy vào cơ sở dữ liệu.

---

## 5. Một số Lưu ý & Mẹo làm việc hiệu quả

1. **Kiểm tra hàm sau khi convert:** Mặc dù bộ chuyển đổi hỗ trợ hầu hết các hàm phổ biến (`date_diff`, `date_add`, `json_extract_scalar`, `unnest`), bạn nên rà soát lại các biểu thức nghiệp vụ phức tạp trong lưới Section 3 trước khi sinh câu lệnh cuối cùng.
2. **Ký tự đặc biệt trong SQL:** Trình sinh script đã tự động xử lý ký tự nháy đơn `'` bằng cách escape thành `''` để đảm bảo chuỗi SQL trong câu lệnh `INSERT` không bị lỗi cú pháp.
3. **Biến ngày chạy:** Luôn sử dụng biến chuẩn dạng `{{process_date}}` hoặc `${process_date}` để engine luồng tự động truyền ngày tham số khi chạy hàng ngày/hàng tháng.

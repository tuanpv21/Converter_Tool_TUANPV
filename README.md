---
title: Presto Spark SQL Transpiler
emoji: ⚡
colorFrom: blue
colorTo: indigo
sdk: static
pinned: false
---

# ⚡ TOOL CHUYỂN ĐỔI CÚ PHÁP SQL 2 CHIỀU: PRESTO ⇄ SPARK SQL
### Hỗ trợ Truy vấn Dữ liệu S3 Data Lake (Presto / Trino / Athena ⇄ Apache Spark SQL)

---

## 1. Giới thiệu & Tính năng cốt lõi

Công cụ chuyên dụng giải quyết bài toán chuyển đổi mã nguồn SQL giữa hai engine phổ biến nhất trên S3 Data Lake: **Presto / Trino** và **Apache Spark SQL**.

- **Chuyển đổi 2 chiều (Bi-directional):** `Presto ➔ Spark SQL` và `Spark SQL ➔ Presto`.
- **Engine phân tích cú pháp AST:** Sử dụng thư viện `sqlglot` để phân tích cây cú pháp trừu tượng, chuyển đổi chính xác các cấu trúc phức tạp (CTE, Subqueries, Joins, Window functions, Unnest/Explode).
- **Quy tắc S3 Data Lake chuyên sâu:**
  - Chuyển đổi hàm đọc JSON (`json_extract_scalar` ⇄ `get_json_object`).
  - Chuyển đổi định dạng ngày giờ (`date_parse`, `date_add`, `date_diff` ⇄ `to_date`, `datediff`).
  - Xử lý mảng và tập hợp (`cardinality`, `contains`, `array_join` ⇄ `size`, `array_contains`, `concat_ws`).
  - Chuẩn hóa kiểu dữ liệu (`VARCHAR` ⇄ `STRING`, `VARBINARY` ⇄ `BINARY`).
- **Bảo mật & Phân quyền (Authentication Server-side):**
  - Xác thực đăng nhập 100% tại máy chủ backend Python (không để lộ mật khẩu hay logic ở HTML).
  - Quản lý tài khoản bằng **SQLite (`auth.db`)** kết hợp mật khẩu băm SHA-256 + Salt.
  - Hỗ trợ biến môi trường (`ADMIN_USERNAME`, `ADMIN_PASSWORD`) giúp chạy container Docker an toàn.
  - Session Cookie HttpOnly 7 ngày (tự động nhớ phiên đăng nhập, chống XSS).
- **Giao diện đa dạng:** Hỗ trợ cả **Desktop App (Tkinter)**, **Web Browser (Dark Mode Split Editor)** và **Command Line (CLI)**.

---

## 2. Thông tin Tài khoản Mặc định

| Thông tin | Giá trị Mặc định | Ghi chú |
| :--- | :--- | :--- |
| **Username** | `admin` | Tùy biến qua biến môi trường `ADMIN_USERNAME` |
| **Password** | `PublicBank@2026` | Tùy biến qua biến môi trường `ADMIN_PASSWORD` hoặc đổi trong Web |

> Sau khi đăng nhập, bạn có thể bấm nút **`🔑 Đổi MK`** ở góc trên bên phải để đổi sang mật khẩu cá nhân.

---

## 3. Cấu trúc Thư mục

```text
Converter_Tool/
├── convert_presto_to_spark.py    # Core Engine xử lý chuyển đổi SQL (CLI & AST)
├── app_gui.py                    # Giao diện ứng dụng máy tính (Desktop App)
├── app_web.py                    # Giao diện Web Server (Tích hợp Auth & SQLite)
├── Chay_Tool_Desktop.bat         # 1-Click mở Desktop App
├── Chay_Tool_Web.bat             # 1-Click mở Web App trên trình duyệt
├── Dockerfile                    # File đóng gói container chuẩn bảo mật
├── docker-compose.yml            # File chạy đa container với 1 lệnh
├── requirements.txt              # Thư viện phụ thuộc (sqlglot)
├── auth.db                       # Cơ sở dữ liệu SQLite quản lý Users & Sessions
└── README.md                     # Tài liệu hướng dẫn sử dụng & triển khai
```

---

## 4. Hướng dẫn sử dụng Cục bộ (Local)

### 4.1. Khởi động 1-Click
- **Giao diện Desktop:** Nhấp đúp chuột vào file `Chay_Tool_Desktop.bat`.
- **Giao diện Web:** Nhấp đúp chuột vào file `Chay_Tool_Web.bat` (tự động mở trình duyệt tại `http://localhost:7860`).

### 4.2. Sử dụng từ Dòng lệnh (CLI)
```bash
# Presto -> Spark
python convert_presto_to_spark.py -m presto2spark -f input.sql -o output.sql

# Spark -> Presto
python convert_presto_to_spark.py -m spark2presto -f input.sql -o output.sql

# Chuyển đổi hàng loạt toàn bộ thư mục
python convert_presto_to_spark.py -m presto2spark -d ./presto_queries/ -o ./spark_queries/
```

---

## 5. Hướng dẫn Triển khai bằng Docker & Dockerfile

File `Dockerfile` đã được tối ưu hóa theo tiêu chuẩn bảo mật doanh nghiệp:
- Base image: `python:3.10-slim` gọn nhẹ (chỉ khoảng ~150MB).
- Tạo user non-root `user` (UID 1000) chống leo thang đặc quyền container.
- Cổng mặc định: `7860`.

### 5.1. Build Docker Image
Mở terminal tại thư mục `Converter_Tool` và chạy lệnh:
```bash
docker build -t sql-transpiler:latest .
```

### 5.2. Chạy Container (3 Cách)

#### Cách 1: Chạy cơ bản (Nhanh nhất)
```bash
docker run -d \
  -p 7860:7860 \
  --name sql-app \
  sql-transpiler:latest
```
*Mở trình duyệt truy cập: `http://localhost:7860` (hoặc `http://<IP-server>:7860` nếu chạy trên máy chủ nội bộ).*

#### Cách 2: Chạy kèm biến môi trường bảo mật & Persistent Volume (Khuyên dùng)
Gắn Volume để lưu file SQLite `auth.db` lâu dài (không bị mất khi restart container) và đặt mật khẩu Admin tùy ý:
```bash
docker run -d \
  -p 7860:7860 \
  --name sql-app \
  --restart always \
  -e ADMIN_USERNAME=tuanpv \
  -e ADMIN_PASSWORD=MatKhauBaoMatCuaBan@123 \
  -v sql_data:/home/user/app \
  sql-transpiler:latest
```

#### Cách 3: Chạy bằng Docker Compose (1-Click)
Trong thư mục đã có sẵn file `docker-compose.yml`. Bạn chỉ cần gõ:
```bash
docker compose up -d
```
Để dừng container:
```bash
docker compose down
```

---

## 6. Bảng Tra cứu Biến Môi trường (Environment Variables)

Khi chạy Docker, bạn có thể truyền các biến môi trường qua cờ `-e` hoặc trong file `docker-compose.yml`:

| Biến Môi Trường | Mặc định | Mô tả |
| :--- | :--- | :--- |
| `PORT` | `7860` | Cổng dịch vụ web lắng nghe bên trong container |
| `ADMIN_USERNAME` | `admin` | Tên đăng nhập mặc định cho quyền quản trị |
| `ADMIN_PASSWORD` | `PublicBank@2026` | Mật khẩu ban đầu để đăng nhập vào hệ thống |

---

## 7. Các Lệnh Quản trị Docker Thường dùng

```bash
# Xem log container đang chạy (theo dõi truy cập và debug)
docker logs -f sql-app

# Kiểm tra trạng thái tài nguyên CPU/RAM
docker stats sql-app

# Dừng container
docker stop sql-app

# Khởi động lại container
docker restart sql-app

# Xóa container cũ để deploy bản mới
docker rm -f sql-app
```

---

## 8. Triển khai Docker lên Cloud & Server Nội bộ

### 8.1. Triển khai lên VPS Riêng / Server Nội bộ Ngân hàng (Ubuntu / Debian / CentOS)
1. Cài đặt Docker trên server: `curl -fsSL https://get.docker.com | sh`
2. Copy thư mục `Converter_Tool` lên server.
3. Chạy `docker compose up -d`.
4. Cấu hình **Nginx Reverse Proxy** và cấp chứng chỉ **SSL Let's Encrypt**:
   ```nginx
   server {
       listen 80;
       server_name sql-tool.publicbank.com.vn;

       location / {
           proxy_pass http://127.0.0.1:7860;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
       }
   }
   ```
   Chạy lệnh cấp HTTPS tự động: `sudo certbot --nginx -d sql-tool.publicbank.com.vn`.

### 8.2. Triển khai lên Render.com (Miễn phí 100% Container Docker)
1. Đưa thư mục `Converter_Tool` lên GitHub Repository (chế độ Private).
2. Đăng nhập [render.com](https://render.com) -> Chọn **New +** -> **Web Service**.
3. Chọn repo GitHub của bạn -> Environment: chọn **Docker**.
4. Render sẽ tự động đọc `Dockerfile` và deploy thành trang web có domain HTTPS miễn phí.

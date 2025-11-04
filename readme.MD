# Bus Data Stream — Hướng dẫn chạy (Windows)

Mô tả ngắn: bộ script Python dùng Kafka để gửi/ xử lý và chèn dữ liệu vào PostgreSQL.
Thư mục gốc: f:\ThacSi\HCSDLTT\Đồ án

Yêu cầu
- Docker & Docker Compose
- Python 3.8+
- Một terminal (PowerShell / CMD) mở tại thư mục dự án

Chuẩn bị
1. Mở PowerShell/CMD tại thư mục dự án:
   ```powershell
   cd "f:\ThacSi\HCSDLTT\Đồ án"
   ```

2. Khởi chạy Kafka + Zookeeper + Postgres:
   ```powershell
   docker compose up -d
   ```

3. Tạo virtual environment và cài thư viện Python:
   - Tạo & kích hoạt venv (PowerShell):
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     ```
     (hoặc CMD: .\.venv\Scripts\activate.bat)

   - File phụ thuộc hiện tên là `requirements.xt`. Hoặc đổi tên:
     ```powershell
     ren requirements.xt requirements.txt
     pip install -r requirements.txt
     ```
     Hoặc cài trực tiếp:
     ```powershell
     pip install -r requirements.xt
     ```

Cấu hình
- Kiểm tra `config.ini` (bootstrap kafka, Postgres credentials, topics, đường dẫn CSV).
- Mặc định:
  - Kafka: localhost:9092
  - Postgres: localhost:5432 (user=admin, password=password123, db=bus_data_db)
  - CSV mặc định: `source.csv` (cấu hình tại [APPLICATION_SETTINGS].csv_file_path)

Chạy các script (thứ tự khuyến nghị)
1. Producer (gửi dữ liệu từ CSV tới topic raw):
   ```powershell
   python csv_producer.py
   ```
2. Stream processor (đọc raw -> publish clean/failed):
   ```powershell
   python stream_processor.py
   ```
3. Postgres inserter (nhận dữ liệu clean -> insert vào Postgres):
   ```powershell
   python postgres_consumer.py
   ```

Kiểm tra dữ liệu trong Postgres
- Kết nối qua psql / pgAdmin tới localhost:5432, database `bus_data_db`, user `admin`.
- Bảng: `bus_logs` (tự tạo bởi script nếu chưa có).

Tệp chính
- csv_producer.py — đọc CSV, gửi event tới topic `raw_input_topic`
- stream_processor.py — chuyển kiểu, gửi tới `clean_output_topic` hoặc `failed_output_topic`
- postgres_consumer.py — batch insert vào Postgres
- config.ini — cấu hình Kafka / Postgres / topics / csv path
- docker-compose.yaml — Kafka + Zookeeper + Postgres
- source.csv / source_data_csv.py — dữ liệu mẫu / sinh dữ liệu

Troubleshooting ngắn
- Lỗi kết nối Kafka: kiểm tra `docker compose ps` và `config.ini` bootstrap_servers.
- Lỗi file CSV không tìm thấy: kiểm tra `csv_file_path` trong `config.ini`.
- Lỗi Postgres: kiểm tra container Postgres đang chạy, đúng ports/credentials.

Dừng service Docker:
```powershell
docker compose down
```

Ghi chú
- Giữ terminal đang chạy producer/processor/inserter theo thứ tự để luồng dữ liệu hoạt động.
- Nếu cần đổi tên file phụ thuộc, dùng `ren requirements.xt requirements.txt`.

# Bus Data Stream — Hướng dẫn chạy (Windows)

Mô tả ngắn: tập hợp script Python để giả lập phát dữ liệu từ CSV vào Kafka, xử lý luồng và chèn vào PostgreSQL.

## Yêu cầu
- Docker Desktop (Docker Compose)
- Python 3.8+
- PowerShell hoặc CMD (mở tại thư mục dự án)

Đường dẫn dự án (ví dụ):
f:\ThacSi\HCSDLTT\Đồ án

## Chuẩn bị
1. Mở PowerShell hoặc CMD tại thư mục dự án:
   ```powershell
   cd "f:\ThacSi\HCSDLTT\Đồ án"
   ```

2. Tạo virtual environment và cài thư viện Python:
   - Tạo & kích hoạt (PowerShell):
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     ```
     (Nếu dùng CMD)
     ```cmd
     python -m venv .venv
     .\.venv\Scripts\activate.bat
     ```
   - Tạo file `requirements.txt` với nội dung:
     ```
     kafka-python
     pandas
     psycopg2-binary
     faker
     ```
   - Cài đặt:
     ```powershell
     pip install -r requirements.txt
     ```

3. Kiểm tra/ chỉnh config:
   - Mở `config.ini` và đảm bảo:
     - [KAFKA].bootstrap_servers = localhost:9092
     - [POSTGRES] trùng với Docker Compose (user/admin, password/password123, database/bus_data_db)
     - [APPLICATION_SETTINGS].csv_file_path trỏ tới file CSV (mặc định `source.csv`)

4. Khởi động hạ tầng (Kafka + Zookeeper + Postgres):
   ```powershell
   docker compose up -d
   ```
   Chờ ~20-30 giây để các service sẵn sàng.

## Thứ tự chạy script (mở nhiều Terminal)
Mở các terminal trong cùng thư mục dự án và chạy theo thứ tự sau:

1. Terminal 1 — Postgres consumer (chạy thứ 2 nhưng mở terminal trước):
   ```powershell
   python postgres_consumer.py
   ```
   (Chức năng: đọc topic clean và chèn batch vào Postgres)

2. Terminal 2 — Stream processor:
   ```powershell
   python stream_processor.py
   ```
   (Chức năng: đọc topic raw, chuyển kiểu/dọn dữ liệu, publish sang topic clean hoặc failed)

3. Terminal 3 — CSV producer (chạy cuối để bắt đầu phát dữ liệu):
   ```powershell
   python csv_producer.py
   ```
   (Chức năng: đọc `source.csv` hoặc file chỉ định trong config và gửi event tới Kafka)

Ghi chú: Các script sẽ chờ message nếu không có dữ liệu. Đóng bằng Ctrl+C.

## Kiểm tra dữ liệu trong Postgres
Kết nối đến container Postgres và dùng psql:
```powershell
docker compose exec postgres psql -U admin -d bus_data_db
```
Trong psql:
```sql
SELECT COUNT(*) FROM bus_logs;
\q
```

## Dừng hạ tầng
Khi xong, dừng và xóa container/volume:
```powershell
docker compose down -v
```

## Troubleshooting nhanh
- Lỗi kết nối Kafka: kiểm tra `docker compose ps` và giá trị `bootstrap_servers` trong `config.ini` (nên là localhost:9092).
- Producer báo không tìm file CSV: kiểm tra `csv_file_path` trong `config.ini` và đường dẫn file (`source.csv` hoặc `bus_data.csv`).
- Postgres connection error: kiểm tra container postgres chạy, ports (5432), user/password trong `config.ini` và `docker-compose.yaml`.
- Nếu gặp lỗi module thiếu: đảm bảo virtualenv đang active và đã pip install -r requirements.txt.

## Files chính
- csv_producer.py — gửi dữ liệu từ CSV tới topic raw
- stream_processor.py — xử lý & publish sang topic clean/failed
- postgres_consumer.py — nhận dữ liệu clean và insert vào Postgres
- config.ini — cấu hình Kafka / Postgres / topics / csv path
- docker-compose.yaml — định nghĩa Kafka, Zookeeper, Postgres
- source.csv / source_data_csv.py — dữ liệu mẫu / script sinh dữ liệu

---
Hướng dẫn này đủ để khởi chạy luồng dữ liệu cơ bản trên máy Windows. Nếu cần thay đổi chi tiết cấu hình (ports, credentials, file names) hãy chỉnh `config.ini` và `docker-compose.yaml`.
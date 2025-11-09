# Pipeline Kho dữ liệu (Data Warehouse) Real-Time - Dữ liệu Taxi NYC

## Tổng quan
Dự án này là một pipeline xử lý dữ liệu luồng (streaming) 3 lớp hoàn chỉnh, được thiết kế để mô phỏng một Kho dữ liệu (Data Warehouse) thời gian thực.

### Mục tiêu
- Nạp (ingest) bộ dữ liệu "NYC Yellow Taxi" từ file CSV
- Xử lý (process) dữ liệu theo thời gian thực qua Apache Kafka
- Tải (load) dữ liệu vào một Lược đồ Hình sao (Star Schema) trong PostgreSQL

> Hệ thống này được xây dựng làm "sân chơi" (sandbox) để thử nghiệm các kỹ thuật Tối ưu hóa Truy vấn Nâng cao (như Lập chỉ mục, Kế hoạch Thực thi Động, và giám sát ML) được mô tả trong bài báo nghiên cứu.

## 🚀 Kiến trúc Hệ thống

Pipeline bao gồm 3 lớp phần mềm, chạy song song và được kết nối bởi Apache Kafka.

### Lớp 1: Ingestion (Thu thập) - csv_producer.py

**Đầu vào:** Đọc file yellow_tripdata_2015-01.csv (hoặc file được định nghĩa trong config.ini)

> **TẢI VỀ TỪ:** https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data

**Công việc:**
- Lấy mẫu (sample) các dòng ngẫu nhiên từ file
- Đọc tất cả dữ liệu dưới dạng chuỗi (str) để đảm bảo an toàn
- "Phát" (produce) tin nhắn JSON vào topic Kafka taxi_trips_raw

**Mục đích:** Giả lập một nguồn dữ liệu "live" (trực tiếp) đang liên tục tạo ra các chuyến đi

### Lớp 2: Processing (Xử lý) - stream_processor.py

**Đầu vào:** Lắng nghe topic taxi_trips_raw

**Công việc:**
- Nhận các tin nhắn JSON thô
- "Dọn dẹp" (clean) và "biến đổi" (transform) dữ liệu
- Xử lý các kiểu dữ liệu và giá trị rỗng (nan)

**Đầu ra:** Gửi dữ liệu đã sạch sang topic taxi_trips_clean

### Lớp 3: Storage (Lưu trữ) - postgres_consumer.py

**Đầu vào:** Lắng nghe topic taxi_trips_clean

**Công việc:**
- Thiết lập CSDL (Lần chạy đầu tiên)
- Tạo Lược đồ Hình sao (Star Schema) gồm 6 bảng
- Nạp Bảng Dim
- Chạy liên tục và thực hiện INSERT hàng loạt

## 📋 Yêu cầu Phần mềm

- Docker và Docker Compose
- Python 3.8+
- File dữ liệu yellow_tripdata_2015-01.csv

## ⚙️ Cài đặt & Cấu hình

### 1. Tải Dữ liệu
Tải file yellow_tripdata_2015-01.csv và đặt vào thư mục dự án

### 2. Cài đặt Thư viện Python
Tạo file requirements.txt:
```
kafka-python
pandas
psycopg2-binary
```

Cài đặt thư viện:
```bash
pip install -r requirements.txt
```

### 3. Kiểm tra Cấu hình (config.ini)
- **[APPLICATION_SETTINGS]**: Cấu hình đường dẫn file CSV
- **[POSTGRES]**: Cấu hình tên CSDL
- **[TOPICS]**: Cấu hình tên các topic

## ⚡ Hướng dẫn chạy

### Terminal 1: Khởi động Hạ tầng
```bash
docker-compose up -d
```

### Terminal 2: Chạy Lớp 3 (Lưu trữ)
```bash
python postgres_consumer.py
```

### Terminal 3: Chạy Lớp 2 (Xử lý)
```bash
python stream_processor.py
```

### Terminal 4: Chạy Lớp 1 (Phát dữ liệu)
```bash
python csv_producer.py
```

### Terminal 5: Kiểm tra Kết quả
```bash
docker-compose exec postgres psql -U admin -d taxi_data_db
```

Các truy vấn kiểm tra:
```sql
-- Đếm số dòng
SELECT COUNT(*) FROM Fact_Trips;

-- Kiểm tra Star Schema
SELECT
    p.payment_type_name,
    COUNT(f.trip_id_pk) AS so_luong_chuyen
FROM
    Fact_Trips f
JOIN
    Dim_Payment_Type p ON f.payment_type_fk = p.payment_type_pk
GROUP BY
    p.payment_type_name
ORDER BY
    so_luong_chuyen DESC;
```

## ⏹️ Dừng Hệ thống

```bash
docker-compose down
# Hoặc xóa hoàn toàn volume:
docker-compose down -v
```

## 🚀 Tối ưu hóa Truy vấn

### 1. Phân tích Query
```sql
EXPLAIN ANALYZE
SELECT ... -- câu query JOIN ở trên
```

### 2. Tạo Index
```sql
CREATE INDEX idx_fact_payment_fk ON Fact_Trips(payment_type_fk);
```

### 3. Giám sát Hiệu năng
```sql
SELECT total_exec_time, calls, mean_exec_time, query
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;
```

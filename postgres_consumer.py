import json
import time
import psycopg2
import configparser  # <--- BƯỚC 1: Import thư viện
from psycopg2 import extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# --- BƯỚC 2: Đọc file config ---
config = configparser.ConfigParser()
config.read('config.ini')

KAFKA_TOPIC = config['TOPICS']['clean_output_topic'] or 'bus_transactions_clean'
KAFKA_BROKERS = config['KAFKA']['bootstrap_servers'] or 'localhost:9092'
KAFKA_GROUP_ID = config['CONSUMER_GROUPS']['inserter_group_id'] or 'postgres_inserter_group_v1'


# --- CẤU HÌNH POSTGRESQL (Đọc từ config) ---
PG_HOST = config['POSTGRES']['host'] or 'localhost'
PG_PORT = config['POSTGRES']['port']  or 5432
PG_DB = config['POSTGRES']['database'] or 'bus_data_db'
PG_USER = config['POSTGRES']['user'] or 'admin'
PG_PASS = config['POSTGRES']['password'] or 'password123'


# --- CẤU HÌNH BATCH INSERT (Đọc từ config) ---
# QUAN TRỌNG: configparser đọc mọi thứ là CHUỖI.
# Bạn phải ép kiểu (cast) chúng sang SỐ (int)
BATCH_SIZE = int(config['APPLICATION_SETTINGS']['batch_size']) or 100
BATCH_TIMEOUT = int(config['APPLICATION_SETTINGS']['batch_timeout_seconds']) or 5

def get_pg_connection():
    """Hàm tiện ích để kết nối tới Postgres."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,    # <--- Biến này giờ đã được đọc từ config
            port=PG_PORT,    # <--- Biến này giờ đã được đọc từ config
            database=PG_DB,  # <--- Biến này giờ đã được đọc từ config
            user=PG_USER,    # <--- Biến này giờ đã được đọc từ config
            password=PG_PASS # <--- Biến này giờ đã được đọc từ config
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"LỖI: Không thể kết nối tới PostgreSQL. Database đã chạy chưa?")
        print(f"Lỗi: {e}")
        return None

def create_table(conn):
    """Tạo bảng (nếu chưa tồn tại) để chứa dữ liệu xe buýt."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bus_logs (
        transID BIGINT PRIMARY KEY,
        payCardID TEXT,
        payCardBank TEXT,
        payCardName TEXT,
        payCardSex VARCHAR(10),
        payCardBirthDate DATE,
        corridorID TEXT,
        corridorName TEXT,
        direction SMALLINT,
        tapInStops TEXT,
        tapInStopsName TEXT,
        tapInStopsLat DOUBLE PRECISION,
        tapInStopsLon DOUBLE PRECISION,
        stopStartSeq INTEGER,
        tapInTime TIMESTAMP WITH TIME ZONE,
        tapOutStops TEXT,
        tapOutStopsName TEXT,
        tapOutStopsLat DOUBLE PRECISION,
        tapOutStopsLon DOUBLE PRECISION,
        stopEndSeq INTEGER,
        tapOutTime TIMESTAMP WITH TIME ZONE,
        payAmount INTEGER,
        produced_at TIMESTAMP WITH TIME ZONE
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            print("Đã kiểm tra/tạo bảng 'bus_logs' thành công.")
    except Exception as e:
        print(f"LỖI khi tạo bảng: {e}")
        conn.rollback()

def insert_batch(conn, batch):
    """Thực hiện batch insert bằng 'execute_values' cho hiệu năng cao."""
    if not batch:
        return 0

    # Chuyển đổi list[dict] thành list[tuple] để insert
    # Lấy danh sách cột theo đúng thứ tự
    cols = batch[0].keys()

    # Tạo list các tuple giá trị
    values_to_insert = [tuple(row[col] for col in cols) for row in batch]

    # Tạo câu lệnh INSERT
    # ví dụ: INSERT INTO bus_logs (transID, payCardID, ...) VALUES %s
    insert_sql = f"INSERT INTO bus_logs ({', '.join(cols)}) VALUES %s"

    try:
        with conn.cursor() as cursor:
            # Dùng 'execute_values' để tối ưu hóa việc insert nhiều dòng
            psycopg2.extras.execute_values(
                cursor,
                insert_sql,
                values_to_insert,
                template=None,
                page_size=BATCH_SIZE
            )
            conn.commit()
            return len(batch)
    except Exception as e:
        print(f"LỖI khi batch insert: {e}")
        conn.rollback()
        return 0

def main():
    """Vòng lặp chính: Nghe Kafka, batch, và insert vào Postgres."""

    # 1. Kết nối tới Postgres và tạo bảng
    pg_conn = get_pg_connection()
    if pg_conn is None:
        return
    create_table(pg_conn)

    # 2. Kết nối tới Kafka
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,                  # <--- Biến này giờ đã được đọc từ config
            bootstrap_servers=KAFKA_BROKERS, # <--- Biến này giờ đã được đọc từ config
            group_id=KAFKA_GROUP_ID,      # <--- Bi..."
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        print("Đã kết nối Kafka. Bắt đầu nghe...")
    except KafkaError as e:
        print(f"LỖI kết nối Kafka: {e}")
        pg_conn.close()
        return

    batch = []
    last_insert_time = time.time()

    try:
        for message in consumer:
            # Nhận dữ liệu sạch từ Lớp 2
            clean_data = message.value
            batch.append(clean_data)

            current_time = time.time()

            # Kiểm tra điều kiện để insert batch:
            # 1. Đã đủ BATCH_SIZE
            # 2. Hoặc đã quá BATCH_TIMEOUT
            if len(batch) >= BATCH_SIZE or (current_time - last_insert_time) > BATCH_TIMEOUT:
                if batch:
                    count = insert_batch(pg_conn, batch)
                    print(f"Đã insert {count} dòng vào PostgreSQL.")
                    batch = [] # Xóa batch cũ
                    last_insert_time = current_time

    except KeyboardInterrupt:
        print("Đã dừng. Đang insert nốt phần còn lại...")
        # Insert nốt những gì còn lại trong batch
        if batch:
            count = insert_batch(pg_conn, batch)
            print(f"Đã insert {count} dòng cuối cùng vào PostgreSQL.")

    finally:
        print("Đóng kết nối...")
        consumer.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
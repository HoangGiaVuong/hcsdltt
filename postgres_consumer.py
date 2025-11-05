import json
import time
import psycopg2
import configparser
from psycopg2 import extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# --- Đọc file config ---
config = configparser.ConfigParser()
config.read('config.ini')

# --- Cấu hình Kafka ---
KAFKA_TOPIC = config['TOPICS']['clean_output_topic']
KAFKA_BROKERS = config['KAFKA']['bootstrap_servers']
KAFKA_GROUP_ID = config['CONSUMER_GROUPS']['inserter_group_id']

# --- Cấu hình POSTGRESQL ---
PG_HOST = config['POSTGRES']['host']
PG_PORT = config['POSTGRES']['port']
PG_DB = config['POSTGRES']['database']
PG_USER = config['POSTGRES']['user']
PG_PASS = config['POSTGRES']['password']

# --- Cấu hình BATCH INSERT ---
BATCH_SIZE = int(config['APPLICATION_SETTINGS']['batch_size'])
BATCH_TIMEOUT = int(config['APPLICATION_SETTINGS']['batch_timeout_seconds'])

TIMESTAMP_COLS = ['tapInTime', 'tapOutTime', 'produced_at']

def get_pg_connection():
    """Hàm tiện ích để kết nối tới Postgres."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, database=PG_DB,
            user=PG_USER, password=PG_PASS
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"LỖI: Không thể kết nối tới PostgreSQL. {e}")
        return None

def create_table(conn):
    """
    Tạo bảng (nếu chưa tồn tại) với SCHEMA ĐÃ ĐƯỢC CẬP NHẬT
    (dựa trên dữ liệu mẫu của bạn).
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bus_logs (
        transID TEXT PRIMARY KEY,
        payCardID TEXT,
        payCardBank TEXT,
        payCardName TEXT,
        payCardSex VARCHAR(10),
        payCardBirthDate INTEGER,
        corridorID INTEGER,
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
            print("Đã kiểm tra/tạo bảng 'bus_logs' với schema cập nhật.")
    except Exception as e:
        print(f"LỖI khi tạo bảng: {e}")
        conn.rollback()

# ... (Phần 'insert_batch' và 'main' giữ nguyên y hệt như cũ) ...
# ... (Bạn không cần thay đổi gì từ hàm 'insert_batch' trở đi) ...

def insert_batch(conn, batch):
    """Thực hiện batch insert bằng 'execute_values' cho hiệu năng cao."""
    if not batch:
        return 0

    # Lấy danh sách cột từ tin nhắn ĐẦU TIÊN
    # Chúng ta không thể giả định thứ tự, vì vậy chúng ta phải
    # lấy tên cột một cách rõ ràng
    cols = batch[0].keys()

    # Tạo list các tuple giá trị
    # (Đảm bảo thứ tự khớp với 'cols')
    values_to_insert = [tuple(row.get(col) for col in cols) for row in batch]

    # Tạo câu lệnh INSERT động
    insert_sql = f"INSERT INTO bus_logs ({', '.join(cols)}) VALUES %s"

    # Thêm 'ON CONFLICT' để tránh crash nếu gửi trùng 'transID'
    # Nó sẽ bỏ qua (DO NOTHING) nếu 'transID' đã tồn tại.
    insert_sql += " ON CONFLICT (transID) DO NOTHING"

    try:
        with conn.cursor() as cursor:
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
        # Thử in dữ liệu gây lỗi
        try:
            print(f"Dữ liệu gây lỗi (có thể): {batch[0]}")
        except Exception:
            pass
        return 0

def main():
    """Vòng lặp chính: Nghe Kafka, batch, và insert vào Postgres."""

    pg_conn = get_pg_connection()
    if pg_conn is None:
        return
    create_table(pg_conn) # Sẽ tạo bảng với schema mới

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=KAFKA_GROUP_ID,
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
            clean_data = message.value

            # --- Xử lý Timestamp đặc biệt cho Postgres ---
            # Postgres cần 'None' thay vì chuỗi rỗng '' cho timestamp
            for ts_col in TIMESTAMP_COLS:
                if ts_col in clean_data and clean_data[ts_col] == '':
                    clean_data[ts_col] = None
            # ----------------------------------------------

            batch.append(clean_data)

            current_time = time.time()

            if len(batch) >= BATCH_SIZE or (current_time - last_insert_time) > BATCH_TIMEOUT:
                if batch:
                    count = insert_batch(pg_conn, batch)
                    print(f"Đã insert/bỏ qua {count} dòng vào PostgreSQL.")
                    batch = []
                    last_insert_time = current_time

    except KeyboardInterrupt:
        print("Đã dừng. Đang insert nốt phần còn lại...")
        if batch:
            count = insert_batch(pg_conn, batch)
            print(f"Đã insert/bỏ qua {count} dòng cuối cùng vào PostgreSQL.")

    finally:
        print("Đóng kết nối...")
        consumer.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
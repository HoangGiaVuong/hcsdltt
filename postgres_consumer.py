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

# Các cột Timestamp
TIMESTAMP_COLS = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'produced_at']

# --- BỘ LỌC DỌN DẸP KHÓA NGOẠI ---
# Chúng ta định nghĩa các ID hợp lệ dựa trên hàm setup_database
VALID_VENDORS = {1, 2}
VALID_PAYMENTS = {1, 2, 3, 4, 5, 6}
VALID_RATES = {1, 2, 3, 4, 5, 6}

# Chúng ta định nghĩa các giá trị "Mặc định" (Default) nếu dữ liệu bẩn
# (Phải là các giá trị hợp lệ ở trên)
DEFAULT_VENDOR = 1
DEFAULT_PAYMENT = 5 # ID 5 là 'Unknown'
DEFAULT_RATE = 5    # ID 5 là 'Negotiated fare' (gần với 'Unknown')
# ------------------------------------

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

def setup_database(conn):
    """
    Tạo tất cả các bảng (Dims và Fact) và nạp dữ liệu ban đầu
    cho các bảng Dim theo yêu cầu.
    """
    queries = [
        # --- Bảng 1: Dim_Vendor (theo yêu cầu) ---
        """
        CREATE TABLE IF NOT EXISTS Dim_Vendor (
            vendor_id_pk INT PRIMARY KEY,
            vendor_name VARCHAR(100)
        );
        """,
        # --- Bảng 2: Dim_Payment_Type (theo yêu cầu) ---
        """
        CREATE TABLE IF NOT EXISTS Dim_Payment_Type (
            payment_type_pk INT PRIMARY KEY,
            payment_type_name VARCHAR(100)
        );
        """,
        # --- Bảng 3: Dim_Rate_Code (theo yêu cầu) ---
        """
        CREATE TABLE IF NOT EXISTS Dim_Rate_Code (
            rate_code_id_pk INT PRIMARY KEY,
            rate_code_name VARCHAR(100)
        );
        """,

        # --- Bảng 4 & 5 (Thỏa hiệp) ---
        # Như đã giải thích, 2 bảng này không khả thi trong
        # pipeline streaming. Chúng ta tạo chúng, nhưng sẽ không sử dụng.
        """
        CREATE TABLE IF NOT EXISTS Dim_Location (
            location_id_pk INT PRIMARY KEY,
            borough VARCHAR(100),
            zone VARCHAR(255),
            service_zone VARCHAR(100)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS Dim_DateTime (
            datetime_pk TIMESTAMP PRIMARY KEY,
            tpep_date DATE,
            tpep_year INT,
            tpep_quarter INT,
            tpep_month INT,
            tpep_day INT,
            tpep_day_of_week INT,
            tpep_hour INT
        );
        """,

        # --- Bảng 6: Bảng FACT_TRIPS (Bảng chính) ---
        # Bảng này sẽ link tới 3 bảng Dim khả thi
        """
        CREATE TABLE IF NOT EXISTS Fact_Trips (
            trip_id_pk BIGSERIAL PRIMARY KEY,

            -- Khóa ngoại (Foreign Keys) tới 3 Dims
            vendor_id_fk INT REFERENCES Dim_Vendor(vendor_id_pk),
            payment_type_fk INT REFERENCES Dim_Payment_Type(payment_type_pk),
            rate_code_id_fk INT REFERENCES Dim_Rate_Code(rate_code_id_pk),

            -- Dữ liệu "chiều" không thể link
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            pickup_longitude DOUBLE PRECISION,
            pickup_latitude DOUBLE PRECISION,
            dropoff_longitude DOUBLE PRECISION,
            dropoff_latitude DOUBLE PRECISION,

            -- Dữ liệu "thực tế" (Measures)
            passenger_count INTEGER,
            trip_distance DOUBLE PRECISION,
            store_and_fwd_flag TEXT,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,

            -- Dữ liệu meta
            produced_at TIMESTAMP WITH TIME ZONE
        );
        """,

        # --- NẠP DỮ LIỆU BAN ĐẦU (Pre-population) ---
        # Chúng ta phải nạp dữ liệu chuẩn cho các bảng Dim
        # (ON CONFLICT DO NOTHING: bỏ qua nếu đã tồn tại)

        # Dim_Vendor (Theo tài liệu của NYC Taxi)
        """
        INSERT INTO Dim_Vendor (vendor_id_pk, vendor_name) VALUES
        (1, 'Creative Mobile Technologies, LLC'),
        (2, 'VeriFone Inc.')
        ON CONFLICT (vendor_id_pk) DO NOTHING;
        """,

        # Dim_Payment_Type (Theo tài liệu của NYC Taxi)
        """
        INSERT INTO Dim_Payment_Type (payment_type_pk, payment_type_name) VALUES
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')
        ON CONFLICT (payment_type_pk) DO NOTHING;
        """,

        # Dim_Rate_Code (Theo tài liệu của NYC Taxi)
        """
        INSERT INTO Dim_Rate_Code (rate_code_id_pk, rate_code_name) VALUES
        (1, 'Standard rate'),
        (2, 'JFK'),
        (3, 'Newark'),
        (4, 'Nassau or Westchester'),
        (5, 'Negotiated fare'),
        (6, 'Group ride')
        ON CONFLICT (rate_code_id_pk) DO NOTHING;
        """
    ]

    try:
        with conn.cursor() as cursor:
            print("Bắt đầu thiết lập CSDL (Star Schema)...")
            for query in queries:
                cursor.execute(query)
            conn.commit()
            print("Thiết lập CSDL thành công. 6 bảng đã được tạo/cập nhật.")
    except Exception as e:
        print(f"LỖI khi thiết lập CSDL: {e}")
        conn.rollback()

def insert_batch(conn, batch):
    """Thực hiện batch insert vào Bảng Fact_Trips."""
    if not batch:
        return 0

    # Danh sách cột trong Fact_Trips
    # (Tên cột trong Fact_Trips PHẢI KHỚP với tên trong tin nhắn (message)
    #  trừ các FK)

    # Bản đồ: Tên cột CSV -> Tên cột Fact_Trips
    column_mapping = {
        'VendorID': 'vendor_id_fk',
        'payment_type': 'payment_type_fk',
        'RateCodeID': 'rate_code_id_fk',
        'tpep_pickup_datetime': 'tpep_pickup_datetime',
        'tpep_dropoff_datetime': 'tpep_dropoff_datetime',
        'passenger_count': 'passenger_count',
        'trip_distance': 'trip_distance',
        'pickup_longitude': 'pickup_longitude',
        'pickup_latitude': 'pickup_latitude',
        'store_and_fwd_flag': 'store_and_fwd_flag',
        'dropoff_longitude': 'dropoff_longitude',
        'dropoff_latitude': 'dropoff_latitude',
        'fare_amount': 'fare_amount',
        'extra': 'extra',
        'mta_tax': 'mta_tax',
        'tip_amount': 'tip_amount',
        'tolls_amount': 'tolls_amount',
        'improvement_surcharge': 'improvement_surcharge',
        'total_amount': 'total_amount',
        'produced_at': 'produced_at'
    }

    # Lọc ra các cột có trong tin nhắn (message)
    cols_in_message = batch[0].keys()

    # Lấy tên cột CSDL (ví dụ: 'vendor_id_fk')
    db_cols = [column_mapping[col] for col in cols_in_message if col in column_mapping]
    # Lấy tên cột tin nhắn (ví dụ: 'VendorID')
    msg_cols = [col for col in cols_in_message if col in column_mapping]

    # Tạo list các tuple giá trị
    values_to_insert = [
        tuple(row.get(msg_col) for msg_col in msg_cols) for row in batch
    ]

    # Tạo câu lệnh INSERT
    insert_sql = f"INSERT INTO Fact_Trips ({', '.join(db_cols)}) VALUES %s"

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
    except psycopg2.Error as e:
        print(f"LỖI khi batch insert: {e}")
        # Lỗi Khóa Ngoại (Foreign Key) là lỗi phổ biến nhất ở đây
        if "violates foreign key constraint" in str(e):
            print("LỖI KHÓA NGOẠI: Dữ liệu CSV chứa ID không tồn tại trong Bảng Dim.")
            print(f"Dữ liệu lỗi (có thể): {batch[0]}")
        conn.rollback()
        return 0
    except Exception as e:
        print(f"LỖI không xác định khi batch insert: {e}")
        conn.rollback()
        return 0

def main():
    """Vòng lặp chính: Nghe Kafka, batch, và insert vào Postgres."""

    pg_conn = get_pg_connection()
    if pg_conn is None:
        return

    # 1. THIẾT LẬP CSDL (Tạo 6 bảng, nạp 3 bảng Dim)
    setup_database(pg_conn)

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=KAFKA_GROUP_ID,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        print(f"Đã kết nối Kafka. Bắt đầu nghe '{KAFKA_TOPIC}'...")
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
            for ts_col in TIMESTAMP_COLS:
                if ts_col in clean_data and (clean_data[ts_col] == '' or clean_data[ts_col] is None):
                    clean_data[ts_col] = None

            # --- BƯỚC DỌN DẸP KHÓA NGOẠI (MỚI) ---
            # (Chúng ta dọn dẹp các tên cột TỪ LỚP 2 gửi đến)

            # 1. Dọn dẹp VendorID
            if clean_data.get('VendorID') not in VALID_VENDORS:
                clean_data['VendorID'] = DEFAULT_VENDOR

            # 2. Dọn dẹp payment_type
            if clean_data.get('payment_type') not in VALID_PAYMENTS:
                clean_data['payment_type'] = DEFAULT_PAYMENT

            # 3. Dọn dẹp RateCodeID (Đây là cái gây lỗi 99)
            if clean_data.get('RateCodeID') not in VALID_RATES:
                clean_data['RateCodeID'] = DEFAULT_RATE
            # --------------------------------------------

            batch.append(clean_data)

            current_time = time.time()

            if len(batch) >= BATCH_SIZE or (current_time - last_insert_time) > BATCH_TIMEOUT:
                if batch:
                    count = insert_batch(pg_conn, batch)
                    print(f"Đã insert {count} chuyến taxi vào Fact_Trips.")
                    batch = []
                    last_insert_time = current_time

    except KeyboardInterrupt:
        print("Đã dừng. Đang insert nốt phần còn lại...")
        if batch:
            count = insert_batch(pg_conn, batch)
            print(f"Đã insert {count} chuyến taxi cuối cùng vào Fact_Trips.")

    finally:
        print("Đóng kết nối...")
        consumer.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
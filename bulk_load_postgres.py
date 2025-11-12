import pandas as pd
import psycopg2
import psycopg2.extras
import configparser
import io
import time
from datetime import datetime

# --- LẤY CẤU HÌNH TỪ config.ini ---
try:
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Postgres
    PG_HOST = config['POSTGRES']['host']
    PG_PORT = config['POSTGRES']['port']
    PG_DB = config['POSTGRES']['database']
    PG_USER = config['POSTGRES']['user']
    PG_PASS = config['POSTGRES']['password']

    # Application
    CSV_FILE = config['APPLICATION_SETTINGS']['csv_file_path']
    # Đọc 100,000 dòng một lúc
    CHUNK_SIZE = int(config['APPLICATION_SETTINGS']['chunk_size'])

    # --- BỘ LỌC DỌN DẸP KHÓA NGOẠI ---
    # Chúng ta định nghĩa các ID hợp lệ dựa trên hàm setup_database
    VALID_VENDORS = {1, 2}
    VALID_PAYMENTS = {1, 2, 3, 4, 5, 6}
    VALID_RATES = {1, 2, 3, 4, 5, 6}

    # Chúng ta định nghĩa các giá trị "Mặc định" (Default) nếu dữ liệu bẩn
    DEFAULT_VENDOR = 1
    DEFAULT_PAYMENT = 5 # ID 5 là 'Unknown'
    DEFAULT_RATE = 5    # ID 5 là 'Negotiated fare' (gần với 'Unknown')
    # ------------------------------------

except Exception as e:
    print(f"LỖI: Không thể đọc file config.ini. Lỗi: {e}")
    exit()

def get_pg_connection():
    """Hàm tiện ích kết nối Postgres."""
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASS
        )
        conn.autocommit = False # Chúng ta sẽ tự quản lý commit
        return conn
    except psycopg2.OperationalError as e:
        print(f"LỖI: Không thể kết nối tới PostgreSQL. {e}")
        return None

def setup_database(conn):
    """
    Tạo 6 bảng Star Schema và nạp dữ liệu cho 3 bảng Dim chính.
    """
    # Dữ liệu chuẩn cho các bảng Dim
    vendor_data = [(1, 'Creative Mobile Technologies, LLC'), (2, 'VeriFone Inc.')]
    payment_type_data = [
        (1, 'Credit card'), (2, 'Cash'), (3, 'No charge'),
        (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided trip')
    ]
    rate_code_data = [
        (1, 'Standard rate'), (2, 'JFK'), (3, 'Newark'),
        (4, 'Nassau or Westchester'), (5, 'Negotiated fare'), (6, 'Group ride')
    ]

    queries = [
        # 1. Dim_Vendor
        """
        CREATE TABLE IF NOT EXISTS Dim_Vendor (
            vendor_id_pk INT PRIMARY KEY,
            vendor_name VARCHAR(100)
        );
        """,
        # 2. Dim_Payment_Type
        """
        CREATE TABLE IF NOT EXISTS Dim_Payment_Type (
            payment_type_pk INT PRIMARY KEY,
            payment_type_name VARCHAR(100)
        );
        """,
        # 3. Dim_Rate_Code
        """
        CREATE TABLE IF NOT EXISTS Dim_Rate_Code (
            rate_code_id_pk INT PRIMARY KEY,
            rate_code_name VARCHAR(100)
        );
        """,
        # 4. Dim_Location (sẽ không nạp dữ liệu ở đây)
        """
        CREATE TABLE IF NOT EXISTS Dim_Location (
            location_id_pk INT PRIMARY KEY,
            borough VARCHAR(100),
            zone VARCHAR(255),
            service_zone VARCHAR(100)
        );
        """,
        # 5. Dim_DateTime (sẽ không nạp dữ liệu ở đây)
        """
        CREATE TABLE IF NOT EXISTS Dim_DateTime (
            datetime_pk TIMESTAMP PRIMARY KEY,
            tpep_date DATE, tpep_year INT, tpep_quarter INT, tpep_month INT,
            tpep_day INT, tpep_day_of_week INT, tpep_hour INT
        );
        """,
        # 6. Bảng Fact chính
        """
        CREATE TABLE IF NOT EXISTS Fact_Trips (
            trip_id_pk SERIAL PRIMARY KEY,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count BIGINT,
            trip_distance DOUBLE PRECISION,
            pickup_longitude DOUBLE PRECISION,
            pickup_latitude DOUBLE PRECISION,
            store_and_fwd_flag BOOLEAN,
            dropoff_longitude DOUBLE PRECISION,
            dropoff_latitude DOUBLE PRECISION,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            produced_at TIMESTAMP WITH TIME ZONE,

            -- Khóa ngoại (Foreign Keys)
            vendor_id_fk INT REFERENCES Dim_Vendor(vendor_id_pk),
            payment_type_fk INT REFERENCES Dim_Payment_Type(payment_type_pk),
            rate_code_id_fk INT REFERENCES Dim_Rate_Code(rate_code_id_pk)
        );
        """
    ]

    print("Bắt đầu thiết lập CSDL (Star Schema)...")
    try:
        with conn.cursor() as cursor:
            # Xóa bảng Fact cũ (nếu có) để nạp lại từ đầu
            # Dùng CASCADE để xóa các ràng buộc nếu có
            print("Đang xóa (DROP) bảng Fact_Trips cũ (nếu tồn tại)...")
            cursor.execute("DROP TABLE IF EXISTS Fact_Trips CASCADE;")

            # Chạy các lệnh tạo bảng
            print("Đang tạo 6 bảng Star Schema...")
            for query in queries:
                cursor.execute(query)

            # Nạp dữ liệu cho 3 bảng Dim
            print("Đang nạp dữ liệu chuẩn cho 3 bảng Dim (Vendor, Payment, RateCode)...")
            psycopg2.extras.execute_values(cursor, "INSERT INTO Dim_Vendor (vendor_id_pk, vendor_name) VALUES %s ON CONFLICT DO NOTHING", vendor_data)
            psycopg2.extras.execute_values(cursor, "INSERT INTO Dim_Payment_Type (payment_type_pk, payment_type_name) VALUES %s ON CONFLICT DO NOTHING", payment_type_data)
            psycopg2.extras.execute_values(cursor, "INSERT INTO Dim_Rate_Code (rate_code_id_pk, rate_code_name) VALUES %s ON CONFLICT DO NOTHING", rate_code_data)

            conn.commit()
            print("Thiết lập CSDL thành công.")

    except Exception as e:
        conn.rollback()
        print(f"LỖI khi thiết lập CSDL: {e}")
        raise

def clean_data_chunk(df):
    """
    Dọn dẹp và biến đổi DataFrame (chunk) để khớp với schema Bảng Fact.
    """
    # Đổi tên cột cho khớp với Khóa ngoại (FK)
    df = df.rename(columns={
        'VendorID': 'vendor_id_fk',
        'payment_type': 'payment_type_fk',
        'RateCodeID': 'rate_code_id_fk'
    })

    # 1. Chuyển đổi kiểu NUMERIC (Float)
    float_cols = [
        'trip_distance', 'pickup_longitude', 'pickup_latitude',
        'dropoff_longitude', 'dropoff_latitude', 'fare_amount',
        'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount'
    ]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 2. Chuyển đổi kiểu INTEGER (Bao gồm các FKs)
    int_cols = [
        'passenger_count', 'vendor_id_fk', 'payment_type_fk', 'rate_code_id_fk'
    ]
    for col in int_cols:
        # Dùng 'Int64' (chữ I hoa) để hỗ trợ giá trị NaN (null)
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # --- DỌN DẸP KHÓA NGOẠI (SỬA LỖI '99') ---
    # Sử dụng .mask() để thay thế các ID không hợp lệ bằng giá trị DEFAULT

    # Dọn dẹp VendorID
    if 'vendor_id_fk' in df.columns:
        # Thay thế bất cứ thứ gì KHÔNG có trong VALID_VENDORS bằng DEFAULT_VENDOR
        df['vendor_id_fk'] = df['vendor_id_fk'].mask(~df['vendor_id_fk'].isin(VALID_VENDORS), DEFAULT_VENDOR)

    # Dọn dẹp payment_type
    if 'payment_type_fk' in df.columns:
        df['payment_type_fk'] = df['payment_type_fk'].mask(~df['payment_type_fk'].isin(VALID_PAYMENTS), DEFAULT_PAYMENT)

    # Dọn dẹp RateCodeID (Sửa lỗi '99')
    if 'rate_code_id_fk' in df.columns:
        df['rate_code_id_fk'] = df['rate_code_id_fk'].mask(~df['rate_code_id_fk'].isin(VALID_RATES), DEFAULT_RATE)
    # --- KẾT THÚC DỌN DẸP ---

    # 3. Chuyển đổi kiểu DATETIME
    datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # 4. Chuyển đổi kiểu BOOLEAN
    # Ánh xạ (Map) 'N' -> False, 'Y' -> True
    if 'store_and_fwd_flag' in df.columns:
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].map(
            {'N': False, 'Y': True, None: pd.NA}
        ).astype('boolean')

    # 5. Chọn các cột cuối cùng theo đúng thứ tự của Bảng Fact
    final_columns_order = [
        'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
        'trip_distance', 'pickup_longitude', 'pickup_latitude',
        'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude',
        'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
        'improvement_surcharge', 'total_amount',
        'vendor_id_fk', 'payment_type_fk', 'rate_code_id_fk'
    ]

    # Đảm bảo chúng ta chỉ giữ các cột tồn tại trong file CSV
    existing_cols = [col for col in final_columns_order if col in df.columns]
    return df[existing_cols]

def bulk_load_copy(conn, df_chunk, column_names):
    """
    Nạp (load) một chunk DataFrame vào Postgres bằng lệnh COPY (siêu nhanh).
    """

    # 1. Tạo một buffer (bộ đệm) trong bộ nhớ
    buffer = io.StringIO()

    # 2. Ghi DataFrame vào buffer dưới dạng CSV (không index, không header)
    #    na_rep='' (chuỗi rỗng) sẽ được Postgres hiểu là NULL
    df_chunk.to_csv(buffer, index=False, header=False, sep=',', na_rep='')

    # 3. Đưa con trỏ buffer về đầu
    buffer.seek(0)

    try:
        with conn.cursor() as cursor:
            # 4. Dùng lệnh COPY, chỉ định rõ các cột

            # COPY Fact_Trips (col1, col2, ...) FROM STDIN WITH (FORMAT CSV, NULL '')
            cursor.copy_expert(
                f"COPY Fact_Trips ({','.join(column_names)}) FROM STDIN WITH (FORMAT CSV, NULL '')",
                buffer
            )
            conn.commit() # Commit sau mỗi chunk

    except Exception as e:
        conn.rollback() # Hoàn tác nếu có lỗi
        print(f"LỖI: Không thể nạp chunk. Lỗi: {e}")
        # In 5 dòng đầu tiên của chunk bị lỗi để debug
        print("Dữ liệu chunk bị lỗi (5 dòng đầu):")
        print(df_chunk.head())
        raise # Dừng script nếu có lỗi nghiêm trọng

def main():
    """Hàm chính điều khiển quá trình ETL."""

    print("Bắt đầu quá trình ETL (Bulk Load) từ CSV vào Postgres...")
    start_time = time.time()

    conn = get_pg_connection()
    if conn is None:
        return

    try:
        # Bước 1: Thiết lập CSDL (Tạo bảng, nạp Dims)
        # Bảng Fact_Trips sẽ bị XÓA và tạo lại
        setup_database(conn)

        # Bước 2 & 3: Đọc (Extract), Biến đổi (Transform) và Nạp (Load) theo Chunk
        print(f"Bắt đầu đọc file {CSV_FILE} theo chunk (size={CHUNK_SIZE})...")

        total_rows = 0
        first_chunk = True
        column_names = []

        # Tạo một trình lặp (iterator) để đọc file theo chunk
        csv_iterator = pd.read_csv(
            CSV_FILE,
            chunksize=CHUNK_SIZE,
            low_memory=False # Tắt cảnh báo kiểu dữ liệu
        )

        for i, chunk in enumerate(csv_iterator):
            chunk_start_time = time.time()

            # 3a. Transform
            cleaned_chunk = clean_data_chunk(chunk)

            if first_chunk:
                # Lấy danh sách cột từ chunk đầu tiên
                column_names = list(cleaned_chunk.columns)
                first_chunk = False

            # 3b. Load
            bulk_load_copy(conn, cleaned_chunk, column_names)

            total_rows += len(cleaned_chunk)
            chunk_time = time.time() - chunk_start_time

            print(f"  -> Đã nạp Chunk {i+1} ({len(cleaned_chunk)} dòng) trong {chunk_time:.2f} giây. Tổng số dòng: {total_rows}")

        end_time = time.time()
        print("-" * 30)
        print("QUÁ TRÌNH ETL HOÀN TẤT!")
        print(f"Tổng cộng đã nạp {total_rows} dòng.")
        print(f"Tổng thời gian: {(end_time - start_time):.2f} giây.")

    except FileNotFoundError:
        print(f"LỖI: Không tìm thấy file {CSV_FILE}. Hãy kiểm tra lại file 'config.ini'.")
    except Exception as e:
        print(f"Một lỗi không mong muốn đã xảy ra: {e}")
    finally:
        if conn:
            conn.close()
            print("Đã đóng kết nối CSDL.")

if __name__ == "__main__":
    main()
import json
import configparser
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

# --- Đọc file config ---
config = configparser.ConfigParser()
config.read('config.ini')

# --- Cấu hình Kafka ---
IN_TOPIC = config['TOPICS']['raw_input_topic']
CLEAN_TOPIC = config['TOPICS']['clean_output_topic']
FAILED_TOPIC = config['TOPICS']['failed_output_topic']
KAFKA_BROKERS = config['KAFKA']['bootstrap_servers']
PROCESSOR_GROUP_ID = config['CONSUMER_GROUPS']['processor_group_id']

# --- ĐỊNH NGHĨA CÁC NHÓM CỘT (Dataset Taxi) ---

# Các cột sẽ được chuyển đổi: "1.0" -> 1 (int)
# (Dùng float-to-int cho an toàn với dữ liệu CSV bẩn)
FLOAT_TO_INT_COLS = ['VendorID', 'passenger_count', 'RateCodeID', 'payment_type']

# Các cột sẽ được chuyển đổi: "-73.9" -> -73.9 (float)
FLOAT_COLS = [
    'trip_distance', 'pickup_longitude', 'pickup_latitude',
    'dropoff_longitude', 'dropoff_latitude', 'fare_amount', 'extra',
    'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge',
    'total_amount'
]

# Các cột là TIMESTAMP (chuỗi ISO)
# Dataset 2015-01 dùng định dạng 'YYYY-MM-DD HH:MM:SS'
TIMESTAMP_COLS = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

# Các cột là CHUỖI
STRING_COLS = ['store_and_fwd_flag', 'produced_at'] # Thêm 'produced_at'

# --- HÀM TIỆN ÍCH "AN TOÀN" ---
def safe_convert(value, convert_func):
    """Hàm chung để thử chuyển đổi, nếu thất bại thì trả về None."""
    if value is None or value == '' or value == 'nan':
        return None
    try:
        return convert_func(value)
    except (ValueError, TypeError):
        return None

print("Khởi động Stream Processor (Taxi Dataset)...")

try:
    consumer = KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=PROCESSOR_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
except KafkaError as e:
    print(f"LỖI: Không thể kết nối Kafka. Docker của bạn đang chạy chứ? {e}")
    exit()

print("Kết nối Kafka thành công. Đang chờ tin nhắn...")
print(f"  - Đang nghe từ topic: {IN_TOPIC}")
print("-" * 30)

try:
    for message in consumer:
        raw_data = message.value
        # Dùng thời gian pickup làm ID log
        log_id = raw_data.get('tpep_pickup_datetime', 'Unknown')
        clean_data = {}

        try:
            # Bắt đầu xử lý từng cột
            for key, value in raw_data.items():

                # 1. Nhóm FLOAT_TO_INT ("1.0" -> 1)
                if key in FLOAT_TO_INT_COLS:
                    clean_data[key] = safe_convert(value, lambda v: int(float(v)))

                # 2. Nhóm FLOAT ("-73.9" -> -73.9)
                elif key in FLOAT_COLS:
                    clean_data[key] = safe_convert(value, float)

                # 3. Nhóm TIMESTAMP
                elif key in TIMESTAMP_COLS:
                    # Dữ liệu 2015 có định dạng '2015-01-15 19:05:39'
                    # Postgres có thể đọc trực tiếp, chỉ cần đảm bảo nó là chuỗi
                    clean_data[key] = safe_convert(value, str)

                # 4. Các cột CHUỖI còn lại
                elif key in STRING_COLS:
                     clean_data[key] = safe_convert(value, str)

                # 5. Bỏ qua các cột không xác định (nếu có)
                else:
                    continue

            # Gửi dữ liệu ĐÃ SẠCH đi
            producer.send(CLEAN_TOPIC, value=clean_data)
            print(f"Đã xử lý (Sạch):   {log_id}")

        except Exception as e:
            # Nếu có lỗi logic nào đó, gửi sang topic FAILED
            print(f"PHÁT HIỆN LỖI (Hỏng): {log_id}, Lỗi: {e}")
            failed_message = {
                'error': str(e),
                'failed_at': datetime.now().isoformat(),
                'original_message': raw_data
            }
            producer.send(FAILED_TOPIC, value=failed_message)

except KeyboardInterrupt:
    print("Đã dừng processor.")
finally:
    print("Đang đóng consumer và producer...")
    consumer.close()
    producer.flush()
    producer.close()
    print("Đã đóng.")
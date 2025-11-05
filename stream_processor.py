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

# --- ĐỊNH NGHĨA CÁC NHÓM CỘT ĐỂ "DỌN DẸP" ---
# (Dựa trên dữ liệu mẫu của bạn)

# Các cột sẽ được chuyển đổi: "1.0" -> 1 (int)
FLOAT_TO_INT_COLS = ['direction', 'stopEndSeq', 'payAmount']

# Các cột sẽ được chuyển đổi: "12" -> 12 (int)
INT_COLS = ['corridorID', 'stopStartSeq', 'payCardBirthDate'] # Thêm 'payCardBirthDate'

# Các cột sẽ được chuyển đổi: "-6.193488" -> -6.193488 (float)
FLOAT_COLS = ['tapInStopsLat', 'tapInStopsLon', 'tapOutStopsLat', 'tapOutStopsLon']

# Các cột là TIMESTAMP (chuỗi ISO)
TIMESTAMP_COLS = ['tapInTime', 'tapOutTime', 'produced_at']

# Các cột còn lại mặc định là TEXT/CHUỖI

# --- HÀM TIỆN ÍCH "AN TOÀN" ---
def safe_convert(value, convert_func):
    """Hàm chung để thử chuyển đổi, nếu thất bại thì trả về None."""
    if value is None or value == '':
        return None
    try:
        return convert_func(value)
    except (ValueError, TypeError):
        # Nếu giá trị là "abc" nhưng yêu cầu là int, nó sẽ trả về None
        return None

print("Khởi động Stream Processor (Phiên bản Thông minh)...")

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
        trans_id = raw_data.get('transID', 'Unknown')
        clean_data = {}
        errors_found = []

        try:
            # Bắt đầu xử lý từng cột
            for key, value in raw_data.items():

                # 1. Nhóm FLOAT_TO_INT ("1.0" -> 1)
                if key in FLOAT_TO_INT_COLS:
                    # Chuyển thành float trước, rồi mới thành int
                    clean_data[key] = safe_convert(value, lambda v: int(float(v)))

                # 2. Nhóm INT ("12" -> 12)
                elif key in INT_COLS:
                    clean_data[key] = safe_convert(value, int)

                # 3. Nhóm FLOAT ("-6.19" -> -6.19)
                elif key in FLOAT_COLS:
                    clean_data[key] = safe_convert(value, float)

                # 4. Nhóm TIMESTAMP (giữ nguyên là chuỗi, nhưng đảm bảo là None nếu rỗng)
                elif key in TIMESTAMP_COLS:
                    clean_data[key] = safe_convert(value, str)

                # 5. Các cột CHUỖI còn lại (transID, payCardID, v.v.)
                else:
                    clean_data[key] = safe_convert(value, str)

            # Gửi dữ liệu ĐÃ SẠCH đi
            producer.send(CLEAN_TOPIC, value=clean_data)
            print(f"Đã xử lý (Sạch):   transID={trans_id}")

        except Exception as e:
            # Nếu có lỗi logic nào đó, gửi sang topic FAILED
            print(f"PHÁT HIỆN LỖI (Hỏng): transID={trans_id}, Lỗi: {e}")
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
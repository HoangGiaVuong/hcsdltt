import json
import configparser
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

# 1. Định nghĩa các "topic" (quầy bưu điện)
# đọc từ file config.ini
config = configparser.ConfigParser()
config.read('config.ini')

IN_TOPIC = config['TOPICS']['raw_input_topic'] or 'bus_transactions'
CLEAN_TOPIC = config['TOPICS']['clean_output_topic'] or 'bus_transactions_clean'
FAILED_TOPIC = config['TOPICS']['failed_output_topic'] or 'bus_transactions_failed'
# đọc cấu hình Kafka từ file config
KAFKA_BROKERS = config['KAFKA']['bootstrap_servers'] or 'localhost:9092'
GROUP_ID = config['CONSUMER_GROUPS']['processor_group_id'] or 'bus_data_processor_group_v1'



# 2. Định nghĩa các cột và kiểu dữ liệu
#    Chúng ta sẽ dùng chúng để tự động chuyển đổi kiểu
INT_COLS = ['transID', 'direction', 'stopStartSeq', 'stopEndSeq', 'payAmount']
FLOAT_COLS = ['tapInStopsLat', 'tapInStopsLon', 'tapOutStopsLat', 'tapOutStopsLon']
# Các cột còn lại mặc định là chuỗi (string), không cần xử lý

print("Khởi động Stream Processor...")

try:
    # 3. Khởi tạo "Người tiêu thụ" (Consumer)
    #    - Nó sẽ "nghe" từ topic 'bus_transactions'
    #    - 'group_id' rất quan trọng. Nó giúp Kafka biết
    #       script này đã đọc đến tin nhắn nào.
    #    - 'auto_offset_reset='latest'': Chỉ đọc các tin nhắn MỚI NHẤT
    consumer = KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=GROUP_ID,
        # Tự động giải mã (decode) JSON từ producer
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

    # 4. Khởi tạo "Người sản xuất" (Producer)
    #    - Script này vừa "nghe" vừa "gửi".
    #    - Nó sẽ gửi dữ liệu đã được xử lý đi
    producer = KafkaProducer(
        bootstrap_servers= KAFKA_BROKERS,
        # Mã hóa (encode) dữ liệu thành JSON
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

except KafkaError as e:
    print(f"LỖI: Không thể kết nối Kafka. Docker của bạn đang chạy chứ?")
    print(f"Lỗi: {e}")
    exit()

print("Kết nối Kafka thành công. Đang chờ tin nhắn...")
print(f"  - Đang nghe từ topic: {IN_TOPIC}")
print(f"  - Sẽ gửi dữ liệu sạch tới: {CLEAN_TOPIC}")
print(f"  - Sẽ gửi dữ liệu lỗi tới: {FAILED_TOPIC}")
print("-" * 30)

# 5. Vòng lặp vô tận để xử lý luồng
try:
    for message in consumer:
        # Nhận tin nhắn (dưới dạng dict)
        raw_data = message.value
        trans_id = raw_data.get('transID', 'Unknown')

        try:
            # 6. BẮT ĐẦU XỬ LÝ (Transformation)
            #    Đây là "công việc" chính của Lớp 2
            clean_data = {}

            # Duyệt qua tất cả dữ liệu thô
            for key, value in raw_data.items():
                # Nếu giá trị là None hoặc chuỗi rỗng
                if value is None or value == '':
                    clean_data[key] = None
                    continue

                # Chuyển đổi sang INT (số nguyên)
                if key in INT_COLS:
                    clean_data[key] = int(value)
                # Chuyển đổi sang FLOAT (số thực)
                elif key in FLOAT_COLS:
                    clean_data[key] = float(value)
                # Giữ nguyên là chuỗi (string)
                else:
                    clean_data[key] = str(value) # Đảm bảo nó là string

            # (Bạn có thể thêm các logic phức tạp hơn ở đây,
            #  ví dụ: tính toán thời gian chuyến đi)

            # 7. Gửi dữ liệu ĐÃ SẠCH đi
            producer.send(CLEAN_TOPIC, value=clean_data)
            print(f"Đã xử lý (Sạch):   transID={trans_id}")

        except ValueError as ve:
            # 8. XỬ LÝ LỖI (Quan trọng)
            #    Nếu một tin nhắn bị lỗi (ví dụ: 'payAmount' = 'abc')
            #    Nó sẽ bị gửi sang topic FAILED
            print(f"PHÁT HIỆN LỖI (Hỏng): transID={trans_id}, Lỗi: {ve}")
            failed_message = {
                'error': str(ve),
                'failed_at': datetime.now().isoformat(),
                'original_message': raw_data
            }
            producer.send(FAILED_TOPIC, value=failed_message)

except KeyboardInterrupt:
    print("Đã dừng processor.")
finally:
    # 9. Đóng kết nối
    print("Đang đóng consumer và producer...")
    consumer.close()
    producer.flush()
    producer.close()
    print("Đã đóng.")
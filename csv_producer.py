import pandas as pd
import time
import json
import configparser
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime

# --- BƯỚC 2: Đọc file config ---
config = configparser.ConfigParser()
config.read('config.ini')

# đọc cấu hình Kafka từ file config
KAFKA_BROKERS = config['KAFKA']['bootstrap_servers'] or 'localhost:9092'

# đọc topic từ file config
KAFKA_TOPIC = config['TOPICS']['raw_input_topic'] or 'bus_transactions'
CSV_FILE = config['APPLICATION_SETTINGS']['csv_file_path'] or 'source.csv'

# 1. Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    # Thêm 'default=str' để tự động xử lý các kiểu dữ liệu phức tạp
)



try:
    # 3. Đọc file CSV
    #    Chúng ta dùng 'dtype=str' để đọc TẤT CẢ các cột dưới dạng CHUỖI.
    #    Việc này ngăn pandas "thông minh" làm hỏng ID (ví dụ: 'S01' thành 1)
    print(f"Đọc file {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE, dtype=str)
    print("Đọc file thành công.")

except FileNotFoundError:
    print(f"LỖI: Không tìm thấy file {CSV_FILE}.")
    print("Bạn đã chạy file 'generate_fake_data.py' chưa?")
    exit()

print(f"Bắt đầu gửi dữ liệu đến topic: {KAFKA_TOPIC}...")
print("Nhấn Ctrl+C để dừng.")

try:
    while True:
        # Lấy ngẫu nhiên 1 dòng từ DataFrame
        # .iloc[0] để lấy Series, sau đó .to_dict() để chuyển thành dict
        sample_row = df.sample(n=1).iloc[0]

        # 4. Chuyển đổi kiểu dữ liệu một cách CÓ CHỦ ĐÍCH
        #    Đây là bước quan trọng trong Data Engineering
        #    Chúng ta kiểm soát chính xác kiểu dữ liệu nào được gửi đi.

        data_to_send = {
            'transID': int(sample_row['transID']),
            'payCardID': sample_row['payCardID'],
            'payCardBank': sample_row['payCardBank'],
            'payCardName': sample_row['payCardName'],
            'payCardSex': sample_row['payCardSex'],
            'payCardBirthDate': sample_row['payCardBirthDate'], # Giữ dạng chuỗi YYYY-MM-DD
            'corridorID': sample_row['corridorID'],
            'corridorName': sample_row['corridorName'],
            'direction': int(sample_row['direction']),
            'tapInStops': sample_row['tapInStops'],
            'tapInStopsName': sample_row['tapInStopsName'],
            'tapInStopsLat': float(sample_row['tapInStopsLat']),
            'tapInStopsLon': float(sample_row['tapInStopsLon']),
            'stopStartSeq': int(sample_row['stopStartSeq']),
            'tapInTime': sample_row['tapInTime'], # Giữ dạng chuỗi ISO timestamp
            'tapOutStops': sample_row['tapOutStops'],
            'tapOutStopsName': sample_row['tapOutStopsName'],
            'tapOutStopsLat': float(sample_row['tapOutStopsLat']),
            'tapOutStopsLon': float(sample_row['tapOutStopsLon']),
            'stopEndSeq': int(sample_row['stopEndSeq']),
            'tapOutTime': sample_row['tapOutTime'], # Giữ dạng chuỗi ISO timestamp
            'payAmount': int(sample_row['payAmount']),

            # Thêm thời gian "sản xuất" (produce time) để biết event mới
            'produced_at': datetime.now().isoformat()
        }

        # 5. Gửi dữ liệu đi
        producer.send(KAFKA_TOPIC, value=data_to_send)

        print(f"Đã gửi transID: {data_to_send['transID']}")

        # Giả lập độ trễ
        time.sleep(0.5) # Gửi 2 tin nhắn mỗi giây

except KeyboardInterrupt:
    print("Đã dừng giả lập.")
finally:
    producer.flush()
    producer.close()
    print("Đã đóng Producer.")
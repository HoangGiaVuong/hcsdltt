import pandas as pd
import time
import json
import configparser
from kafka import KafkaProducer
from datetime import datetime

# --- Đọc file config ---
config = configparser.ConfigParser()
config.read('config.ini')

KAFKA_BROKERS = config['KAFKA']['bootstrap_servers']
KAFKA_TOPIC = config['TOPICS']['raw_input_topic']
CSV_FILE = config['APPLICATION_SETTINGS']['csv_file_path']

# 1. Khởi tạo Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
except Exception as e:
    print(f"LỖI: Không thể kết nối đến Kafka. Docker của bạn đang chạy chứ?")
    print(f"Lỗi: {e}")
    exit()

try:
    # 2. Đọc file CSV
    #    QUAN TRỌNG: dtype=str đọc TẤT CẢ mọi thứ dưới dạng chuỗi.
    print(f"Đang đọc file {CSV_FILE}...")
    # Thêm 'low_memory=False' để tối ưu hóa việc đọc file CSV lớn
    df = pd.read_csv(CSV_FILE, dtype=str, low_memory=False)

    # Thay thế các giá trị 'nan' (chuỗi 'nan' do pandas đọc) thành None (null)
    df = df.where(pd.notnull(df), None)

    print(f"Đọc thành công. Tìm thấy {len(df)} dòng.")

except FileNotFoundError:
    print(f"LỖI: Không tìm thấy file có tên '{CSV_FILE}'.")
    exit()
except Exception as e:
    print(f"Lỗi khi đọc file CSV: {e}")
    exit()

print(f"Bắt đầu gửi dữ liệu từ file {CSV_FILE} đến topic: {KAFKA_TOPIC}...")
print("Nhấn Ctrl+C để dừng.")

try:
    while True:
        # 3. Lấy ngẫu nhiên 1 dòng
        sample_row = df.sample(n=1).iloc[0]

        # 4. Chuyển thẳng sang dict.
        data_to_send = sample_row.to_dict()

        # 5. Thêm timestamp
        data_to_send['produced_at'] = datetime.now().isoformat()

        # 6. Gửi đi
        producer.send(KAFKA_TOPIC, value=data_to_send)

        # Cố gắng in một ID có ý nghĩa (nếu có)
        id_to_print = data_to_send.get('tpep_pickup_datetime', '...')
        print(f"Đã gửi: {id_to_print}")

        # Dataset này lớn, chúng ta có thể gửi nhanh hơn
        time.sleep(0.1) # 10 tin nhắn/giây

except KeyboardInterrupt:
    print("Đã dừng giả lập.")
except Exception as e:
    print(f"Đã xảy ra lỗi trong vòng lặp gửi: {e}")
finally:
    print("Đang dọn dẹp và đóng producer...")
    producer.flush()
    producer.close()
    print("Đã đóng Producer.")
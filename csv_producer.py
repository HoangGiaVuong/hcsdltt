import csv
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

# --- Cài đặt Tốc độ ---
# Điều chỉnh tốc độ gửi (tin nhắn/giây)
# 0.1 = 10 tin nhắn/giây
# 0.01 = 100 tin nhắn/giây
# 0.001 = 1000 tin nhắn/giây
# 0 = Nhanh nhất có thể (có thể làm CPU 100%)
# Bắt đầu với tốc độ vừa phải để kiểm tra
SLEEP_TIME = 0.001

# 1. Khởi tạo Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        # Tăng tốc Kafka Producer (gom tin nhắn lại)
        linger_ms=100, # Chờ 100ms để gom tin nhắn
        batch_size=65536 # Kích thước batch 64KB
    )
    print("Đã kết nối Kafka...")
except Exception as e:
    print(f"LỖI: Không thể kết nối đến Kafka. Docker của bạn đang chạy chứ?")
    print(f"Lỗi: {e}")
    exit()

def stream_csv_file():
    """
    Hàm đọc file CSV từng dòng một và gửi đi.
    Lặp vô tận khi hết file.
    """
    count = 0
    total_sent = 0
    start_time = time.time()

    print(f"Bắt đầu gửi dữ liệu từ {CSV_FILE} đến topic {KAFKA_TOPIC}...")
    if SLEEP_TIME > 0:
        print(f"Tốc độ: ~{1/SLEEP_TIME:.0f} tin nhắn/giây (ước tính)")
    else:
        print("Tốc độ: TỐI ĐA (CPU-bound)")
    print("Nhấn Ctrl+C để dừng.")

    try:
        # 2. Lặp vô tận để giả lập luồng không bao giờ dừng
        while True:
            print("\n--- Bắt đầu đọc file từ đầu (Looping) ---")
            count_this_loop = 0

            try:
                # 3. Mở file và đọc bằng thư viện 'csv'
                with open(CSV_FILE, mode='r', encoding='utf-8') as f:
                    # DictReader tự động đọc header (dòng đầu tiên)
                    # và biến mỗi dòng thành một dict
                    reader = csv.DictReader(f)

                    # 4. Đọc TỪNG DÒNG MỘT (An toàn RAM)
                    for row_dict in reader:
                        # row_dict bây giờ là một dict, ví dụ:
                        # {'VendorID': '1', 'tpep_pickup_datetime': '...'}

                        # 5. Thêm timestamp (giống như trước)
                        row_dict['produced_at'] = datetime.now().isoformat()

                        # 6. Gửi đi
                        producer.send(KAFKA_TOPIC, value=row_dict)

                        count += 1
                        count_this_loop += 1
                        total_sent += 1

                        # In ra thông báo sau mỗi 10000 tin nhắn
                        if count % 10000 == 0:
                            print(f"Đã gửi {count} tin nhắn...")
                            count = 0 # Reset bộ đếm

                        # 7. Tạm dừng để giả lập tốc độ
                        if SLEEP_TIME > 0:
                            time.sleep(SLEEP_TIME)

            except FileNotFoundError:
                print(f"LỖI: Không tìm thấy file có tên '{CSV_FILE}'.")
                print("Hãy kiểm tra lại 'config.ini' và tên file của bạn.")
                return # Thoát hàm
            except Exception as e:
                print(f"Lỗi khi đang đọc file: {e}")
                time.sleep(5) # Chờ 5 giây rồi thử lại

            print(f"--- Kết thúc file (đã gửi {count_this_loop} dòng) ---")

    except KeyboardInterrupt:
        print("\nĐã dừng giả lập.")
    except Exception as e:
        print(f"Đã xảy ra lỗi trong vòng lặp gửi: {e}")
    finally:
        end_time = time.time()
        total_time = end_time - start_time

        print("Đang dọn dẹp và đóng producer (flush)...")
        producer.flush() # Gửi nốt các tin nhắn còn trong batch
        producer.close()

        if total_time > 0.01:
            print(f"Đã đóng Producer.")
            print(f"Tóm tắt: Đã gửi {total_sent} tin nhắn trong {total_time:.2f} giây.")
            print(f"Tốc độ trung bình: {total_sent / total_time:.2f} tin nhắn/giây.")
        else:
            print("Đã đóng Producer.")

# Chạy hàm chính
if __name__ == "__main__":
    stream_csv_file()
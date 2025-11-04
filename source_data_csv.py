import csv
import random
from faker import Faker
from datetime import datetime, timedelta

# Khởi tạo Faker để tạo dữ liệu giả
fake = Faker('vi_VN')  # Sử dụng ngôn ngữ Tiếng Việt

# Định nghĩa các cột
HEADER = [
    'transID', 'payCardID', 'payCardBank', 'payCardName', 'payCardSex',
    'payCardBirthDate', 'corridorID', 'corridorName', 'direction',
    'tapInStops', 'tapInStopsName', 'tapInStopsLat', 'tapInStopsLon',
    'stopStartSeq', 'tapInTime', 'tapOutStops', 'tapOutStopsName',
    'tapOutStopsLat', 'tapOutStopsLon', 'stopEndSeq', 'tapOutTime', 'payAmount'
]

# Dữ liệu mẫu
banks = ['Vietcombank', 'Techcombank', 'MB Bank', 'Agribank', 'BIDV']
corridors = {'T01': 'Tuyến Bến Thành - Suối Tiên'}
stops = {
    'S01': {'name': 'Bến Thành', 'lat': 10.7714, 'lon': 106.6984},
    'S15': {'name': 'Trạm Suối Tiên', 'lat': 10.8719, 'lon': 106.8017}
}

print("Bắt đầu tạo file bus_data.csv...")

with open('bus_data.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(HEADER)  # Ghi header

    start_trans_id = 500000

    for i in range(1000):  # Tạo 1000 dòng dữ liệu
        trans_id = start_trans_id + i
        sex = random.choice(['Nam', 'Nữ'])
        name = fake.name_male() if sex == 'Nam' else fake.name_female()
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d')

        # Giả lập thời gian vào và ra
        tap_in_time = datetime.now() - timedelta(minutes=random.randint(5, 60))
        tap_out_time = tap_in_time + timedelta(minutes=random.randint(5, 20))

        row = [
            trans_id,  # transID
            fake.credit_card_number(),  # payCardID
            random.choice(banks),  # payCardBank
            name,  # payCardName
            sex,  # payCardSex
            birth_date,  # payCardBirthDate
            'T01',  # corridorID
            corridors['T01'],  # corridorName
            random.choice([0, 1]),  # direction
            'S01',  # tapInStops
            stops['S01']['name'],  # tapInStopsName
            stops['S01']['lat'],  # tapInStopsLat
            stops['S01']['lon'],  # tapInStopsLon
            1,  # stopStartSeq
            tap_in_time.isoformat(),  # tapInTime
            'S15',  # tapOutStops
            stops['S15']['name'],  # tapOutStopsName
            stops['S15']['lat'],  # tapOutStopsLat
            stops['S15']['lon'],  # tapOutStopsLon
            15,  # stopEndSeq
            tap_out_time.isoformat(),  # tapOutTime
            random.randint(3000, 7000)  # payAmount
        ]
        writer.writerow(row)

print(f"Đã tạo xong bus_data.csv với {1000} dòng.")
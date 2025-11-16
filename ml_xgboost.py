import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import psycopg2
import psycopg2.extras
import time
import random
import re
import xgboost as xgb
import configparser

# --- PHẦN 1: TƯƠNG TÁC VỚI CSDL POSTGRESQL ---

# !!! QUAN TRỌNG: CẬP NHẬT THÔNG TIN CỦA BẠN VÀO ĐÂY !!!

config = configparser.ConfigParser()
config.read('config.ini')

DB_CONFIG = {
    "dbname": config['POSTGRES']['database'],
    "user": config['POSTGRES']['user'],
    "password": config['POSTGRES']['password'],
    "host": config['POSTGRES']['host'],
    "port": config['POSTGRES']['port']
}

def get_db_connection():
    """Tạo kết nối mới tới CSDL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Lỗi: Không thể kết nối tới CSDL.")
        print(e)
        return None

def get_actual_cost_from_db(conn, query_params, plan_id):
    """
    Thực thi EXPLAIN ANALYZE để lấy chi phí thực tế (thời gian thực thi).
    Chúng ta sẽ "buộc" CSDL dùng kế hoạch mong muốn.
    """

    # Template truy vấn của chúng ta
    sql_template = """
    SELECT COUNT(*)
    FROM Fact_Trips
    WHERE vendor_id_fk = %(vendor_id)s AND tip_amount > %(tip_amount)s;
    """

    # Các lệnh để "buộc" kế hoạch
    plan_settings = {
        'plan_full_scan': [
            "SET enable_indexscan = off;",
            "SET enable_bitmapscan = off;",
            "SET enable_seqscan = on;"
        ],
        'plan_index_vendor': [
            "SET enable_indexscan = on;",
            "SET enable_bitmapscan = on;",
            "SET enable_seqscan = off;"
        ],
        'plan_index_tip': [
            "SET enable_indexscan = on;",
            "SET enable_bitmapscan = on;",
            "SET enable_seqscan = off;"
        ]
    }

    # Regex để trích xuất thời gian thực thi từ EXPLAIN ANALYZE
    time_regex = re.compile(r"Execution Time: (\d+\.\d+) ms")

    cost_ms = None

    with conn.cursor() as cur:
        try:
            # 1. Thiết lập các tham số để buộc kế hoạch
            settings = plan_settings.get(plan_id)
            if not settings:
                raise ValueError(f"Kế hoạch không xác định: {plan_id}")

            for setting in settings:
                cur.execute(setting)

            # 2. Chạy EXPLAIN ANALYZE
            cur.execute(f"EXPLAIN ANALYZE {sql_template}", query_params)

            # 3. Lấy kết quả
            explain_output = cur.fetchall()

            # 4. Trích xuất (parse) thời gian thực thi
            for line in explain_output:
                match = time_regex.search(line[0])
                if match:
                    cost_ms = float(match.group(1))
                    break

            if cost_ms is None:
                print(f"Cảnh báo: Không tìm thấy Execution Time cho {query_params}, {plan_id}")
                return None

        except psycopg2.Error as e:
            print(f"Lỗi khi thực thi truy vấn: {e}")
            conn.rollback() # Hoàn tác nếu có lỗi
            return None
        finally:
            # Luôn reset lại các thiết lập về mặc định
            cur.execute("RESET enable_indexscan;")
            cur.execute("RESET enable_bitmapscan;")
            cur.execute("RESET enable_seqscan;")

    return cost_ms

def generate_historical_logs(n_samples=5000):
    """
    Tạo log lịch sử bằng cách kết nối CSDL và chạy EXPLAIN ANALYZE.
    """
    print(f"Đang tạo {n_samples} mẫu log lịch sử từ CSDL PostgreSQL...")
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame() # Trả về DF rỗng nếu không kết nối được

    logs = []
    possible_plans = ['plan_full_scan', 'plan_index_vendor', 'plan_index_tip']

    # Lấy các giá trị min/max thực tế từ CSDL
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(vendor_id_fk), MAX(vendor_id_fk) FROM Fact_Trips")
        min_vendor, max_vendor = cur.fetchone()

        cur.execute("SELECT MIN(tip_amount), MAX(tip_amount) FROM Fact_Trips")
        min_tip, max_tip = cur.fetchone()

    print(f"Phạm vi dữ liệu: VendorID ({min_vendor}-{max_vendor}), Tip ({min_tip}-{max_tip})")

    for i in range(n_samples):
        # 1. Tạo một truy vấn ngẫu nhiên
        query_params = {
            'vendor_id': random.randint(min_vendor, max_vendor),
            'tip_amount': round(random.uniform(min_tip, max_tip / 10), 2) # Giả sử tip > X
        }

        # 2. Chọn một kế hoạch ngẫu nhiên để "thực thi"
        plan_id = random.choice(possible_plans)

        # 3. Lấy chi phí thực tế từ CSDL
        cost = get_actual_cost_from_db(conn, query_params, plan_id)

        if cost is not None:
            logs.append({
                'vendor_id': query_params['vendor_id'],
                'tip_amount': query_params['tip_amount'],
                'plan_id': plan_id,
                'actual_cost': cost # Chi phí là mili-giây
            })

        if (i + 1) % 100 == 0:
            print(f"  Đã hoàn thành {i + 1}/{n_samples} mẫu...")

    conn.close()
    print("Hoàn tất tạo log.")
    return pd.DataFrame(logs)

# --- PHẦN 2: HUẤN LUYỆN MÔ HÌNH ML ---

def train_cost_model(log_df):
    """
    Huấn luyện mô hình XGBoost để dự đoán 'actual_cost'
    từ các đặc trưng của truy vấn và kế hoạch.
    """
    print("Đang huấn luyện mô hình dự đoán chi phí (XGBoost)...")

    if log_df.empty or len(log_df) < 100:
        print("Lỗi: Dữ liệu log không đủ để huấn luyện.")
        return None

    features = ['vendor_id', 'tip_amount', 'plan_id']
    target = 'actual_cost'

    X = log_df[features]
    y = log_df[target]

    categorical_features = ['plan_id']
    numerical_features = ['vendor_id', 'tip_amount']

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', 'passthrough', numerical_features),
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
        ])

    # --- THAY ĐỔI 2: Thay thế RandomForest bằng XGBoost ---
    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', xgb.XGBRegressor(
            objective='reg:squarederror', # Mục tiêu là hồi quy (lỗi bình phương)
            n_estimators=200,             # Số lượng "cây"
            learning_rate=0.05,           # Tốc độ học
            max_depth=5,                  # Độ sâu tối đa của cây
            random_state=42,
            n_jobs=-1                     # Sử dụng tất cả các lõi CPU
        ))
    ])
    # -----------------------------------------------------

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    start_time = time.time()
    # Huấn luyện mô hình (XGBoost có thể cần early stopping trong thực tế)
    model_pipeline.fit(X_train, y_train)
    end_time = time.time()

    print(f"Huấn luyện xong trong {end_time - start_time:.2f} giây.")

    score = model_pipeline.score(X_test, y_test)
    print(f"Model R^2 score trên tập test: {score:.4f}")

    return model_pipeline

# --- PHẦN 3: LỚP MÔ PHỎNG TỐI ƯU HÓA ---
# (Không thay đổi. Lớp này hoạt động với bất kỳ mô hình
# nào tuân thủ API của scikit-learn)

class MLQueryOptimizer:
    """
    Mô phỏng bộ tối ưu hóa truy vấn sử dụng mô hình ML
    để ước tính chi phí và chọn kế hoạch tốt nhất.
    """
    def __init__(self, cost_model_pipeline):
        self.model = cost_model_pipeline
        self.candidate_plans = ['plan_full_scan', 'plan_index_vendor', 'plan_index_tip']

    def get_optimal_plan(self, query_params):
        """
        Dự đoán kế hoạch tối ưu cho một truy vấn mới.
        """
        print(f"\nĐang tìm kế hoạch tối ưu cho truy vấn: {query_params}")

        if not self.model:
            print("Lỗi: Bộ tối ưu hóa chưa được huấn luyện.")
            return None, 0

        best_plan = None
        min_predicted_cost = float('inf')
        plan_costs = {}

        vendor_id = query_params['vendor_id']
        tip_amount = query_params['tip_amount']

        for plan_id in self.candidate_plans:
            input_df = pd.DataFrame([{
                'vendor_id': vendor_id,
                'tip_amount': tip_amount,
                'plan_id': plan_id
            }])

            predicted_cost = self.model.predict(input_df)[0]
            plan_costs[plan_id] = predicted_cost

            if predicted_cost < min_predicted_cost:
                min_predicted_cost = predicted_cost
                best_plan = plan_id

        print("--- Chi phí dự đoán bởi ML Model (XGBoost) (đơn vị: ms) ---")
        for plan, cost in sorted(plan_costs.items(), key=lambda item: item[1]):
            print(f"  {plan}: {cost:,.3f} ms")
        print("-------------------------------------------------------")

        return best_plan, min_predicted_cost

# --- PHẦN 4: CHẠY MÔ PHỎNG ---

if __name__ == "__main__":

    # BƯỚC 1: TẠO DỮ LIỆU HUẤN LUYỆN TỪ CSDL
    # (Bạn có thể comment dòng này nếu đã có file log)
    historical_logs_df = generate_historical_logs(n_samples=10000)

    # Lưu log lại để không phải tạo lại
    if not historical_logs_df.empty:
        log_filename = "pg_query_logs_xgb.csv"
        historical_logs_df.to_csv(log_filename, index=False)
        print(f"Đã lưu log lịch sử vào '{log_filename}'")

        # BƯỚC 2: HUẤN LUYỆN MÔ HÌNH
        # (Nếu bạn đã có file log, hãy load nó ở đây)
        # historical_logs_df = pd.read_csv("pg_query_logs_xgb.csv")

        ml_cost_model = train_cost_model(historical_logs_df)

        if ml_cost_model:
            # BƯỚC 3: KHỞI TẠO BỘ TỐI ƯU HÓA
            optimizer = MLQueryOptimizer(ml_cost_model)

            # BƯỚC 4: MÔ PHỎNG CÁC TRUY VẤN MỚI
            vid_sample = historical_logs_df['vendor_id'].sample(1).iloc[0]
            tip_sample_low = historical_logs_df['tip_amount'].quantile(0.1)
            tip_sample_high = historical_logs_df['tip_amount'].quantile(0.9)

            query1 = {'vendor_id': vid_sample, 'tip_amount': tip_sample_low}
            best_plan1, cost1 = optimizer.get_optimal_plan(query1)
            print(f"==> Quyết định: Dùng '{best_plan1}' (Chi phí dự đoán: {cost1:,.3f} ms)")

            query2 = {'vendor_id': vid_sample, 'tip_amount': tip_sample_high}
            best_plan2, cost2 = optimizer.get_optimal_plan(query2)
            print(f"==> Quyết định: Dùng '{best_plan2}' (Chi phí dự đoán: {cost2:,.3f} ms)")
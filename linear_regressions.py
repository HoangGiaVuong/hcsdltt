import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression # <<< Thuật toán 1
from sklearn.tree import DecisionTreeRegressor  # <<< Thuật toán 2
import psycopg2
import psycopg2.extras
import time
import random
import re
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
    (Hàm này giữ nguyên)
    """

    # Template truy vấn của chúng ta
    sql_template = """
    SELECT COUNT(*)
    FROM Fact_Trips
    WHERE vendor_id_fk = %(vendor_id)s AND tip_amount > %(tip_amount)s;
    """

    plan_settings = {
        'plan_full_scan': [
            "SET enable_indexscan = off;", "SET enable_bitmapscan = off;",
            "SET enable_seqscan = on;"
        ],
        'plan_index_vendor': [
            "SET enable_indexscan = on;", "SET enable_bitmapscan = on;",
            "SET enable_seqscan = off;"
        ],
        'plan_index_tip': [
            "SET enable_indexscan = on;", "SET enable_bitmapscan = on;",
            "SET enable_seqscan = off;"
        ]
    }

    time_regex = re.compile(r"Execution Time: (\d+\.\d+) ms")
    cost_ms = None

    with conn.cursor() as cur:
        try:
            settings = plan_settings.get(plan_id)
            for setting in settings:
                cur.execute(setting)

            cur.execute(f"EXPLAIN ANALYZE {sql_template}", query_params)
            explain_output = cur.fetchall()

            for line in explain_output:
                match = time_regex.search(line[0])
                if match:
                    cost_ms = float(match.group(1))
                    break

            if cost_ms is None: return None

        except psycopg2.Error as e:
            conn.rollback()
            return None
        finally:
            cur.execute("RESET enable_indexscan;")
            cur.execute("RESET enable_bitmapscan;")
            cur.execute("RESET enable_seqscan;")

    return cost_ms

def generate_historical_logs(n_samples=2000):
    """
    Tạo log lịch sử bằng cách kết nối CSDL và chạy EXPLAIN ANALYZE.
    (Hàm này giữ nguyên)
    """
    print(f"Đang tạo {n_samples} mẫu log lịch sử từ CSDL PostgreSQL...")
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()

    logs = []
    # Chỉ 3 kế hoạch này là liên quan đến truy vấn của chúng ta
    possible_plans = ['plan_full_scan', 'plan_index_vendor', 'plan_index_tip']

    with conn.cursor() as cur:
        cur.execute("SELECT MIN(vendor_id_fk), MAX(vendor_id_fk) FROM Fact_Trips")
        min_vendor, max_vendor = cur.fetchone()
        cur.execute("SELECT MIN(tip_amount), MAX(tip_amount) FROM Fact_Trips")
        min_tip, max_tip = cur.fetchone()

    print(f"Phạm vi dữ liệu: VendorID ({min_vendor}-{max_vendor}), Tip ({min_tip}-{max_tip})")

    for i in range(n_samples):
        query_params = {
            'vendor_id': random.randint(min_vendor, max_vendor),
            'tip_amount': round(random.uniform(min_tip, max_tip / 10), 2)
        }
        plan_id = random.choice(possible_plans)
        cost = get_actual_cost_from_db(conn, query_params, plan_id)

        if cost is not None:
            logs.append({
                'vendor_id': query_params['vendor_id'],
                'tip_amount': query_params['tip_amount'],
                'plan_id': plan_id,
                'actual_cost': cost
            })

        if (i + 1) % 100 == 0:
            print(f"  Đã hoàn thành {i + 1}/{n_samples} mẫu...")

    conn.close()
    print("Hoàn tất tạo log.")
    return pd.DataFrame(logs)

# --- PHẦN 2: HUẤN LUYỆN MÔ HÌNH ML ---

def create_preprocessor():
    """
    Tạo pipeline tiền xử lý chung.
    <<< THAY ĐỔI: Đặc trưng là 'vendor_id' và 'tip_amount' >>>
    """

    categorical_features = ['plan_id']
    # Đây là các đặc trưng từ log CSDL của chúng ta
    numerical_features = ['vendor_id', 'tip_amount']

    # Pipeline cho đặc trưng SỐ
    # StandardScaler rất quan trọng cho Linear Regression
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler()) # Scale dữ liệu
    ])

    # Pipeline cho đặc trưng PHÂN LOẠI
    categorical_transformer = Pipeline(steps=[
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])

    # Kết hợp bằng ColumnTransformer
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numerical_features),
            ('cat', categorical_transformer, categorical_features)
        ])
    return preprocessor

def train_model_pipeline(log_df, model_object, model_name):
    """
    Hàm chung để huấn luyện một mô hình (LR hoặc DT)
    (Hàm này giữ nguyên)
    """
    print(f"\n--- Đang huấn luyện mô hình: {model_name} ---")

    # Đặc trưng của chúng ta bây giờ là từ CSDL
    features = ['vendor_id', 'tip_amount', 'plan_id']
    target = 'actual_cost'

    X = log_df[features]
    y = log_df[target]

    preprocessor = create_preprocessor()

    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', model_object)
    ])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    print("Bắt đầu huấn luyện...")
    start_time = time.time()
    model_pipeline.fit(X_train, y_train)
    end_time = time.time()
    print(f"Huấn luyện xong trong {end_time - start_time:.2f} giây.")

    score = model_pipeline.score(X_test, y_test)
    print(f"Model R^2 score trên tập test: {score:.4f}")

    return model_pipeline

# --- PHẦN 3: LỚP MÔ PHỎNG TỐI ƯU HÓA ---

class MLQueryOptimizer:
    """
    Bộ tối ưu hóa truy vấn cho template truy vấn cụ thể.
    """
    def __init__(self, cost_model_pipeline, model_name="Model"):
        self.model = cost_model_pipeline
        self.model_name = model_name
        # <<< THAY ĐỔI: Chỉ 3 kế hoạch này được huấn luyện >>>
        self.candidate_plans = ['plan_full_scan', 'plan_index_vendor', 'plan_index_tip']

    def get_optimal_plan(self, query_params):
        """
        Dự đoán kế hoạch tối ưu cho một truy vấn mới (dựa trên tham số).
        <<< THAY ĐỔI: Xử lý 'query_params' thay vì 'query_context' >>>
        """
        print(f"\n--- Đang tìm kế hoạch (Sử dụng: {self.model_name}) cho: {query_params} ---")

        if not self.model:
            print("Lỗi: Bộ tối ưu hóa chưa được huấn luyện.")
            return None, 0

        best_plan = None
        min_predicted_cost = float('inf')
        plan_costs = {}

        # Lấy các tham số truy vấn
        vendor_id = query_params['vendor_id']
        tip_amount = query_params['tip_amount']

        # Tạo 3 hàng đầu vào (1 cho mỗi kế hoạch)
        input_data_list = []
        for plan_id in self.candidate_plans:
            features = {
                'plan_id': plan_id,
                'vendor_id': vendor_id,
                'tip_amount': tip_amount
            }
            input_data_list.append(features)

        input_df = pd.DataFrame(input_data_list)

        # Dự đoán chi phí cho cả 3 kịch bản
        predicted_costs = self.model.predict(input_df)

        # Tìm chi phí thấp nhất
        print(f"--- Chi phí dự đoán bởi {self.model_name} (đơn vị: ms) ---")
        for i, plan_id in enumerate(self.candidate_plans):
            cost = predicted_costs[i]
            plan_costs[plan_id] = cost
            print(f"  {plan_id:<20}: {cost:,.3f} ms")

            if cost < min_predicted_cost:
                min_predicted_cost = cost
                best_plan = plan_id
        print("-------------------------------------------------------")

        return best_plan, min_predicted_cost

# --- PHẦN 4: CHẠY MÔ PHỎNG VÀ SO SÁNH ---

if __name__ == "__main__":

    # 1. Tạo dữ liệu lịch sử (CHỈ 1 LẦN)
    # Cảnh báo: Sẽ chạy n_samples truy vấn lên CSDL của bạn.
    # (Bạn có thể comment dòng này nếu đã có file log)
    historical_logs_df = generate_historical_logs(n_samples=10000)

    if historical_logs_df.empty or len(historical_logs_df) < 100:
        print("Không thể tạo/load log. Dừng chương trình.")
    else:
        # Lưu lại để dùng sau
        log_filename = "pg_query_logs_lr_dt.csv"
        historical_logs_df.to_csv(log_filename, index=False)
        print(f"Đã lưu log lịch sử vào '{log_filename}'")

        # (Nếu bạn đã có file log, hãy load nó ở đây)
        # historical_logs_df = pd.read_csv(log_filename)

        # 2. Huấn luyện cả hai mô hình

        # Huấn luyện Hồi quy tuyến tính
        lr_model = train_model_pipeline(
            historical_logs_df,
            LinearRegression(),
            "Linear Regression"
        )

        # Huấn luyện Cây quyết định
        dt_model = train_model_pipeline(
            historical_logs_df,
            DecisionTreeRegressor(max_depth=10, random_state=42),
            "Decision Tree (max_depth=10)"
        )

        # 3. Khởi tạo hai bộ tối ưu hóa
        optimizer_lr = MLQueryOptimizer(lr_model, "Linear Regression")
        optimizer_dt = MLQueryOptimizer(dt_model, "Decision Tree")

        # 4. Mô phỏng các kịch bản truy vấn mới và SO SÁNH
        # Lấy một vài giá trị mẫu
        vid_sample_low = historical_logs_df['vendor_id'].quantile(0.25)
        vid_sample_high = historical_logs_df['vendor_id'].quantile(0.75)
        tip_sample_low = historical_logs_df['tip_amount'].quantile(0.1)
        tip_sample_high = historical_logs_df['tip_amount'].quantile(0.9)

        # KỊCH BẢN 1: Truy vấn có vẻ chọn lọc (tip thấp)
        print("\n\n" + "="*50)
        print("KỊCH BẢN 1: Truy vấn có vẻ chọn lọc")
        print("="*50)
        query1 = {'vendor_id': int(vid_sample_low), 'tip_amount': float(tip_sample_low)}

        best_plan_lr, cost_lr = optimizer_lr.get_optimal_plan(query1)
        best_plan_dt, cost_dt = optimizer_dt.get_optimal_plan(query1)

        print(f"\n==> Quyết định (LR): Dùng '{best_plan_lr}' (Chi phí: {cost_lr:,.3f} ms)")
        print(f"==> Quyết định (DT): Dùng '{best_plan_dt}' (Chi phí: {cost_dt:,.3f} ms)")

        # KỊCH BẢN 2: Truy vấn có vẻ không chọn lọc (tip cao)
        print("\n\n" + "="*50)
        print("KỊCH BẢN 2: Truy vấn có vẻ không chọn lọc")
        print("="*50)
        query2 = {'vendor_id': int(vid_sample_high), 'tip_amount': float(tip_sample_high)}

        best_plan_lr, cost_lr = optimizer_lr.get_optimal_plan(query2)
        best_plan_dt, cost_dt = optimizer_dt.get_optimal_plan(query2)

        print(f"\n==> Quyết định (LR): Dùng '{best_plan_lr}' (Chi phí: {cost_lr:,.3f} ms)")
        print(f"==> Quyết định (DT): Dùng '{best_plan_dt}' (Chi phí: {cost_dt:,.3f} ms)")
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
import xgboost as xgb
import psycopg2
import psycopg2.extras
import time
import random
import re
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


# --- PHẦN 1: TƯƠNG TÁC VỚI CSDL POSTGRESQL ---
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

def get_actual_join_cost(conn, query_params, plan_id):
    """
    Thực thi EXPLAIN ANALYZE cho một truy vấn JOIN,
    cố gắng "ép" một kế hoạch JOIN cụ thể.
    """

    # Template truy vấn JOIN mới
    sql_template = """
    SELECT V.vendor_name, COUNT(*)
    FROM Fact_Trips T
    JOIN Dim_Vendor V ON T.vendor_id_fk = V.vendor_id_pk
    WHERE T.tip_amount > %(tip_amount)s AND V.vendor_name = %(vendor_name)s
    GROUP BY V.vendor_name;
    """

    # Các lệnh SET để "ép" kế hoạch. Đây là phần phức tạp.
    # Chúng ta vô hiệu hóa các chiến lược KHÁC.
    plan_settings = {
        'plan_nested_loop': [
            "SET enable_hashjoin = off;",
            "SET enable_mergejoin = off;",
            "SET enable_nestloop = on;"
        ],
        'plan_hash_join': [
            "SET enable_hashjoin = on;",
            "SET enable_mergejoin = off;",
            "SET enable_nestloop = off;"
        ],
        'plan_merge_join': [
            "SET enable_hashjoin = off;",
            "SET enable_mergejoin = on;",
            "SET enable_nestloop = off;"
        ]
    }

    # Lệnh RESET luôn luôn chạy
    reset_settings = [
        "RESET enable_hashjoin;",
        "RESET enable_mergejoin;",
        "RESET enable_nestloop;"
    ]

    time_regex = re.compile(r"Execution Time: (\d+\.\d+) ms")
    cost_ms = None

    with conn.cursor() as cur:
        try:
            settings = plan_settings.get(plan_id)
            if not settings:
                raise ValueError(f"Kế hoạch không xác định: {plan_id}")

            # 1. Thiết lập các tham số để buộc kế hoạch
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
                # Kế hoạch có thể đã thất bại
                print(f"Cảnh báo: Không tìm thấy Execution Time cho {query_params}, {plan_id}")
                return None

        except psycopg2.Error as e:
            print(f"Lỗi khi thực thi truy vấn: {e}")
            conn.rollback()
            return None
        finally:
            # 5. LUÔN LUÔN RESET
            for setting in reset_settings:
                try:
                    cur.execute(setting)
                except psycopg2.Error:
                    pass # Bỏ qua lỗi nếu reset thất bại (ít quan trọng hơn)

    return cost_ms

def generate_historical_logs(conn, n_samples=1000):
    """
    Tạo log lịch sử bằng cách chạy EXPLAIN ANALYZE cho các truy vấn JOIN.
    """
    print(f"Đang tạo {n_samples} mẫu log JOIN lịch sử từ CSDL...")
    logs = []

    # Các kế hoạch JOIN chúng ta muốn so sánh
    possible_plans = ['plan_nested_loop', 'plan_hash_join', 'plan_merge_join']

    # Lấy dữ liệu mẫu từ CSDL để tạo truy vấn ngẫu nhiên
    with conn.cursor() as cur:
        # Lấy một vài vendor name có thật
        cur.execute("SELECT vendor_name FROM Dim_Vendor ORDER BY RANDOM() LIMIT 10")
        vendor_names = [row[0] for row in cur.fetchall()]

        # Lấy phạm vi tip_amount
        cur.execute("SELECT MIN(tip_amount), MAX(tip_amount) FROM Fact_Trips")
        min_tip, max_tip = cur.fetchone()

    if not vendor_names:
        print("Lỗi: Không tìm thấy dữ liệu trong Dim_Vendor.")
        return pd.DataFrame()

    print(f"Phạm vi dữ liệu: Tip ({min_tip}-{max_tip}), Vendors ({len(vendor_names)} mẫu)")

    for i in range(n_samples):
        # 1. Tạo một truy vấn ngẫu nhiên
        query_params = {
            'tip_amount': round(random.uniform(min_tip, max_tip / 10), 2),
            'vendor_name': random.choice(vendor_names)
        }

        # 2. Chọn một kế hoạch ngẫu nhiên để "thực thi"
        plan_id = random.choice(possible_plans)

        # 3. Lấy chi phí thực tế từ CSDL
        cost = get_actual_join_cost(conn, query_params, plan_id)

        if cost is not None:
            logs.append({
                # Thêm các đặc trưng số hóa
                'tip_amount': query_params['tip_amount'],
                # Biến đổi 'vendor_name' thành một đặc trưng số (ví dụ: độ hiếm)
                # Tạm thời chỉ dùng 1_0 để đơn giản hóa
                'vendor_selectivity': 1.0 / len(vendor_names),
                'plan_id': plan_id,
                'actual_cost': cost # Chi phí là mili-giây
            })

        if (i + 1) % 50 == 0:
            print(f"  Đã hoàn thành {i + 1}/{n_samples} mẫu...")

    print("Hoàn tất tạo log.")
    return pd.DataFrame(logs)

# --- PHẦN 2: HUẤN LUYỆN MÔ HÌNH ML (XGBoost) ---

def create_preprocessor():
    """Tạo pipeline tiền xử lý chung."""

    categorical_features = ['plan_id']
    # Các đặc trưng mới của chúng ta
    numerical_features = ['tip_amount', 'vendor_selectivity']

    # Pipeline cho đặc trưng SỐ
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler()) # XGBoost không cần nhưng cũng không hại
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

def train_cost_model(log_df):
    """
    Huấn luyện mô hình XGBoost.
    """
    print(f"\n--- Đang huấn luyện mô hình: XGBoost ---")

    features = ['tip_amount', 'vendor_selectivity', 'plan_id']
    target = 'actual_cost'

    X = log_df[features]
    y = log_df[target]

    preprocessor = create_preprocessor()

    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', xgb.XGBRegressor(
            objective='reg:squarederror',
            n_estimators=150,
            learning_rate=0.05,
            max_depth=5,
            random_state=42,
            n_jobs=-1
        ))
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
    Bộ tối ưu hóa truy vấn cho template JOIN.
    """
    def __init__(self, cost_model_pipeline, model_name="XGBoost"):
        self.model = cost_model_pipeline
        self.model_name = model_name
        # Các kế hoạch JOIN chúng ta đã huấn luyện
        self.candidate_plans = ['plan_nested_loop', 'plan_hash_join', 'plan_merge_join']

    def get_optimal_plan(self, query_params, all_vendors):
        """
        Dự đoán kế hoạch JOIN tối ưu cho một truy vấn mới.
        """
        print(f"\n--- Đang tìm kế hoạch JOIN (Sử dụng: {self.model_name}) cho: {query_params} ---")

        if not self.model:
            print("Lỗi: Bộ tối ưu hóa chưa được huấn luyện.")
            return None, 0

        best_plan = None
        min_predicted_cost = float('inf')
        plan_costs = {}

        # Tạo các đặc trưng số hóa từ query_params
        features_numeric = {
            'tip_amount': query_params['tip_amount'],
            # Tạm thời, chúng ta có thể dùng một giá trị trung bình
            'vendor_selectivity': 1.0 / len(all_vendors)
        }

        input_data_list = []
        for plan_id in self.candidate_plans:
            features = {
                'plan_id': plan_id,
                **features_numeric # Kết hợp dict
            }
            input_data_list.append(features)

        input_df = pd.DataFrame(input_data_list)

        # Dự đoán chi phí cho cả 3 kịch bản JOIN
        predicted_costs = self.model.predict(input_df)

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

# --- PHẦN 4: CHẠY MÔ PHỎNG ---

if __name__ == "__main__":

    conn = get_db_connection()
    if conn:
        # 1. Tạo dữ liệu lịch sử (CHỈ 1 LẦN)
        # Bắt đầu với số lượng mẫu nhỏ (ví dụ: 500) vì nó rất chậm.
        historical_logs_df = generate_historical_logs(conn, n_samples=5000)
        conn.close() # Đóng kết nối sau khi tạo log

        if historical_logs_df.empty or len(historical_logs_df) < 50:
            print("Không thể tạo/load log. Dừng chương trình.")
        else:
            log_filename = "pg_query_logs_JOIN_xgb.csv"
            historical_logs_df.to_csv(log_filename, index=False)
            print(f"Đã lưu log lịch sử vào '{log_filename}'")

            # (Nếu bạn đã có file log, hãy load nó ở đây)
            # historical_logs_df = pd.read_csv(log_filename)

            # 2. Huấn luyện mô hình
            ml_cost_model = train_cost_model(historical_logs_df)

            if ml_cost_model:
                # 3. Khởi tạo bộ tối ưu hóa
                optimizer = MLQueryOptimizer(ml_cost_model)

                # 4. Mô phỏng truy vấn mới
                # Chúng ta cần một danh sách vendors để truyền vào optimizer
                # (Lý tưởng là lấy lại từ CSDL, nhưng tạm thời dùng lại từ log)
                all_vendors_list = historical_logs_df['vendor_selectivity'].unique()

                # KỊCH BẢN 1: tip_amount thấp (có thể chọn lọc)
                print("\n\n" + "="*50)
                print("KỊCH BẢN 1: Truy vấn JOIN (tip thấp)")
                print("="*50)
                query1 = {
                    'tip_amount': historical_logs_df['tip_amount'].quantile(0.1),
                    'vendor_name': 'VTS' # Tên vendor giả định
                }

                best_plan_xgb, cost_xgb = optimizer.get_optimal_plan(query1, all_vendors_list)
                print(f"\n==> Quyết định (XGB): Dùng '{best_plan_xgb}' (Chi phí: {cost_xgb:,.3f} ms)")

                # KỊCH BẢN 2: tip_amount cao (ít chọn lọc)
                print("\n\n" + "="*50)
                print("KỊCH BẢN 2: Truy vấn JOIN (tip cao)")
                print("="*50)
                query2 = {
                    'tip_amount': historical_logs_df['tip_amount'].quantile(0.9),
                    'vendor_name': 'CMT' # Tên vendor giả định
                }

                best_plan_xgb, cost_xgb = optimizer.get_optimal_plan(query2, all_vendors_list)
                print(f"\n==> Quyết định (XGB): Dùng '{best_plan_xgb}' (Chi phí: {cost_xgb:,.3f} ms)")
    else:
        print("Không thể kết nối CSDL. Đã dừng.")
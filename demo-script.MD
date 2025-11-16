### BÁO CÁO THỰC NGHIỆM TỐI ƯU HÓA TRUY VẤN TRÊN CSDL BIG DATA THEO THỜI GIAN THỰC

---

### PHẦN 3: THỰC NGHIỆM KỸ THUẬT VIẾT LẠI TRUY VẤN (QUERY REWRITING)

#### 3.1. Kỹ thuật 1: Đẩy Vị từ (Predicate Pushdown)

##### 3.1.1. Mục tiêu Thực nghiệm

Thí nghiệm này nhằm kiểm chứng khả năng của CBO trong việc tự động "viết lại" truy vấn bằng cách đẩy các vị từ (điều kiện `WHERE`) vào các cấu trúc lồng nhau (ví dụ: `VIEW`), nhằm lọc dữ liệu tại bước sớm nhất có thể.

##### 3.1.2. Phương pháp Thực nghiệm

Một `VIEW` (`V_Trips_With_Payment_Name`) được tạo ra, thực hiện phép `JOIN` giữa bảng `Fact_Trips` (đang ở mức \~2.3 triệu hàng) và `Dim_Payment_Type`. Chúng ta thực hiện hai thí nghiệm: (1) Lọc `VIEW` bằng một vị từ thuộc bảng `Fact` và (2) Lọc `VIEW` bằng một vị từ thuộc bảng `Dim`.

##### 3.1.3. Kết quả và Phân tích (Ví dụ 1: Lọc trên Bảng Fact)

- **Truy vấn:**

```
CREATE OR REPLACE VIEW V_Trips_With_Payment_Name AS
SELECT
  T.trip_id_pk,
  T.total_amount,
  T.passenger_count,
  D.payment_type_name
FROM
  Fact_Trips AS T
JOIN
  Dim_Payment_Type AS D ON T.payment_type_fk = D.payment_type_pk;

```

```
-- Câu lệnh thực nghiệm
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM V_Trips_With_Payment_Name
WHERE
    total_amount > 100;
```

- **Kết quả:**
  ```sql
  "Finalize Aggregate  (cost=58839.94..58839.95 rows=1 width=8) (actual time=2930.239..2941.602 rows=1 loops=1)"
  "  ->  Hash Join  (cost=17.20..57835.29 rows=1773 width=0) (actual time=8.080..2895.971 rows=682 loops=3)"
  "        ->  Parallel Seq Scan on fact_trips t  (cost=0.00..57813.37 rows=1773 width=4) (actual time=6.579..2891.427 rows=682 loops=3)"
  "              Filter: (total_amount > '100'::double precision)"
  "              Rows Removed by Filter: 673318"
  ```
- **Phân tích:** Bằng chứng mấu chốt là dòng `Filter: (total_amount > '100'::double precision)` nằm ngay bên dưới `Parallel Seq Scan on fact_trips t`. Điều này xác nhận CBO đã **"đẩy" vị từ** vào bên trong `VIEW`, áp dụng nó tại nguồn. Thay vì `JOIN` \~1.85 triệu hàng, CBO đã lọc trước, loại bỏ 2.02 triệu hàng (`Rows Removed by Filter: 673318` \* 3 workers) và chỉ đưa **2.046 hàng** (`rows=682` \* 3 workers) vào `Hash Join`, giảm tải cho phép `JOIN` tới 99.89%.

##### 3.1.4. Kết quả và Phân tích (Ví dụ 2: Lọc trên Bảng Dim)

- **Truy vấn:**

```
-- Câu lệnh thực nghiệm
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM V_Trips_With_Payment_Name
WHERE
  payment_type_name = 'Credit card';
```

- **Kết quả:**
  ```sql
  "Finalize Aggregate  (cost=71611.97..71611.98 rows=1 width=8) (actual time=3350.113..3367.512 rows=1 loops=1)"
  "  ->  Hash Join  (cost=14.03..70595.72 rows=6413 width=0) (actual time=2.904..3255.158 rows=507068 loops=3)"
  "        ->  Parallel Seq Scan on fact_trips t  (...)"
  "        ->  Hash  (...)"
  "              ->  Seq Scan on dim_payment_type d  (...)"
  "                    Filter: ((payment_type_name)::text = 'Credit card'::text)"
  "                    Rows Removed by Filter: 5"
  ```
- **Phân tích:** Tương tự, CBO đã **"đẩy" vị từ** `payment_type_name = 'Credit card'` xuống áp dụng khi quét `dim_payment_type`, loại bỏ 5 hàng và chỉ giữ lại 1 hàng. Phép `Hash Join` sau đó đã trở thành một phép so khớp N-với-1 hiệu quả cao, thay vì N-với-6.

---

#### 3.2. Kỹ thuật 2: Chuyển đổi Truy vấn con (Subquery Unnesting)

##### 3.2.1. Mục tiêu Thực nghiệm

Kiểm chứng CBO "viết lại" một truy vấn `IN (SELECT...)` (Subquery) thành một phép `JOIN` hiệu quả.

##### 3.2.2. Phương pháp thực nghiệm

So sánh Kế hoạch Thực thi của hai truy vấn (A: Dùng Subquery, B: Dùng JOIN) có cùng mục tiêu nghiệp vụ: "Đếm các chuyến đi \> 50$ VÀ cước phí 'Standard rate'".

##### 3.2.3. Kết quả và phân tích (So sánh A và B)

- **Truy vấn A (Subquery):** `... WHERE total_amount > 50 AND rate_code_id_fk IN (SELECT ... 'Standard rate');`

```
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM Fact_Trips
WHERE
  total_amount > 50
  AND rate_code_id_fk IN (
      SELECT rate_code_id_pk
      FROM Dim_Rate_Code
      WHERE rate_code_name = 'Standard rate'
  );

```

      * **Kết quả (`Time: 3839.301 ms`):**
        ```sql
        ->  Hash Join  (Hash Cond: (fact_trips.rate_code_id_fk = dim_rate_code.rate_code_id_pk))
              ->  Parallel Seq Scan on fact_trips (Filter: (total_amount > '50'))
              ->  Hash -> Seq Scan on dim_rate_code (Filter: (... 'Standard rate'))
        ```

- **Truy vấn B (JOIN):** `... FROM Fact_Trips AS T JOIN Dim_Rate_Code AS D ... WHERE T.total_amount > 50 AND D.rate_code_name = 'Standard rate';`

```
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(T.*)
FROM Fact_Trips AS T
JOIN Dim_Rate_Code AS D
  ON T.rate_code_id_fk = D.rate_code_id_pk
WHERE
  T.total_amount > 50
  AND D.rate_code_name = 'Standard rate';

```

      * **Kết quả (`Time: 791.438 ms`):**
        ```sql
        ->  Hash Join  (Hash Cond: (t.rate_code_id_fk = d.rate_code_id_pk))
              ->  Parallel Seq Scan on fact_trips t (Filter: (total_amount > '50'))
              ->  Hash -> Seq Scan on dim_rate_code d (Filter: (... 'Standard rate'))
        ```

- **Phân tích:** Bằng chứng mấu chốt là **Kế hoạch Thực thi của cả hai truy vấn là giống hệt nhau về mặt cấu trúc**. CBO đã nhận diện Truy vấn A, "tháo" (unnest) truy vấn con và "viết lại" nó thành một `Hash Join`, chính xác như Kế hoạch Tối ưu B.
  - _Ghi chú học thuật:_ Sự chênh lệch lớn về thời gian (3.8s vs 0.8s) không phải do Kế hoạch Thực thi, mà do **I/O Caching** của Hệ điều hành. Lần chạy đầu tiên (A) chịu chi phí "đọc lạnh" (cold read) vật lý từ đĩa (`read=49951` blocks). Lần chạy thứ hai (B) được hưởng lợi từ "đọc ấm" (warm read) khi dữ liệu đã nằm trong bộ đệm (cache) của OS.

---

#### 3.3. Kỹ thuật 3: Sắp xếp lại thứ tự Nối (Join Reordering)

##### 3.3.1. Mục tiêu thực nghiệm

Kiểm chứng CBO phớt lờ thứ tự `JOIN` trong cú pháp (syntactic order) và "viết lại" theo thứ tự có chi phí (cost) thấp nhất.

##### 3.3.2. Phương pháp thực nghiệm

Thực thi một phép `JOIN` 3 bảng (`Dim_R JOIN Fact_T JOIN Dim_P`) với các bộ lọc trên cả hai bảng `Dim`.

ví dụ: "Đếm số chuyến đi và tổng tiền boa (tip) cho các chuyến đi được trả bằng 'Credit card' VÀ có cước phí 'Standard rate'."

##### 3.3.3. Kết quả và hân tích

- **Truy vấn:**

```
-- Câu lệnh thực nghiệm
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  COUNT(T.trip_id_pk),
  SUM(T.tip_amount)
FROM
  Dim_Rate_Code AS R      -- Bảng nhỏ 1 (Viết trước)
JOIN
  Fact_Trips AS T         -- Bảng LỚN (Viết ở giữa)
  ON R.rate_code_id_pk = T.rate_code_id_fk
JOIN
  Dim_Payment_Type AS P   -- Bảng nhỏ 2 (Viết cuối)
  ON T.payment_type_fk = P.payment_type_pk
WHERE
  R.rate_code_name = 'Standard rate'
  AND P.payment_type_name = 'Credit card';

```

- **Kết quả (`Time: 5562.975 ms`):**
  ```sql
  "  ->  Hash Join  (Hash Cond: (t.payment_type_fk = p.payment_type_pk))"
  "        ->  Hash Join  (Hash Cond: (t.rate_code_id_fk = r.rate_code_id_pk))"
  "              ->  Parallel Seq Scan on fact_trips t"
  "              ->  Hash -> Seq Scan on dim_rate_code r (Filter: ... 'Standard rate')"
  "        ->  Hash -> Seq Scan on dim_payment_type p (Filter: ... 'Credit card')"
  ```
- **Phân tích:** Kế hoạch thực thi xác nhận giả thuyết.
  1.  **Predicate Pushdown:** CBO đã đẩy các bộ lọc xuống `dim_rate_code` và `dim_payment_type` trước.
  2.  **Join Reordering:** Thứ tự cú pháp là `(R JOIN T) JOIN P`. Tuy nhiên, CBO đã "viết lại" và chọn thứ tự thực thi là `( (Fact_Trips T JOIN Dim_Rate_Code_Filtered R) JOIN Dim_Payment_Type_Filtered P )`. Nó đã phân tích chi phí và chọn một "Kế hoạch tối ưu" (Optimized Plan) khác hoàn toàn với cú pháp, khẳng định khả năng tự động "Sắp xếp lại thứ tự nối".

---

### PHẦN 4: THỰC NGHIỆM KỸ THUẬT ĐÁNH CHỈ MỤC (INDEXING)

#### 4.1. Giai đoạn 1: Hạn chế của kỹ thuật truyền thống (B-Tree)

##### 4.1.1. Mục tiêu và phương pháp

Phần thực nghiệm này nhằm mục đích phân tích và kiểm chứng các luận điểm của bài báo liên quan đến hiệu năng của các kỹ thuật đánh chỉ mục. Chúng ta sẽ tập trung vào các kỹ thuật truyền thống, cụ thể là "Tiếp cận dựa trên Heuristic" (Heuristic-based Approaches) và "Chỉ mục B-Tree" (B-tree Indexes), để làm rõ các hạn chế của chúng trong môi trường phân tích dữ liệu lớn (OLAP) thời gian thực.

Các thí nghiệm được thực thi trên bảng `Fact_Trips` (với ~3.3 triệu hàng) bằng công cụ `EXPLAIN (ANALYZE, BUFFERS)`.

##### 4.1.2. Kết quả và phân tích (Thí nghiệm 4.1: Độ chọn lọc thấp)

- **Truy vấn:**

```
-- Bước 1: Tạo chỉ mục B-Tree
CREATE INDEX idx_basic_payment ON Fact_Trips (payment_type_fk);
-- Bước 2: Chạy EXPLAIN (Đếm các chuyến KHÔNG phải 'Credit card')
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*) FROM Fact_Trips WHERE payment_type_fk != 1;

```

- **Kết quả (`Time: 455.207 ms`):**
  ```sql
  "  ->  Parallel Index Only Scan using idx_basic_payment on fact_trips (...)"
  "        Heap Fetches: 148726"
  "        Buffers: shared hit=7642 read=2853"
  ```
- **Phân tích:** Kế hoạch `Index Only Scan` đã thất bại. Con số `Heap Fetches: 148726` (trên mỗi worker) là bằng chứng của **\~446.000** lượt I/O ngẫu nhiên (random I/O), khi CSDL phải "nhảy" từ index về bảng chính (heap) để xác nhận tính hiển thị của hàng (do streaming). Điều này chứng minh B-Tree truyền thống không hiệu quả cho các truy vấn có độ chọn lọc thấp.

##### 4.1.3. Kết quả và phân tích (Thí nghiệm 4.2: Truy vấn đa chiều)

- **Truy vấn:**

```
-- Bước 1: Dọn dẹp
DROP INDEX IF EXISTS idx_basic_payment;
-- (Giả định các index GIST/Expr khác đã được dọn dẹp)

-- Bước 2: Chạy EXPLAIN (Truy vấn đa chiều)
EXPLAIN (ANALYZE, BUFFERS)
SELECT SUM(tip_amount), COUNT(*) FROM Fact_Trips
WHERE
  EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 8 AND 10
  AND pickup_latitude BETWEEN 40.748 AND 40.758
  AND pickup_longitude BETWEEN -73.980 AND -73.990
  AND passenger_count > 2;

```

- **Kết quả (`Time: 1606.649 ms`):**
  ```sql
  "  ->  Parallel Seq Scan on fact_trips  (...)"
  "        Filter: ((pickup_latitude >= '40.748'...) AND ...)"
  "        Rows Removed by Filter: 1106401"
  "        Buffers: shared hit=11905 read=65793"
  ```
- **Phân tích:** Đây là bằng chứng cho sự không hiệu quả của B-Tree. CSDL không có lựa chọn nào khác ngoài `Parallel Seq Scan`, quét toàn bộ **\~3.3 triệu hàng** (`Rows Removed by Filter: 1106401` \* 3 workers). Kế hoạch này gây ra chi phí I/O vật lý khổng lồ (`read=65,793` blocks) và hoàn toàn **không có khả năng mở rộng (not scalable)**.

#### 4.2. Giai đoạn 2: Hiệu quả của "Advanced Indexing"

##### 4.2.1. Mục tiêu Thực nghiệm

Áp dụng các "Advanced Indexing" (Spatial, Bitmap, Combined) để giải quyết hạn chế (pain point) I/O của Thí nghiệm 4.2.

##### 4.2.2. Phương pháp thực nghiệm (Thí nghiệm 4.3)

Tạo các chỉ mục chuyên dụng: (1) `idx_gist_location` (Chỉ mục GiST cho **Spatial Indexing**), (2) `idx_expr_hour` (Chỉ mục Biểu thức), (3) `idx_pass_count` (B-Tree). Đây là một **Kỹ thuật Kết hợp (Combined Technique)**.

##### 4.2.3. Kết quả và Phân tích (Thí nghiệm 4.3)

- **Truy vấn:** (Giống hệt Thí nghiệm 4.2).

```
-- Bước 1: Cài đặt extension cần thiết cho Chỉ mục Không gian
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Bước 2: Tạo Chỉ mục Không gian (Spatial Indexing) bằng GIST
-- Kỹ thuật này xử lý 2 cột latitude/longitude
CREATE INDEX idx_gist_location ON Fact_Trips USING GIST (
  pickup_latitude,
  pickup_longitude
);

-- Bước 3: Tạo Chỉ mục Biểu thức (Expression Index - B-Tree nâng cao)
-- Kỹ thuật này xử lý hàm EXTRACT(HOUR...)
CREATE INDEX idx_expr_hour ON Fact_Trips (
  (EXTRACT(HOUR FROM tpep_pickup_datetime))
);

-- Bước 4: Tạo B-Tree cơ bản cho cột còn lại
CREATE INDEX idx_pass_count ON Fact_Trips (passenger_count);

-- Bước 5: Chạy lại chính xác truy vấn "thất bại" của Thí nghiệm 4.2
EXPLAIN (ANALYZE, BUFFERS)
SELECT
  SUM(tip_amount),
  COUNT(*)
FROM
  Fact_Trips
WHERE
  EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 8 AND 10
  AND pickup_latitude BETWEEN 40.748 AND 40.758
  AND pickup_longitude BETWEEN -73.980 AND -73.990
  AND passenger_count > 2;
```

- **Kết quả (`Time: 437.901 ms`):**
  ```sql
  "  ->  Bitmap Heap Scan on fact_trips  (...)"
  "        Buffers: shared hit=2429 read=3136 written=2780"
  "        ->  BitmapAnd  (...)"
  "              ->  Bitmap Index Scan on idx_gist_location  (...)"
  "                    Buffers: shared hit=2429 read=3136 written=2780"
  "              ->  Bitmap Index Scan on idx_expr_hour  (...)"
  "                    (never executed)"
  ```
- **Phân tích:** Kế hoạch thực thi đã thay đổi hoàn toàn, xác nhận giả thuyết của chúng ta:
  1.  CBO đã từ bỏ `Parallel Seq Scan` và chọn `Bitmap Heap Scan`.
  2.  Nó đã áp dụng **Spatial Indexing** (`idx_gist_location`) và **Bitmap Indexing** (`BitmapAnd`).
  3.  Tối ưu hóa "Chập mạch" (Short-circuiting): CSDL đã quét `idx_gist_location` trước, thấy có 0 hàng khớp, và đã `(never executed)` (không cần chạy) `idx_expr_hour`, tiết kiệm I/O.
  4.  **So sánh định lượng (4.2 vs 4.3):**
      - **I/O Đọc vật lý:** Giảm từ **65.793** blocks xuống **3.136** blocks (Giảm 95.2%).
      - **Tổng Thời gian:** Giảm từ **1606.649 ms** xuống **437.901 ms** (Nhanh hơn 3.67 lần).

##### 4.2.4. Kết luận (Phần 4)

Thí nghiệm đã chứng minh B-Tree truyền thống thất bại với các truy vấn OLAP đa chiều, dẫn đến kế hoạch `Parallel Seq Scan` không có khả năng mở rộng. Việc áp dụng các Kỹ thuật Nâng cao (Spatial, Bitmap, Combined) đã giải quyết triệt để vấn đề, **giảm 95.2% I/O đĩa** và **tăng tốc 3.67 lần**.

---

### PHẦN 5: THỰC NGHIỆM KẾ HOẠCH THỰC THI ĐỘNG (ADAPTIVE PLANNING)

#### 5.1. Mục tiêu thực nghiệm

Kiểm chứng CBO có khả năng "thích ứng động" (dynamically adapt), chọn các "Kế hoạch tối ưu" (optimized plan) khác nhau cho cùng một "Câu lệnh Chuẩn bị" (Prepared Statement) dựa trên tham số đầu vào.

#### 5.2. Phương pháp thực nghiệm

Chuẩn bị một câu lệnh `dynamic_plan_test` (`...WHERE total_amount > $1`). Thực thi hai lần: (5.1) với Độ chọn lọc cao (`$1 = 500`) và (5.2) với Độ chọn lọc thấp (`$1 = 1`).

#### 5.3. Kết quả và phân tích (So sánh 5.1 và 5.2)

**Chuẩn bị**

- `CREATE INDEX idx_total_amount ON Fact_Trips (total_amount);`
- `PREPARE dynamic_plan_test (double precision) AS SELECT COUNT(*) FROM Fact_Trips WHERE total_amount > $1; `

- **Thí nghiệm 5.1 (CAO): `EXECUTE ... (500)`**
  Ví dụ: "Đếm các chuyến đi có giá trị rất cao (lớn hơn 500$)"

  - **Truy vấn**

    ```
    EXPLAIN (ANALYZE, BUFFERS)
    EXECUTE dynamic_plan_test(500);
    ```

  - **Kết quả (`Time: 1.154 ms`):**
    ```sql
    "  ->  Index Only Scan using idx_total_amount on fact_trips  (...)"
    "        Index Cond: (total_amount > '500'::double precision)"
    "        Buffers: shared hit=31 read=7"
    ```
    - **Phân tích:** CBO nhận diện Độ chọn lọc cao (chỉ 30 hàng), chọn `Index Only Scan` (ưu tiên độ trễ), I/O gần như bằng không.

- **Thí nghiệm 5.2 (THẤP): `EXECUTE ... (1)`** Ví dụ: "Đếm các chuyến đi có giá trị thông thường (lớn hơn 1$)"

  - **Truy vấn**

    ```
    EXPLAIN (ANALYZE, BUFFERS)
    EXECUTE dynamic_plan_test(1);

    ```

  - **Kết quả (`Time: 2853.507 ms`):**
    ```sql
    "  ->  Parallel Seq Scan on fact_trips  (...)"
    "        Filter: (total_amount > '1'::double precision)"
    "        Buffers: shared hit=1540 read=87047"
    ```
  - **Phân tích:** CBO nhận diện Độ chọn lọc thấp (\~3.78 triệu hàng), chọn `Parallel Seq Scan` (ưu tiên thông lượng), đọc **87.047** khối I/O vật lý.

#### 5.4. Kết luận (Phần 5)

Bảng so sánh Kế hoạch Thực thi cho _cùng một câu lệnh_ đã chứng minh rõ ràng khả năng Lập kế hoạch Thích ứng:

| Tham số (`$1`) | Độ chọn lọc             | Kế hoạch CBO đã chọn | Thời gian       | I/O Đọc vật lý    |
| :------------- | :---------------------- | :------------------- | :-------------- | :---------------- |
| `500`          | **CAO** (30 hàng)       | `Index Only Scan`    | **1.154 ms**    | **7** blocks      |
| `1`            | **THẤP** (\~3.78M hàng) | `Parallel Seq Scan`  | **2853.507 ms** | **87,047** blocks |

Kết quả này xác nhận CSDL đã **thích ứng động** chiến lược của mình dựa trên tham số, một hành vi "thông minh" cốt lõi để duy trì hiệu suất trong môi trường workload biến đổi.

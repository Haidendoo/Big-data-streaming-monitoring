# 🧪 Kịch bản kiểm thử toàn diện (End-to-End Test Plan)

Tài liệu này cung cấp kịch bản kiểm thử từng bước để xác minh tính thông suốt của toàn bộ luồng dữ liệu (Fact Ingestion, Dimension CDC, Spark Batch Processing, Iceberg Compaction và Visualization).

---

## 📋 Chuẩn bị trước khi Test
Đảm bảo bạn đã khởi động và khôi phục toàn bộ hạ tầng bằng cách chạy lệnh trên máy host:
```bash
./infrastructure/scripts/setup-pipeline.sh
```
*Đợi lệnh trên chạy xong và in ra trạng thái các cổng kết nối đều hiển thị `✅ OK`.*

---

## 🛠️ Bước 1: Kiểm thử luồng CDC (PostgreSQL -> Iceberg)
Luồng này kiểm tra khả năng bắt sự kiện thay đổi dữ liệu cấu hình máy chủ từ PostgreSQL để đồng bộ sang bảng Dimension của Iceberg.

1. **Kiểm tra dữ liệu cấu hình máy chủ hiện tại trên Trino**:
   ```bash
   kubectl exec -n lakehouse deployment/trino-coordinator -- trino --server http://127.0.0.1:8888 --execute "SELECT * FROM iceberg.monitoring.server_config"
   ```

2. **Chèn một dòng cấu hình máy chủ mới vào PostgreSQL**:
   ```bash
   kubectl exec -n lakehouse dim-data-postgresql-0 -- psql -U dim_user -d dim_data -c "
   INSERT INTO server_config (server_id, server_name, ip, province, station) 
   VALUES (106, 'test-cdc-01', '192.168.1.99', 'Hue', 'Tram Huong Giang')
   ON CONFLICT (server_id) DO UPDATE SET station = EXCLUDED.station;
   "
   ```

3. **Xác minh dữ liệu tự động đồng bộ sang Iceberg**:
   *Đợi khoảng 60 giây (chu kỳ checkpoint của Flink) rồi chạy:*
   ```bash
   kubectl exec -n lakehouse deployment/trino-coordinator -- trino --server http://127.0.0.1:8888 --execute "
   SELECT * FROM iceberg.monitoring.server_config WHERE server_id = 106;
   "
   ```
   *Kết quả mong đợi: Dòng dữ liệu server_id = 106 hiển thị đầy đủ trên Trino.*

---

## 📥 Bước 2: Kiểm thử luồng Ingestion (SFTP -> NiFi -> Kafka -> Flink -> Iceberg)
Luồng này xác minh dữ liệu hiệu năng máy chủ (CSV/XML) được tải lên SFTP sẽ được đưa vào Data Lakehouse theo thời gian thực.

1. **Chạy script giả lập dữ liệu mẫu**:
   ```bash
   python3 infrastructure/scripts/generate-mock-data.py
   ```
   *Script sẽ tạo file ngẫu nhiên (CSV/XML), đẩy lên SFTP.*

2. **Xác minh file đã được NiFi quét và đẩy lên MinIO Landing**:
   ```bash
   kubectl exec -n lakehouse minio-0 -- mc ls -r local/lakehouse/raw-file/
   ```
   *Kết quả mong đợi: File thô mới xuất hiện trong thư mục `2026/06/28/...` trên MinIO.*

3. **Xác minh Flink đã parse dữ liệu và ghi vào bảng Iceberg**:
   *Đợi khoảng 60 giây (chu kỳ checkpoint Flink) để commit dữ liệu, sau đó truy vấn:*
   ```bash
   kubectl exec -n lakehouse deployment/trino-coordinator -- trino --server http://127.0.0.1:8888 --execute "
   SELECT * FROM iceberg.monitoring.raw_sftp_table ORDER BY ts DESC LIMIT 5;
   "
   ```

---

## ⚡ Bước 3: Kiểm thử luồng Spark Batch (Airflow -> Spark -> Iceberg KPI Summary)
Luồng này thực hiện tính toán chỉ số KPI trung bình hàng giờ bằng cách join bảng Fact (`raw_sftp_table`) với bảng Dimension (`server_config`).

1. **Đảm bảo dữ liệu giả lập có mốc thời gian trong vòng 1 giờ qua**:
   Chạy script mock data thêm 2 lần để chắc chắn có dữ liệu mới nhất:
   ```bash
   python3 infrastructure/scripts/generate-mock-data.py
   python3 infrastructure/scripts/generate-mock-data.py
   ```
   *Đợi 60 giây để Flink ghi nhận dữ liệu mới.*

2. **Trigger chạy Spark Batch Job thông qua Airflow CLI**:
   ```bash
   kubectl exec -n orchestration deployment/airflow -c airflow-scheduler -- airflow dags trigger server_monitoring_hourly_kpi
   ```

3. **Theo dõi tiến trình chạy Spark Job**:
   ```bash
   # Lấy danh sách pod để tìm pod driver mới
   kubectl get pods -n orchestration | grep aggregate-kpi-summary
   # Xem logs (thay thế tên pod thực tế của bạn)
   kubectl logs -n orchestration -l spark-role=driver --tail=50
   ```

4. **Xác minh dữ liệu KPI tổng hợp trong Trino**:
   *Sau khi pod Spark Driver chạy xong (chuyển sang trạng thái Completed và biến mất), chạy:*
   ```bash
   kubectl exec -n lakehouse deployment/trino-coordinator -- trino --server http://127.0.0.1:8888 --execute "
   SELECT * FROM iceberg.monitoring.kpi_summary;
   "
   ```
   *Kết quả mong đợi: Xuất hiện các bản ghi KPI tổng hợp trung bình CPU, RAM, Disk theo Province và Station.*

---

## 📦 Bước 4: Kiểm thử luồng Iceberg Compaction (Gộp file nhỏ)
Tránh phân mảnh lưu trữ do Flink tạo ra nhiều file Parquet nhỏ sau mỗi phút checkpoint.

1. **Kiểm tra số lượng files hiện tại của bảng raw**:
   ```bash
   kubectl exec -n lakehouse deployment/trino-coordinator -- trino --server http://127.0.0.1:8888 --execute "
   SELECT count(distinct file_path) FROM \"iceberg\".\"monitoring\".\"raw_sftp_table\$files\";
   "
   ```

2. **Khởi chạy lệnh Compaction trực tiếp trên Trino** (Optimize bảng):
   ```bash
   kubectl exec -n lakehouse deployment/trino-coordinator -- trino --server http://127.0.0.1:8888 --execute "
   ALTER TABLE iceberg.monitoring.raw_sftp_table EXECUTE optimize;
   "
   ```

3. **Xác minh Snapshot Compaction**:
   Kiểm tra danh sách lịch sử commit để thấy snapshot `replace` (chứng tỏ đã gộp thành công):
   ```bash
   kubectl exec -n lakehouse deployment/trino-coordinator -- trino --server http://127.0.0.1:8888 --execute "
   SELECT snapshot_id, operation, summary 
   FROM \"iceberg\".\"monitoring\".\"raw_sftp_table\$snapshots\" 
   ORDER BY committed_at DESC LIMIT 3;
   "
   ```
   *Kết quả mong đợi: Snapshot mới nhất có operation = `replace`.*

---

## 📊 Bước 5: Kiểm tra giao diện Visualization
Đảm bảo dashboard vẽ dữ liệu chính xác:
* **Grafana (Realtime Dashboard)**: Truy cập [http://localhost:3000](http://localhost:3000) $\rightarrow$ Xem các biểu đồ CPU/RAM của server 106 mới thêm.
* **Metabase (BI Dashboard)**: Truy cập [http://localhost:3001](http://localhost:3001) $\rightarrow$ Truy vấn bảng `kpi_summary` vẽ biểu đồ phân bố tải hiệu năng theo tỉnh thành (`province`).

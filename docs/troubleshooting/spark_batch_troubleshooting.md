# Hướng dẫn xử lý sự cố Spark Batch trên Kubernetes & Airflow 3 (Troubleshooting Guide)

Tài liệu này ghi nhận các sự cố thường gặp, nguyên nhân gốc rễ và giải pháp khắc phục khi vận hành luồng xử lý dữ liệu Batch sử dụng **Apache Airflow 3**, **Spark Standalone (1 Master + 3 Workers)** trên cụm **Kubernetes (k3d/k3s)**.

---

## 1. Lỗi Classpath Mismatch (`InvalidClassException` trên `org.apache.iceberg.BaseFile`)

### Triệu chứng (Symptoms)
Trong quá trình Spark Executor nhận task từ Spark Driver, logs ghi nhận lỗi:
```text
java.io.InvalidClassException: org.apache.iceberg.BaseFile; local class incompatible: stream classdesc serialVersionUID = -xxxxxxxxxxxxxxxxx, local class serialVersionUID = -yyyyyyyyyyyyyyyyy
```

### Nguyên nhân gốc rễ (Root Cause)
Do định nghĩa lớp (class definition) của Iceberg tại Driver và Executor bị lệch phiên bản. Sự lệch này xảy ra khi cấu hình Spark Submit truyền đồng thời hai thư viện:
1. Bản unshaded `org.apache.iceberg:iceberg-hive-metastore:1.5.2` mang theo các dependencies thô của Iceberg.
2. Bản shaded `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2` chứa các phiên bản class đã được shaded/renamed.
Sự trùng lặp này dẫn đến việc nạp chồng Classpath khác nhau giữa Driver (chạy client mode trong KubernetesPodOperator) và Executor (chạy trên Spark Standalone Workers).

### Cách khắc phục (Resolution)
Loại bỏ hoàn toàn thư viện `iceberg-hive-metastore` ra khỏi tham số `--packages` của Spark. Chỉ sử dụng duy nhất bản shaded runtime:
* **File cấu hình:** [dags/server_monitoring_batch.py](file:///home/haiden/bku/vdt/server-monitoring-lakehouse/dags/server_monitoring_batch.py)
* **Cấu hình chuẩn:**
  ```python
  DEFAULT_PACKAGES = (
      "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
      "org.apache.hadoop:hadoop-aws:3.3.4,"
      "com.amazonaws:aws-java-sdk-bundle:1.12.262"
  )
  ```

---

## 2. Lỗi Phân giải Tên miền ngược (`UnknownHostException` trên Driver Host)

### Triệu chứng (Symptoms)
Spark Executors khởi chạy thành công trên Workers nhưng không thể kết nối ngược lại Spark Driver (chạy trong pod Airflow KubernetesPodOperator) để nhận task:
```text
java.net.UnknownHostException: aggregate-kpi-summary-xxxxxxxx-driver-pod-name
```

### Nguyên nhân gốc rễ (Root Cause)
Mặc định, Spark Driver lấy tên hostname của Pod làm địa chỉ kết nối (`spark.driver.host`). Tuy nhiên, Spark Workers (chạy ở namespace `orchestration` trên các nodes khác) không thể phân giải được tên hostname này qua DNS của CoreDNS do pod Driver không có bản ghi A record tương ứng.

### Cách khắc phục (Resolution)
Cấu hình cho Spark Driver sử dụng trực tiếp địa chỉ IP của Pod thay vì hostname bằng cách nạp biến môi trường qua **Downward API** của Kubernetes:
1. Trong file DAG định nghĩa Pod Operator, lấy IP của Pod gán vào biến môi trường `SPARK_LOCAL_IP`:
   ```python
   env_vars=[
       k8s.V1EnvVar(
           name="SPARK_LOCAL_IP",
           value_from=k8s.V1EnvVarSource(
               field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP")
           ),
       ),
   ]
   ```
2. Thêm tham số cấu hình Spark Driver Host sử dụng biến này:
   ```python
   SPARK_CONF = [
       "--conf", "spark.driver.host=$(SPARK_LOCAL_IP)",
       # ... các cấu hình khác
   ]
   ```

---

## 3. Lỗi Tràn bộ nhớ Pod Driver (`OOMKilled - Exit Code 137`)

### Triệu chứng (Symptoms)
Tiến trình chạy Spark Batch đột ngột dừng, pod Airflow KubernetesPodOperator biến mất hoặc hiển thị trạng thái `Failed` với lỗi `Exit Code 137`.

### Nguyên nhân gốc rễ (Root Cause)
Do dung lượng bộ nhớ RAM được cấp cho Pod Driver trong k8s quá nhỏ (`limits.memory: 512Mi`), không đủ đáp ứng dung lượng yêu cầu tối thiểu của Spark JVM Driver (vốn đã chiếm `512m` cho Heap Memory, chưa tính Memory Overhead và Metaspace).

### Cách khắc phục (Resolution)
Nâng giới hạn tài nguyên bộ nhớ (Memory limits) của Container của Pod Operator trong DAG lên tối thiểu `1Gi` (1024Mi):
* **Cấu hình chuẩn:**
  ```python
  container_resources=k8s.V1ResourceRequirements(
      requests={"cpu": "100m", "memory": "512Mi"},
      limits={"cpu": "500m", "memory": "1024Mi"},
  )
  ```

---

## 4. Lỗi Cạn kiệt Inotify Watchers (`failed to create fsnotify watcher: too many open files`)

### Triệu chứng (Symptoms)
Logs của Airflow liên tục in ra các cảnh báo lặp đi lặp lại mỗi giây:
```text
failed to create fsnotify watcher: too many open files: source="airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
WARNING - Pod aggregate-kpi-summary-xxxx log read interrupted but container base still running. Logs generated in the last one second might get duplicated.
```

### Nguyên nhân gốc rễ (Root Cause)
Cơ chế log streaming của `KubernetesPodOperator` sử dụng thư viện `fsnotify` để lắng nghe file log. Hệ thống máy chủ WSL2/Linux Host mặc định giới hạn số lượng inotify instance (`fs.inotify.max_user_instances`) ở mức rất thấp (thường là `128`). Khi chạy K8s nội bộ với nhiều pod, giới hạn này bị cạn kiệt khiến Airflow không thể mở thêm watcher.
*(Lưu ý: Lỗi này chỉ ảnh hưởng đến việc hiển thị log trực tiếp trên Airflow Web UI, Spark Job bên dưới vẫn hoàn thành bình thường).*

### Cách khắc phục (Resolution)
Tăng giới hạn inotify trên máy chủ Host (WSL2 hoặc máy Linux của bạn):
1. **Áp dụng tạm thời:**
   ```bash
   sudo sysctl -w fs.inotify.max_user_instances=8192
   sudo sysctl -w fs.inotify.max_user_watches=524288
   ```
2. **Áp dụng vĩnh viễn (sau khi khởi động lại máy):**
   Thêm các dòng sau vào cuối file `/etc/sysctl.conf`:
   ```text
   fs.inotify.max_user_instances = 8192
   fs.inotify.max_user_watches = 524288
   ```
   Sau đó lưu file và chạy lệnh:
   ```bash
   sudo sysctl -p
   ```

---

## 5. Lỗi Treo tài nguyên Spark Standalone (`Initial job has not accepted any resources`)

### Triệu chứng (Symptoms)
Job Spark mới khởi tạo hiển thị cảnh báo liên tục:
```text
WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
```

### Nguyên nhân gốc rễ (Root Cause)
Do có một Pod Spark Driver cũ bị Airflow bỏ rơi hoặc bị treo (do chạy lỗi hoặc mất kết nối) nhưng chưa bị xóa. Driver này tiếp tục giữ kết nối tới Spark Master và chiếm dụng toàn bộ tài nguyên (ví dụ: `total-executor-cores: 3`) của 3 Spark Workers. Do đó, các Job mới chạy sau không thể xin được tài nguyên và bị xếp hàng chờ vô hạn.

### Cách khắc phục (Resolution)
1. Kiểm tra danh sách Pods trong namespace `orchestration`:
   ```bash
   kubectl get pods -n orchestration
   ```
2. Tìm pod Driver bị treo (có tuổi đời lâu nhưng vẫn ở trạng thái `Running`) và tiến hành xóa thủ công:
   ```bash
   kubectl delete pod <tên-pod-treo> -n orchestration
   ```
   Sau khi xóa, tài nguyên sẽ ngay lập tức được giải phóng để phục vụ các tác vụ mới.

---

## 6. Lỗi Lệch Metadata Bảng Iceberg (`NoSuchKey` / `NotFoundException` trên S3)

### Triệu chứng (Symptoms)
Các query engine (Trino, Spark) khi truy vấn bảng Iceberg gặp lỗi:
```text
File s3a://lakehouse/lakehouse/monitoring.db/raw_sftp_table-xxxxxxxx/data/... not found: The specified key does not exist. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey)
```
Hoặc khi kiểm tra MinIO, bạn thấy các file metadata `.json` nằm ở một thư mục bảng có đuôi UUID này, nhưng dữ liệu thực tế `.parquet` lại nằm ở thư mục bảng có đuôi UUID khác.

### Nguyên nhân gốc rễ (Root Cause)
Do bảng bị `DROP` và `CREATE` lại (tạo ra thư mục bảng có UUID mới trong Hive Metastore). Tuy nhiên, Flink Ingestion/CDC job cũ vẫn đang chạy ngầm liên tục và giữ cache tham chiếu đến đường dẫn UUID cũ. Kết quả là Flink tiếp tục ghi file data vào thư mục cũ, nhưng cố commit metadata vào thư mục mới, gây ra sự phân mảnh/lệch đường dẫn.

### Cách khắc phục (Resolution)
1. **Hủy Job cũ**: Thực hiện Cancel Flink Ingestion Job bị lỗi trên Flink UI hoặc sử dụng CLI.
2. **Dọn dẹp MinIO**: Xóa toàn bộ các thư mục bảng chứa hậu tố UUID cũ/rác trên MinIO để tránh rác dữ liệu.
3. **Submit Job mới**: Submit lại Flink job từ đầu. Lúc này Flink sẽ truy vấn Hive Metastore để lấy đường dẫn bảng mới nhất và ghi đồng bộ cả data và metadata vào chung 1 thư mục bảng chính xác.

---

## 7. Lỗi Bảng Iceberg bị Treo Khóa trên Hive Metastore (`MetastoreLock$WaitingForLockException`)

### Triệu chứng (Symptoms)
Flink CDC hoặc Flink Ingestion liên tục cảnh báo hoặc bị crash-loop với lỗi:
```text
org.apache.iceberg.hive.MetastoreLock$WaitingForLockException: Waiting for lock on table monitoring.server_config
```

### Nguyên nhân gốc rễ (Root Cause)
Khi một Flink job hoặc Spark job đang ghi dữ liệu xuống bảng Iceberg thì bị crash hoặc ngắt đột ngột (ví dụ khi K3d cluster restart). Điều này làm tồn tại các lock "mồ côi" (stale locks) chưa được giải phóng trong database `metastore` của Hive Metastore. Các tiến trình chạy sau sẽ bị kẹt khi cố xin quyền lock ghi bảng.

### Cách khắc phục (Resolution)
Truy cập trực tiếp vào cơ sở dữ liệu PostgreSQL lưu trữ Hive Metastore (`hive-db-postgresql`) và xóa toàn bộ dữ liệu trong bảng `hive_locks`:
```bash
kubectl exec -n lakehouse hive-db-postgresql-0 -- env PGPASSWORD="password123" psql -U hive -d metastore -c "DELETE FROM hive_locks;"
```
Sau khi xóa, các job đang chờ sẽ lập tức được ghi nhận và hoạt động bình thường.

---

## 8. Bảng Dimension Rỗng Hoặc Spark Inner Join Ra 0 Dòng (Cold Start CDC)

### Triệu chứng (Symptoms)
Trigger DAG Airflow báo `success` nhưng không thấy sinh ra file dữ liệu KPI mới nào trong MinIO (`kpi_summary`), bảng tổng hợp rỗng.

### Nguyên nhân gốc rễ (Root Cause)
Do Flink CDC chỉ đồng bộ các sự kiện thay đổi dữ liệu (DML - INSERT/UPDATE/DELETE) kể từ khi nó khởi động. Nếu bảng PostgreSQL nguồn đã được nạp dữ liệu từ trước khi Flink CDC chạy lần đầu tiên, Flink CDC sẽ không đồng bộ dữ liệu cũ này sang Iceberg. Điều này dẫn đến bảng `server_config` trong Iceberg bị rỗng, phép inner join của Spark Job bị rỗng (0 kết quả) nên không có dữ liệu KPI nào được ghi.

### Cách khắc phục (Resolution)
Chạy một câu lệnh cập nhật giả lập trên bảng PostgreSQL để kích hoạt lại các sự kiện CDC, giúp Flink CDC nhận diện và đồng bộ toàn bộ dữ liệu sang Iceberg:
```bash
kubectl exec -n lakehouse dim-data-postgresql-0 -- psql -U dim_user -d dim_data -c "UPDATE server_config SET station = station;"
```
 Dữ liệu sẽ lập tức được đồng bộ sang Iceberg sau tối đa 60 giây (chu kỳ checkpoint Flink).

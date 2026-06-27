# Hệ thống Giám sát Server Lakehouse (Server Monitoring Lakehouse Platform)

Hệ thống giám sát máy chủ tập trung thời gian thực (100+ servers) sử dụng kiến trúc **Lakehouse** hiện đại để tối ưu hóa lưu trữ và truy vấn hiệu năng cao. Luồng dữ liệu đi qua các thành phần: **SFTP -> Apache NiFi -> MinIO (Lakehouse Bucket) & Apache Kafka -> Apache Flink -> Apache Iceberg -> Trino Query Engine -> Grafana Dashboard**. Luồng batch dùng **Apache Airflow 3 -> Apache Spark Distributed (1 Master, 3 Workers) -> Iceberg Gold -> Metabase**.

Hạ tầng được triển khai hoàn toàn trên cluster **k3d/k3s (Kubernetes lightweight)** cấu hình đa-node giả lập môi trường Production thực tế.

---

## 🗺️ Sơ đồ Kiến trúc & Luồng dữ liệu (Data Flow)

```mermaid
graph TD
    subgraph "External Sources (SFTP)"
        SFTP[SFTP Server /home/sftpuser/upload]
        Files[CSV/XML Metrics Files]
    end

    subgraph "Ingestion Layer (Apache NiFi - ingestion namespace)"
        NF_Fetch[NiFi: ListSFTP / FetchSFTP]
        NF_Minio[NiFi: PutS3Object]
        NF_Kafka[NiFi: PublishKafka]
    end

    subgraph "Messaging Layer (Apache Kafka - streaming namespace)"
        Kafka[Kafka Topic: file-arrival-events]
    end

    subgraph "Speed Layer (Apache Flink - streaming namespace)"
        FL_Cons[Flink: Kafka Consumer]
        FL_Read[Flink: S3 Reader]
        FL_Parse[Flink: XML/CSV Parser]
        FL_Sink[Flink: Iceberg Connector]
    end

    subgraph "Batch Layer (orchestration namespace)"
        Airflow[Apache Airflow 3]
        Spark[Spark Standalone 1 Master + 3 Workers]
    end

    subgraph "Storage & Metadata Layer (Lakehouse - lakehouse namespace)"
        Minio[(MinIO S3 lakehouse bucket)]
        HMS[Hive Metastore]
        Raw[Iceberg Raw Fact]
        Dim[Iceberg Dim Config]
        Gold[Iceberg Gold KPI]
    end

    subgraph "Serving & Monitoring Layer"
        Trino[Trino SQL Engine]
        Prometheus[Prometheus Server]
        Grafana[Grafana Dashboard]
        Metabase[Metabase BI]
    end

    %% Data Flow
    SFTP --> Files
    Files --> NF_Fetch
    NF_Fetch --> NF_Minio
    NF_Minio --> Minio
    NF_Minio --> NF_Kafka
    NF_Kafka --> Kafka
    Kafka --> FL_Cons
    FL_Cons --> FL_Read
    FL_Read <--> Minio
    FL_Read --> FL_Parse
    FL_Parse --> FL_Sink
    FL_Sink --> Raw
    Airflow --> Spark
    Raw --> Spark
    Dim --> Spark
    Spark --> Gold
    Raw --> Minio
    Dim --> Minio
    Gold --> Minio
    Raw <--> HMS
    Dim <--> HMS
    Gold <--> HMS
    Trino <--> HMS
    Trino <--> Minio
    Trino --> Grafana
    Trino --> Metabase
    Prometheus --> Grafana
```

---

## 🛠️ Yêu cầu chuẩn bị (Prerequisites)

Trước khi bắt đầu, hãy đảm bảo máy host của bạn đã cài đặt các công cụ sau:
- **Docker** (phiên bản >= 20.10)
- **k3d** (để quản lý cluster K3s cục bộ)
- **kubectl** (để tương tác với cluster)
- **Helm v3** (để triển khai các package)
- **Python 3** (cùng thư viện `requests` và `pyyaml` để chạy mock script và generator)
- Bộ nhớ RAM khuyến nghị: **>= 16 GB** (hệ thống được tối ưu hóa cực kỳ nhẹ, không dùng Bitnami cồng kềnh để tiết kiệm tối đa tài nguyên).

---

## 🚀 Hướng dẫn khởi chạy nhanh Hạ tầng

Hệ thống cung cấp sẵn các script tự động hóa toàn bộ quá trình thiết lập cluster và deploy ứng dụng.

### Bước 1: Khởi tạo cluster k3d
Chạy script bootstrap để tạo cluster `vdt-lakehouse` đa-node (1 Master, 3 Workers) và tự động cấu hình MTU 1400 tránh đứt gãy kết nối mạng:
```bash
chmod +x infrastructure/scripts/bootstrap.sh
./infrastructure/scripts/bootstrap.sh
```

### Bước 2: Triển khai toàn bộ Services
Chạy script deploy để tạo các namespace (`ingestion`, `streaming`, `lakehouse`, `monitoring`, `orchestration`) và triển khai toàn bộ ứng dụng qua Helm & Manifests:
```bash
chmod +x infrastructure/scripts/deploy-all.sh
./infrastructure/scripts/deploy-all.sh
```

### Bước 3: Khởi tạo luồng xử lý và bảng dữ liệu (Setup Pipeline)
Sau khi hạ tầng deploy xong, chạy script setup để biên dịch Flink job, submit Flink, cấu hình tự động cho NiFi và khởi tạo các bảng Iceberg trong Trino:
```bash
chmod +x infrastructure/scripts/setup-pipeline.sh
./infrastructure/scripts/setup-pipeline.sh
```

---

## 🌐 Thông tin cổng truy cập các dịch vụ

Sau khi deploy hoàn tất, toàn bộ các service đều được cấu hình LoadBalancer hoặc thông qua Port-Forwarding trực tiếp qua `localhost` của máy host:

| Dịch vụ | Địa chỉ truy cập (URL) | Tài khoản / Mật khẩu | Namespace | Phương thức |
| :--- | :--- | :--- | :--- | :--- |
| **Grafana (Dashboards)** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | `monitoring` | LoadBalancer |
| **Apache NiFi (Ingestion)** | [https://localhost:8443/nifi](https://localhost:8443/nifi) | `admin` / `password123456` | `ingestion` | LoadBalancer |
| **MinIO Console (Storage)** | [http://localhost:9001](http://localhost:9001) | `admin` / `password123` | `lakehouse` | LoadBalancer |
| **Kafka UI (Topic Management)** | [http://localhost:9080](http://localhost:9080) | Không có | `streaming` | LoadBalancer |
| **Trino Server (SQL queries)** | [http://localhost:8888](http://localhost:8888) | User: `admin` | `lakehouse` | LoadBalancer |
| **Flink UI (Job Monitoring)** | [http://localhost:8081](http://localhost:8081) | Không có | `streaming` | LoadBalancer |
| **Spark Master UI** | [http://localhost:7070](http://localhost:7070) | Không có | `orchestration` | LoadBalancer |
| **Apache Airflow 3** | [http://localhost:8082](http://localhost:8082) | `admin` / `admin` | `orchestration` | LoadBalancer |
| **Metabase BI** | [http://localhost:3001](http://localhost:3001) | Tạo user lần đầu | `orchestration` | LoadBalancer |
| **Prometheus (Metrics UI)** | [http://localhost:9090](http://localhost:9090) | Không có | `monitoring` | Port-Forward |
| **Alertmanager (Alerting UI)** | [http://localhost:9093](http://localhost:9093) | Không có | `monitoring` | Port-Forward |
| **PostgreSQL (dim_data DB)** | `localhost:5433` | User: `dim_user` / Pass: `dim_pass` | `lakehouse` | Port-Forward |

> [!NOTE]
> Cổng truy cập cho **Prometheus**, **Alertmanager** và **PostgreSQL (dim_data)** được tự động kích hoạt chạy ngầm sau khi chạy script `./infrastructure/scripts/setup-pipeline.sh`. 
> Đối với **Spark Master UI**, **Airflow 3** và **Metabase**, script sẽ tự động kiểm tra xem cổng tương ứng trên máy host đã được k3d map sẵn chưa; nếu chưa (chẳng hạn như chạy trên cụm k3d cũ chưa được khởi tạo lại), script sẽ tự động khởi chạy cổng port-forward dự phòng ngầm.
> Nếu cần khởi động lại thủ công tất cả các cổng từ terminal máy host, bạn có thể chạy:
> ```bash
> kubectl port-forward -n monitoring svc/prometheus-server 9090:9090 --address 0.0.0.0 &
> kubectl port-forward -n monitoring svc/prometheus-alertmanager 9093:9093 --address 0.0.0.0 &
> kubectl port-forward -n lakehouse svc/dim-data-postgresql 5433:5432 --address 0.0.0.0 &
> # Chạy thêm các lệnh này nếu cụm k3d cũ của bạn chưa map sẵn port:
> kubectl port-forward -n orchestration svc/spark-ui 7070:7070 --address 0.0.0.0 &
> kubectl port-forward -n orchestration svc/airflow 8082:8082 --address 0.0.0.0 &
> kubectl port-forward -n orchestration svc/metabase 3001:3001 --address 0.0.0.0 &
> ```

> [!IMPORTANT]
> **Lưu ý đối với Apache NiFi:** NiFi bắt buộc phải truy cập bằng giao thức **HTTPS**. Do chứng chỉ là tự ký, trình duyệt sẽ đưa ra cảnh báo bảo mật. Hãy chọn **Advanced** -> **Proceed to localhost (unsafe)** để tiếp tục truy cập.

---

## 🕹️ Hướng dẫn Chạy Demo Chi tiết (End-to-End)

Dưới đây là các bước chạy thử nghiệm hệ thống để kiểm chứng luồng dữ liệu tự động đổ về Lakehouse.

### Bước 1: Tạo dữ liệu giám sát giả lập (Mock Data)
Hệ thống cung cấp một script Python để tạo các tệp chỉ số hiệu năng máy chủ cục bộ (định dạng CSV hoặc XML luân phiên ngẫu nhiên) và tự động tải chúng lên máy chủ SFTP trong cluster:

Chạy script tạo mock data:
```bash
python3 infrastructure/scripts/generate-mock-data.py
```
*Mỗi lần chạy script này sẽ sinh ra dữ liệu cho 5 máy chủ giám sát (`prod-web-01`, `prod-web-02`, v.v.) và đẩy qua SFTP.*

### Bước 2: Quan sát luồng Ingestion & Processing
1. **Apache NiFi:** Truy cập [https://localhost:8443/nifi](https://localhost:8443/nifi). Bạn sẽ thấy NiFi tự động phát hiện file trên SFTP, đẩy file thô vào MinIO bucket `lakehouse` dưới folder `raw-file/` theo định dạng phân cấp thời gian (`raw-file/yyyy/mm/dd/HH/`), và phát hành một sự kiện JSON thông báo lên topic `file-arrival-events` của Kafka.
2. **Apache Flink:** Truy cập Flink UI tại [http://localhost:8081](http://localhost:8081) để kiểm tra Job Stream xử lý dữ liệu. Flink sẽ tiêu thụ sự kiện từ Kafka, tải tệp tương ứng từ MinIO, phân tích định dạng (CSV hoặc XML), ép kiểu trường dữ liệu hiệu năng và ghi trực tiếp xuống Iceberg Table.

### Bước 3: Truy vấn SQL bằng Trino & Thử nghiệm Time-travel
Bạn có thể truy vấn bảng Iceberg trực tiếp bằng Trino.

1. **Kiểm tra số lượng bản ghi đã cam kết (committed):**
```bash
python3 -c "
import requests
sql = 'SELECT count(*) FROM iceberg.monitoring.raw_sftp_table'
r = requests.post('http://localhost:8888/v1/statement', data=sql, headers={'X-Trino-User': 'admin'})
print('Tổng số bản ghi trong Iceberg Table:', r.json()['data'][0][0])
"
```
*(Lưu ý: Flink ghi dữ liệu theo checkpoint chu kỳ 1 phút, do đó dữ liệu mới sẽ xuất hiện sau tối đa 60 giây).*

2. **Thử nghiệm tính năng Time-travel (Đặc trưng của Iceberg):**
Liệt kê danh sách các Snapshots (các thời điểm lịch sử dữ liệu được commit):
```bash
python3 -c "
import requests
sql = 'SELECT snapshot_id, committed_at FROM iceberg.monitoring.raw_sftp_table\$snapshots ORDER BY committed_at DESC'
r = requests.post('http://localhost:8888/v1/statement', data=sql, headers={'X-Trino-User': 'admin'})
for row in r.json().get('data', []):
    print(f'Snapshot ID: {row[0]} | Thời gian commit: {row[1]}')
"
```
Truy vấn lại dữ liệu tại một snapshot lịch sử cũ:
```bash
python3 -c "
import requests
# Thay thế SNAPSHOT_ID_DUMMY bằng Snapshot ID thực tế lấy ở bước trên
snapshot_id = 'SNAPSHOT_ID_DUMMY'
sql = f'SELECT count(*) FROM iceberg.monitoring.raw_sftp_table FOR VERSION AS OF {snapshot_id}'
r = requests.post('http://localhost:8888/v1/statement', data=sql, headers={'X-Trino-User': 'admin'})
print(f'Số lượng bản ghi tại Snapshot {snapshot_id}:', r.json()['data'][0][0])
"
```

### Bước 4: Xem biểu đồ giám sát thời gian thực trên Grafana
1. Truy cập Grafana tại [http://localhost:3000](http://localhost:3000) (Tài khoản: `admin` / Mật khẩu: `admin`).
2. Vào phần **Dashboards** -> Chọn dashboard **Server Performance Monitoring**.
3. Dashboard sẽ trực quan hóa các biểu đồ CPU, RAM, Disk, IO và bảng thông số mới nhất của toàn bộ 100+ servers thời gian thực.
4. Nhờ cơ chế tự động phân nhóm động bằng SQL (`CAST(server_id AS VARCHAR) AS metric`), khi bạn chạy thêm các file dữ liệu giả lập cho các server_id mới, Grafana sẽ tự động vẽ thêm các đường biểu đồ mới mà không cần bất kỳ cấu hình hay thao tác chọn biến thủ công nào.

### Bước 5: Kiểm thử Flink CDC đồng bộ Dimension Table (PostgreSQL -> Iceberg)
Luồng Flink CDC sẽ tự động bắt các thay đổi (INSERT/UPDATE/DELETE) ở bảng cấu hình trong database PostgreSQL và đồng bộ gần thời gian thực xuống bảng Iceberg `server_config`.

1. **Kết nối DBeaver hoặc chạy lệnh SQL trực tiếp vào PostgreSQL:**
   Sử dụng DBeaver kết nối qua cổng `localhost:5433` (hoặc chạy lệnh psql trong Pod) và chèn một dòng cấu hình máy chủ mới:
   ```bash
   kubectl exec -n lakehouse svc/dim-data-postgresql -- psql -U dim_user -d dim_data -c "INSERT INTO server_config (server_id, server_name, ip, province, station) VALUES (106, 'stage-cache-01', '192.168.1.99', 'Hue', 'Tram Huong Giang');"
   ```

2. **Kiểm tra kết quả đồng bộ trên Iceberg qua Trino:**
   Truy vấn bảng Iceberg bằng Trino, bạn sẽ thấy dòng cấu hình máy chủ `106` mới xuất hiện ngay lập tức (sau tối đa 10 giây tương ứng chu kỳ checkpoint Flink):
   ```bash
   kubectl exec -n lakehouse deployment/trino-coordinator -- trino --server http://localhost:8888 --execute "SELECT * FROM iceberg.monitoring.server_config WHERE server_id = 106"
   ```
*Kết quả mong đợi:*
`"106","stage-cache-01","192.168.1.99","Hue","Tram Huong Giang"`

### Bước 6: Kiểm thử Spark Batch và BI
1. Truy cập Airflow tại [http://localhost:8082](http://localhost:8082), đăng nhập `admin` / `admin`.
2. Bật và trigger DAG `server_monitoring_hourly_kpi` để Spark đọc `raw_sftp_table`, join `server_config`, rồi upsert kết quả vào `kpi_summary`.
3. Trigger DAG `server_monitoring_daily_compaction` để chạy Iceberg compaction cho bảng raw.
4. Truy cập Metabase tại [http://localhost:3001](http://localhost:3001), tạo kết nối Presto/Trino tới host `trino.lakehouse.svc.cluster.local`, port `8888`, catalog `iceberg`, schema `monitoring`, user `admin`.
5. Dùng bảng `kpi_summary` để dựng dashboard BI theo `province`, `station`, `window_start`.

---

## 🔔 Cấu hình Cảnh báo Email (SMTP)

Để cấu hình hệ thống tự động gửi cảnh báo về email của bạn khi phát hiện hiệu năng server quá tải (ví dụ: CPU > 90%):

1. **Khởi tạo thông tin**: Sửa file `.env` ở thư mục gốc và điền thông tin SMTP của bạn:
   * **Gmail**: Bật Xác minh 2 bước và tạo **Mật khẩu ứng dụng (App Password)** gồm 16 ký tự để điền vào `GF_SMTP_PASSWORD`.
   * **Email nhận thư**: Điền vào `SMTP_RECIPIENT`.
2. **Kích hoạt cấu hình**: Chạy script helper để tự động nạp Secret và restart Grafana Pod:
   ```bash
   ./infrastructure/scripts/setup-email-alerts.sh
   ```
3. **Kiểm tra**: Vào Grafana (`Alerting` -> `Contact points`), chọn `Email Channel` và gửi thử một Email Test.

---

## 🔄 Khởi động lại & Tiếp tục (Start & Stop Cluster)

Khi bạn muốn tắt máy hoặc tạm dừng hệ thống mà không làm mất dữ liệu và các cấu hình đã cài:

1. **Tạm dừng Cluster**:
   ```bash
   k3d cluster stop vdt-lakehouse
   ```
2. **Khởi động lại & Tiếp tục**:
   * Chạy lệnh start cluster:
     ```bash
     k3d cluster start vdt-lakehouse
     ```
   * Chạy lại pipeline để kích hoạt Gmail connection:
     ```bash
       ./infrastructure/scripts/setup-email-alerts.sh
     ```
   * Chạy lại pipeline để kích hoạt Flink/NiFi và cổng port-forward tự động:
     ```bash
     ./infrastructure/scripts/setup-pipeline.sh
     ```

---

## ❓ Xử lý sự cố (Troubleshooting)

Nếu gặp sự cố trong quá trình vận hành, vui lòng kiểm tra các mục dưới đây:

### 1. Hướng dẫn xử lý sự cố chung
* [Hướng dẫn Xử lý sự cố Spark Batch & Kubernetes](file:///home/haiden/bku/vdt/server-monitoring-lakehouse/docs/troubleshooting/spark_batch_troubleshooting.md)

### 2. Sự cố lệch Metadata bảng Iceberg (NoSuchKey / NotFoundException)
* **Triệu chứng**: Chạy câu query SQL trong Trino hoặc Spark báo lỗi `NoSuchKey: File not found` ở đường dẫn chứa hậu tố UUID cũ, hoặc metadata và data nằm ở 2 thư mục UUID khác nhau trong MinIO.
* **Nguyên nhân**: Bảng đã bị `DROP` và `CREATE` lại, nhưng Flink Ingestion/CDC Job cũ vẫn đang chạy ngầm và tiếp tục ghi file data vào thư mục cũ.
* **Khắc phục**: 
  1. Hủy (Cancel) job Flink cũ đang bị hỏng trên Flink UI (hoặc via CLI).
  2. Xóa các folder UUID rác đã bị xóa trong MinIO để làm sạch vùng lưu trữ.
  3. Submit lại Flink job mới để Flink nhận diện lại đường dẫn bảng chính thức từ Hive Metastore.

### 3. Sự cố bảng Iceberg bị LOCK trên Hive Metastore (MetastoreLock$WaitingForLockException)
* **Triệu chứng**: Flink hoặc Spark job bị kẹt ở trạng thái commit/checkpoint và log báo lỗi `Waiting for lock on table...`
* **Nguyên nhân**: Job trước đó bị crash đột ngột trong khi đang commit dẫn đến lock mồ côi (stale lock) trong database của Hive Metastore chưa được giải phóng.
* **Khắc phục**: Chạy lệnh SQL sau trực tiếp vào database metastore của Hive để giải phóng toàn bộ lock:
  ```bash
  kubectl exec -n lakehouse hive-db-postgresql-0 -- env PGPASSWORD="password123" psql -U hive -d metastore -c "DELETE FROM hive_locks;"
  ```

### 4. Bảng server_config trống trơn hoặc Spark Join trả về 0 bản ghi (Cold Start)
* **Triệu chứng**: Trigger DAG Airflow báo `success` nhưng không thấy sinh ra file dữ liệu KPI mới trong MinIO.
* **Nguyên nhân**: CDC Flink chỉ đồng bộ các sự kiện thay đổi dữ liệu (DML) từ thời điểm khởi động. Nếu bảng Postgres đã seed dữ liệu từ trước khi Flink CDC chạy lần đầu, dữ liệu cũ sẽ không tự động đồng bộ sang Iceberg `server_config` dẫn đến phép inner join trong Spark bị rỗng.
* **Khắc phục**: Tạo một thay đổi giả lập trên bảng cấu hình PostgreSQL để buộc Flink CDC đồng bộ lại:
  ```bash
  kubectl exec -n lakehouse dim-data-postgresql-0 -- psql -U dim_user -d dim_data -c "UPDATE server_config SET station = station;"
  ```


---

## 🛠️ Dọn dẹp & Xóa toàn bộ Cluster
Để dọn dẹp tài nguyên và xóa hoàn toàn cluster thử nghiệm trên máy local của bạn:
```bash
k3d cluster delete vdt-lakehouse
```

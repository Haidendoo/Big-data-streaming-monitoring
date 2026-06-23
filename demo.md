# Hướng dẫn Demo Luồng Dữ liệu (End-to-End Demo Guide)

Tài liệu này hướng dẫn cách chạy và trình diễn luồng dữ liệu thời gian thực của hệ thống giám sát hiệu năng máy chủ (**Server Monitoring Lakehouse**) từ nguồn dữ liệu thô đến bảng điều khiển trực quan trên Grafana.

---

## 1. Tổng quan Kiến trúc Demo

```
                                  [ SFTP Server ]
                                         │  (Tải file dữ liệu mới lên)
                                         ▼
                                  [ Apache NiFi ]
                    ┌────────────────────┴────────────────────┐
                    │ (Lưu trữ file thô)                      │ (Bắn sự kiện)
                    ▼                                         ▼
             [ MinIO (S3) ]                            [ Apache Kafka ]
             landing-zone/                             topic: file-arrival-events
                    │                                         │
                    └────────────────────┬────────────────────┘
                                         ▼
                                 [ Apache Flink ] (Real-time Stream Parser)
                                         │ (Đọc file từ S3, parse & map chỉ số)
                                         ▼
                             [ Apache Iceberg (Fact) ]
                            monitoring.raw_sftp_table
                                         │
                                         ▼ (Truy vấn SQL realtime trực tiếp)
                            [ Grafana Dashboard ]
```

---

## 2. Chuẩn bị trước khi Demo (Prerequisites)

Đảm bảo các cổng kết nối đến service trong Kubernetes được forward về local để truy cập tiện lợi:

| Thành phần | Địa chỉ truy cập | Tài khoản / Mật khẩu | Lệnh Port-Forward |
| :--- | :--- | :--- | :--- |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | `admin` / `admin` | `kubectl port-forward -n monitoring svc/grafana 3000:80` |
| **Kafka UI** | [http://localhost:9080](http://localhost:9080) | Không có | Cổng mặc định của cluster |
| **Apache NiFi** | [http://localhost:8080](http://localhost:8080) | Không có | Cổng mặc định của cluster |

---

## 3. Các bước thực hiện Demo (Step-by-Step)

### Bước 1: Khởi tạo nguồn dữ liệu giả lập (SFTP Upload)
Chạy script python trên máy host để sinh ngẫu nhiên dữ liệu CPU, RAM, Disk, I/O của các máy chủ và tải lên thư mục SFTP:
```bash
python3 infrastructure/scripts/generate-mock-data.py
```
* **Dưới nền:** Script tạo file `.csv` hoặc `.xml` tương tự như hệ thống thật rồi đẩy vào `/home/sftpuser/upload/` trên SFTP container.

---

### Bước 2: NiFi tự động Thu thập & Chuyển tiếp (NiFi & Kafka)
1. Truy cập **Apache NiFi UI** tại [http://localhost:8080/nifi/](http://localhost:8080/nifi/).
2. NiFi sẽ phát hiện file mới từ SFTP, chuyển tiếp file vào **MinIO S3** tại bucket `landing-zone`.
3. Đồng thời NiFi đẩy một JSON event thông báo sang Kafka topic `file-arrival-events` với nội dung dạng:
   ```json
   {
     "file_path": "s3a://landing-zone/2026/06/23/08/metrics_20260623_150555.csv",
     "file_name": "metrics_20260623_150555.csv",
     "timestamp": "2026-06-23T08:06:13Z",
     "format": "csv"
   }
   ```
* **Kiểm chứng:** Có thể mở **Kafka UI** ([http://localhost:9080](http://localhost:9080)), vào topic `file-arrival-events` $\rightarrow$ tab Messages để xem sự kiện vừa bắn lên.

---

### Bước 3: Flink xử lý luồng sự kiện thời gian thực (Flink Stream Processing)
1. Flink Job đang chạy ngầm trong cụm Kubernetes sẽ tự động nhận sự kiện từ Kafka.
2. Nó trực tiếp đọc file từ MinIO qua thư viện Hadoop S3A, parse dữ liệu XML/CSV, map kiểu và chỉ lọc các thuộc tính phần cứng thô (`server_id`, `cpu_util`, `ram_util`, `disk_util`, `io_stat`).
3. Dữ liệu được ghi thẳng xuống bảng **Iceberg Fact** (`monitoring.raw_sftp_table`).
4. **Kiểm tra Logs:** Xem tiến trình parse file của Flink:
   ```bash
   kubectl logs -n streaming -l component=taskmanager --tail=50
   ```
   *Kết quả mong đợi:*
   ```text
   Processing event: FileArrivalEvent{filePath='s3a://landing-zone/.../metrics_xxx.csv', ...}
   Successfully processed metrics_xxx.csv. Emitted 5 records.
   Committed append for checkpoint 2 to table hive_catalog.monitoring.raw_sftp_table
   ```

---

### Bước 4: Kiểm chứng dữ liệu thô qua Trino (Trino Query Engine)
Dữ liệu thô lưu trực tiếp trong bảng Fact (`raw_sftp_table`) có thể được kiểm chứng nhanh qua Trino.

Mở một Terminal khác và kiểm chứng dữ liệu qua Trino bằng script truy vấn:
```bash
python3 ~/.gemini/antigravity-cli/brain/ecbc6fdc-4ca6-4caa-918c-55a509b1fbc5/scratch/query_trino.py "SELECT * FROM iceberg.monitoring.raw_sftp_table ORDER BY ts DESC LIMIT 5"
```
* **Dưới nền:** Câu truy vấn lấy dữ liệu trực tiếp từ bảng Fact `raw_sftp_table`.
* *Kết quả trả về sẽ hiển thị `server_id` thô và các hardware metrics (không cần join thông tin tĩnh).*

---

### Bước 5: Kiểm tra biểu đồ giám sát trực quan (Grafana Dashboard)
1. Mở trình duyệt và truy cập **Grafana** tại [http://localhost:3000](http://localhost:3000).
2. Đăng nhập bằng tài khoản: `admin` / mật khẩu: `admin`.
3. Mở dashboard **"Server Performance Monitoring"**.
4. Các biểu đồ CPU, RAM, Disk, I/O của tất cả máy chủ sẽ được cập nhật tự động.
5. **Điểm nổi bật khi demo:** Nhờ cơ chế tự động gom nhóm bằng SQL (`CAST(server_id AS VARCHAR) AS metric`), khi bạn tiếp tục chạy script tạo mock-data mới ở Bước 1, Grafana sẽ **tự động vẽ thêm các đường biểu đồ tương ứng cho các server_id đó** trên màn hình mà không cần bất kỳ cấu hình hay thao tác thủ công nào.

---

## 4. Điểm nhấn Công nghệ (Key Highlights)

Khi thuyết trình/demo, bạn nên nhấn mạnh các điểm tối ưu kiến trúc sau:
1. **Luồng Realtime Siêu Nhẹ (Lightweight Realtime Ingestion):** Bản tin truyền đi và lưu trữ dưới dạng thô chỉ gồm `server_id` và các chỉ số đo đạc, giúp tối ưu hóa tối đa băng thông mạng và dung lượng lưu trữ trên Lakehouse.
2. **Dynamic Dashboard Querying:** Sử dụng SQL động gom nhóm theo `server_id` giúp Grafana tự động vẽ thêm các đường biểu đồ khi có máy chủ mới gia nhập hệ thống.
3. **Quản lý Classloader & Relocation (Flink):** Giải quyết triệt để vấn đề xung đột thư viện XML/Jackson của hệ thống bằng cách đóng gói độc lập các thư viện phụ thuộc và đổi tên package (relocate) Jackson sang `com.company.shaded.jackson` trong fat jar.

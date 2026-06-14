# Tài liệu Yêu cầu Kỹ thuật Chi tiết - Hệ thống Giám sát Server Lakehouse

## 1. Tổng quan hệ thống
Hệ thống thu thập dữ liệu giám sát từ 100+ server định kỳ mỗi phút một lần. Dữ liệu được đẩy qua luồng: **SFTP -> NiFi -> Minio (Landing) & Kafka -> Flink -> Iceberg (Lakehouse) -> Trino (Query)**. Toàn bộ hệ thống được triển khai trên nền tảng **k3s/k3d** để đảm bảo khả năng mở rộng và quản lý tài nguyên tập trung.

## 2. Hạ tầng kỹ thuật (Infrastructure)
*   **Platform:** K3s/K3d (Kubernetes lightweight).
*   **Kiến trúc:** Distributed (1 Master node, 3 Worker nodes).
*   **Quản lý tài nguyên:**
    *   Sử dụng Helm Chart để deploy các thành phần.
    *   Thiết lập Resource Requests/Limits cho từng Pod (NiFi, Flink, Kafka, Trino).
    *   Persistent Volume: Sử dụng `local-path` hoặc `longhorn` cho lưu trữ dữ liệu Kafka và Minio.

## 3. Luồng Thu thập Dữ liệu (Ingestion - NiFi)
*   **Nguồn (Source):** SFTP Server (giả định có sẵn hoặc dựng 1 container SFTP để test).
*   **Định dạng file:** XML (theo `req.md`) hoặc CSV (theo yêu cầu bổ sung). Cấu trúc: `Timestamp, ServerName, IP, CPU, RAM, DISK, IO`.
*   **Quy trình NiFi:**
    1.  **ListSFTP/FetchSFTP:** Polling file mỗi 1 phút.
    2.  **PutHDFS/PutS3 (Minio Landing):** Lưu file nguyên bản vào Minio bucket `landing-zone/yyyy/mm/dd/HH/`.
    3.  **PublishKafka:** Sau khi lưu thành công vào Minio, gửi 1 message JSON vào Kafka topic `file-arrival-events`.
        *   *Schema Message:* `{"file_path": "s3://landing-zone/...", "file_name": "...", "timestamp": "...", "format": "xml"}`.
    4.  **Xử lý lỗi:** Di chuyển các file lỗi sang thư mục `error/` trên SFTP hoặc Minio.

## 4. Hàng đợi thông điệp (Messaging - Kafka)
*   **Topic:** `file-arrival-events`.
*   **Partitions:** Ít nhất 3 partitions để hỗ trợ xử lý song song.
*   **Retention:** Lưu trữ 7 ngày.

## 5. Luồng Xử lý dữ liệu (Processing - Flink)
*   **Input:** Kafka Consumer (topic `file-arrival-events`).
*   **Xử lý:**
    1.  Đọc message từ Kafka để lấy đường dẫn file.
    2.  Sử dụng S3 Connector để đọc nội dung file từ Minio.
    3.  **Parser:** Parse nội dung XML/CSV thành DataStream/Table API.
    4.  **Transformation:** Cast kiểu dữ liệu (CPU/RAM thành Float, Timestamp thành SQL Timestamp).
*   **Output (Sink):** Iceberg Sink.
    *   Sử dụng Iceberg Flink Connector.
    *   Chế độ ghi: `upsert` hoặc `append`.
    *   Commit Interval: 1 phút (đồng bộ với tần suất sinh file).

## 6. Lưu trữ Lakehouse (Iceberg + Minio + Hive)
*   **Storage:** Minio (S3 API).
*   **Metadata:** Hive Metastore (lưu trữ schema và vị trí các file Iceberg).
*   **Table Format:** Apache Iceberg.
    *   **Table Name:** `monitoring.server_metrics`.
    *   **Partitioning:** Theo `day(timestamp)` và `hour(timestamp)` để tối ưu truy vấn theo thời gian.
    *   **Schema:**
        *   `ts`: Timestamp
        *   `server_name`: String
        *   `ip`: String
        *   `cpu_util`: Float
        *   `ram_util`: Float
        *   `disk_util`: Float
        *   `io_stat`: Float

## 7. Truy vấn & Khai thác (Query Engine - Trino)
*   **Trino Catalog:** Cấu hình kết nối tới Hive Metastore và Minio.
*   **Khả năng truy vấn:**
    *   Truy vấn trực tiếp dữ liệu trên Iceberg table bằng SQL tiêu chuẩn.
    *   Hỗ trợ Time-travel (truy vấn dữ liệu tại một thời điểm trong quá khứ).
    *   Tối ưu hóa query nhờ cơ chế Partition Pruning và Columnar Storage (Parquet/ORC).

## 8. Kế hoạch triển khai (Roadmap)
1.  Thiết lập Cluster k3d.
2.  Deploy Minio & Hive Metastore.
3.  Deploy Kafka & NiFi.
4.  Cấu hình luồng NiFi (SFTP -> Minio -> Kafka).
5.  Viết code Flink Job (Java/Python) và deploy lên cluster.
6.  Cài đặt Trino và kiểm tra dữ liệu.

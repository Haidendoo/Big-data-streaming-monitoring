# Server Monitoring Lakehouse

Dự án xây dựng hệ thống thu thập và xử lý dữ liệu giám sát server theo kiến trúc Lakehouse (Iceberg, Minio, Flink, Trino).

## 📁 Cấu trúc thư mục
- `infrastructure/`: Chứa k3d config, Helm charts và scripts triển khai hạ tầng.
- `nifi/`: Chứa cấu hình luồng thu thập dữ liệu (SFTP -> Minio/Kafka).
- `flink/`: Mã nguồn Job Flink xử lý file XML/CSV và ghi vào Iceberg.
- `catalogs/`: Định nghĩa schema và catalogs cho Trino/Iceberg.
- `monitoring/`: Cấu hình Prometheus và Grafana để quan sát hệ thống.

## 🚀 Hướng dẫn nhanh

### 1. Khởi tạo hạ tầng
Di chuyển vào thư mục dự án và chạy bootstrap:
```bash
cd server-monitoring-lakehouse
chmod +x infrastructure/scripts/*.sh
./infrastructure/scripts/bootstrap.sh
```

### 2. Kiểm tra dịch vụ
```bash
kubectl get pods -A
```

### 3. Truy cập UI
- **Minio Console:** http://localhost:9001
- **NiFi UI:** https://localhost:8443
- **Trino UI:** http://localhost:8888
- **Flink UI:** http://localhost:8081

## 🛠️ Công nghệ sử dụng
- **Orchestration:** k3s/k3d
- **Ingestion:** Apache NiFi
- **Streaming:** Apache Kafka (KRaft mode)
- **Processing:** Apache Flink
- **Table Format:** Apache Iceberg
- **Query Engine:** Trino
- **Storage:** Minio (S3 API)

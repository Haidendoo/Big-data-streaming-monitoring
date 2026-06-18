# Server Monitoring Lakehouse Platform

Hệ thống giám sát máy chủ tập trung sử dụng kiến trúc Lakehouse (Minio, Iceberg, Trino) tích hợp NiFi, Kafka, Flink và Monitoring Stack.

## 🚀 Hướng dẫn khởi chạy nhanh

Để dựng toàn bộ hạ tầng (Cluster k3d, Storage, Streaming, SQL, Monitoring):

```bash
chmod +x infrastructure/scripts/bootstrap.sh
./infrastructure/scripts/bootstrap.sh
```

## 🌐 Thông tin truy cập các dịch vụ

Tất cả các dịch vụ đều được cấu hình để truy cập trực tiếp từ máy Host thông qua `localhost`.

| Dịch vụ | Địa chỉ truy cập | Tài khoản / Mật khẩu |
| :--- | :--- | :--- |
| **NiFi (Ingestion)** | [https://localhost:8443/nifi](https://localhost:8443/nifi) | `admin` / `password123456` |
| **Minio (Storage)** | [http://localhost:9001](http://localhost:9001) | `admin` / `password123` |
| **Grafana (Dashboards)**| [http://localhost:3000](http://localhost:3000) | `admin` / *Lấy lệnh bên dưới* |
| **Kafka UI** | [http://localhost:9080](http://localhost:9080) | Không có |
| **Trino (SQL)** | [http://localhost:8888](http://localhost:8888) | User: `admin` |
| **Flink UI** | [http://localhost:8081](http://localhost:8081) | Không có |

> **Lưu ý NiFi:** Phải dùng **HTTPS**. Trình duyệt sẽ báo cảnh báo bảo mật, hãy chọn "Advanced" -> "Proceed to localhost".

## 🔑 Lệnh lấy mật khẩu Grafana
```bash
kubectl get secret --namespace monitoring grafana -o jsonpath="{.data.admin-password}" | base64 --decode ; echo
```

## 🏗️ Cấu trúc hạ tầng (Standalone - No Bitnami)
Hệ thống đã được tối ưu hóa cho môi trường mạng hạn chế:
- **Official Images:** Sử dụng ảnh gốc từ Docker Hub (`apache/nifi`, `apache/kafka`, `postgres:alpine`).
- **Network Fix:** Tự động cấu hình MTU 1400 trong script bootstrap để tránh đứt kết nối.
- **Permanent Manifests:** Các file cấu hình nằm tại `infrastructure/helm/*/manifests.yaml`.

## 🛠️ Xóa toàn bộ Cluster
```bash
k3d cluster delete vdt-lakehouse
```

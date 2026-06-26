#!/bin/bash
set -e

# Deploy dim-data PostgreSQL for CDC
echo "🚀 Deploying dim-data PostgreSQL for CDC..."
helm upgrade --install dim-data-postgres infrastructure/helm/postgres-dim \
  --namespace lakehouse --create-namespace

# Wait for PostgreSQL pod to be ready
echo "⏳ Waiting for dim-data PostgreSQL pod..."
kubectl wait --namespace lakehouse --for=condition=ready pod -l app=dim-postgres --timeout=300s

# Initialize dim_data schema/table (if not exists) and seed data
echo "🔧 Initializing and seeding dim_data schema..."
kubectl exec -n lakehouse svc/dim-data-postgresql \
  -- psql -U dim_user -d dim_data -c "
CREATE TABLE IF NOT EXISTS server_config (
  server_id INT PRIMARY KEY,
  server_name VARCHAR(100),
  ip VARCHAR(50),
  province VARCHAR(100),
  station VARCHAR(100)
);
INSERT INTO server_config (server_id, server_name, ip, province, station) VALUES
(101, 'prod-web-01', '192.168.1.10', 'TPHCM', 'Tram Quan 1'),
(102, 'prod-web-02', '192.168.1.11', 'TPHCM', 'Tram Quan 1'),
(103, 'prod-db-01', '192.168.1.20', 'Ha Noi', 'Tram Cau Giay'),
(104, 'prod-db-02', '192.168.1.21', 'Ha Noi', 'Tram Cau Giay'),
(105, 'stage-app-01', '172.16.5.10', 'Da Nang', 'Tram Hai Chau')
ON CONFLICT (server_id) DO NOTHING;
" || true

echo "🚀 Checking cluster status and waiting for pods to be fully ready..."

# Waiting for services to be ready
echo "⏳ Waiting for Trino, SFTP, NiFi, Flink, Kafka UI, Spark, Airflow, and Metabase pods..."
kubectl wait --namespace lakehouse --for=condition=ready pod -l app.kubernetes.io/name=trino --timeout=300s
kubectl wait --namespace ingestion --for=condition=ready pod -l app=sftp --timeout=300s
kubectl wait --namespace ingestion --for=condition=ready pod -l app=nifi --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l app=kafka-ui --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l app=kafka --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l component=jobmanager --timeout=300s
kubectl wait --namespace orchestration --for=condition=ready pod -l app=spark,component=master --timeout=300s
kubectl wait --namespace orchestration --for=condition=ready pod -l app=spark,component=worker --timeout=300s
kubectl wait --namespace orchestration --for=condition=ready pod -l app=airflow --timeout=600s
kubectl wait --namespace orchestration --for=condition=ready pod -l app=metabase --timeout=300s

echo "✅ All pods are ready. Running setup-pipeline.py..."
chmod +x infrastructure/scripts/setup-pipeline.py
python3 infrastructure/scripts/setup-pipeline.py

echo "🔄 Khởi động port-forward chạy ngầm cho các dịch vụ..."
pkill -f "port-forward.*9090" || true
pkill -f "port-forward.*9093" || true
pkill -f "port-forward.*5433" || true
pkill -f "port-forward.*7070" || true
pkill -f "port-forward.*8082" || true
pkill -f "port-forward.*3001" || true
sleep 1

# Luôn port-forward cho các ClusterIP services (không được expose qua k3d LoadBalancer)
nohup kubectl port-forward -n monitoring svc/prometheus-server 9090:9090 --address 0.0.0.0 >/dev/null 2>&1 &
nohup kubectl port-forward -n monitoring svc/prometheus-alertmanager 9093:9093 --address 0.0.0.0 >/dev/null 2>&1 &
nohup kubectl port-forward -n lakehouse svc/dim-data-postgresql 5433:5432 --address 0.0.0.0 >/dev/null 2>&1 &

# Chỉ port-forward cho Spark, Airflow, Metabase nếu port tương ứng trên host chưa được k3d map (chưa có ai lắng nghe)
if ! nc -z 127.0.0.1 7070 >/dev/null 2>&1; then
  echo "⚡ Cổng 7070 chưa mở trên host, tiến hành port-forward cho Spark UI..."
  nohup kubectl port-forward -n orchestration svc/spark-ui 7070:7070 --address 0.0.0.0 >/dev/null 2>&1 &
else
  echo "⚡ Cổng 7070 đã được k3d map sẵn trên máy host."
fi

if ! nc -z 127.0.0.1 8082 >/dev/null 2>&1; then
  echo "🌬️ Cổng 8082 chưa mở trên host, tiến hành port-forward cho Airflow 3..."
  nohup kubectl port-forward -n orchestration svc/airflow 8082:8082 --address 0.0.0.0 >/dev/null 2>&1 &
else
  echo "🌬️ Cổng 8082 đã được k3d map sẵn trên máy host."
fi

if ! nc -z 127.0.0.1 3001 >/dev/null 2>&1; then
  echo "📊 Cổng 3001 chưa mở trên host, tiến hành port-forward cho Metabase..."
  nohup kubectl port-forward -n orchestration svc/metabase 3001:3001 --address 0.0.0.0 >/dev/null 2>&1 &
else
  echo "📊 Cổng 3001 đã được k3d map sẵn trên máy host."
fi

sleep 2

echo "✨ Đã kích hoạt cổng truy cập từ máy host:"
echo "📊 Prometheus UI: http://localhost:9090"
echo "🔔 Alertmanager UI: http://localhost:9093"
echo "🐘 PostgreSQL (dim_data) Port: localhost:5433 (DBeaver connection)"
echo "   ↳ User: dim_user | Pass: dim_pass | DB: dim_data"
echo "⚡ Spark UI: http://localhost:7070"
echo "🌬️ Airflow 3: http://localhost:8082"
echo "📊 Metabase: http://localhost:3001"

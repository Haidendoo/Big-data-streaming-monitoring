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
echo "⏳ Waiting for Trino, SFTP, NiFi, Flink, and Kafka UI pods..."
kubectl wait --namespace lakehouse --for=condition=ready pod -l app.kubernetes.io/name=trino --timeout=300s
kubectl wait --namespace ingestion --for=condition=ready pod -l app=sftp --timeout=300s
kubectl wait --namespace ingestion --for=condition=ready pod -l app=nifi --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l app=kafka-ui --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l app=kafka --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l component=jobmanager --timeout=300s

echo "✅ All pods are ready. Running setup-pipeline.py..."
chmod +x infrastructure/scripts/setup-pipeline.py
python3 infrastructure/scripts/setup-pipeline.py

echo "🔄 Khởi động port-forward chạy ngầm cho Prometheus, Alertmanager và PostgreSQL (dim_data)..."
pkill -f "port-forward.*9090" || true
pkill -f "port-forward.*9093" || true
pkill -f "port-forward.*5433" || true
sleep 1

nohup kubectl port-forward -n monitoring svc/prometheus-server 9090:9090 --address 0.0.0.0 >/dev/null 2>&1 &
nohup kubectl port-forward -n monitoring svc/prometheus-alertmanager 9093:9093 --address 0.0.0.0 >/dev/null 2>&1 &
nohup kubectl port-forward -n lakehouse svc/dim-data-postgresql 5433:5432 --address 0.0.0.0 >/dev/null 2>&1 &
sleep 2

echo "✨ Đã kích hoạt cổng truy cập từ máy host:"
echo "📊 Prometheus UI: http://localhost:9090"
echo "🔔 Alertmanager UI: http://localhost:9093"
echo "🐘 PostgreSQL (dim_data) Port: localhost:5433 (DBeaver connection)"
echo "   ↳ User: dim_user | Pass: dim_pass | DB: dim_data"

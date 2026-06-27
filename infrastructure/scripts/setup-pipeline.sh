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

# Auto-recover NiFi if it was scaled down (e.g. after cluster restart crash-loop)
NIFI_REPLICAS=$(kubectl get deployment nifi -n ingestion -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
if [ "$NIFI_REPLICAS" -eq "0" ]; then
  echo "🔧 NiFi deployment is scaled to 0. Scaling up to 1..."
  kubectl scale deployment nifi -n ingestion --replicas=1
fi

# Ensure Flink TaskManager has 2 replicas (may be reset to 1 after cluster restart)
FLINK_TM_REPLICAS=$(kubectl get deployment flink-taskmanager -n streaming -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "1")
if [ "$FLINK_TM_REPLICAS" -lt "2" ]; then
  echo "🔧 Flink TaskManager replicas=$FLINK_TM_REPLICAS. Scaling up to 2..."
  kubectl scale deployment flink-taskmanager -n streaming --replicas=2
fi

# Ensure Kafka StatefulSet has 3 replicas (KRaft quorum requires all 3 voters)
KAFKA_REPLICAS=$(kubectl get statefulset kafka -n streaming -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "3")
if [ "$KAFKA_REPLICAS" -lt "3" ]; then
  echo "🔧 Kafka StatefulSet replicas=$KAFKA_REPLICAS but KRaft needs 3. Scaling up to 3..."
  kubectl scale statefulset kafka -n streaming --replicas=3
  echo "⏳ Waiting 30s for Kafka pods to initialize..."
  sleep 30
fi

# Ensure Airflow DAGs are updated from local dags/ directory
echo "📂 Đồng bộ hóa Airflow DAGs từ thư mục dags/..."
kubectl create configmap airflow-dags --from-file=dags/ -n orchestration --dry-run=client -o yaml | kubectl apply -f -

# Waiting for services to be ready
echo "⏳ Waiting for Trino, SFTP, Flink, Kafka, Spark, Airflow, and Metabase pods..."
kubectl wait --namespace lakehouse --for=condition=ready pod -l app.kubernetes.io/name=trino --timeout=300s
kubectl wait --namespace ingestion --for=condition=ready pod -l app=sftp --timeout=300s
# NiFi: wait up to 120s for it to be ready (we may have just scaled it up)
if kubectl get pods -n ingestion -l app=nifi 2>/dev/null | grep -q 'Running\|Pending\|Init\|ContainerCreating'; then
  kubectl wait --namespace ingestion --for=condition=ready pod -l app=nifi --timeout=120s || echo "⚠️  NiFi not ready in time, will configure after."
else
  echo "⚠️  NiFi pods not found, skipping NiFi wait."
fi
kubectl wait --namespace streaming --for=condition=ready pod -l app=kafka-ui --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l app=kafka --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l app=flink,component=jobmanager --timeout=300s
kubectl wait --namespace orchestration --for=condition=ready pod -l app=spark,component=master --timeout=300s
kubectl wait --namespace orchestration --for=condition=ready pod -l app=spark,component=worker --timeout=300s
kubectl wait --namespace orchestration --for=condition=ready pod -l app=airflow --timeout=600s
kubectl wait --namespace orchestration --for=condition=ready pod -l app=metabase --timeout=300s

echo "✅ All pods are ready. Running setup-pipeline.py..."
chmod +x infrastructure/scripts/setup-pipeline.py
python3 infrastructure/scripts/setup-pipeline.py

# Always reconfigure NiFi after setup (handles fresh pod with empty flow)
echo "⚙️  Reconfiguring NiFi pipeline (idempotent)..."
python3 nifi/configure_nifi.py && echo "✅ NiFi pipeline configured." || echo "⚠️  NiFi configure failed (non-fatal)."

echo "🔄 Khởi động port-forward chạy ngầm cho các dịch vụ..."
pkill -f "kubectl port-forward" 2>/dev/null || true
sleep 2

kubectl port-forward -n monitoring svc/prometheus-server 9090:9090 --address 0.0.0.0 >/dev/null 2>&1 &
kubectl port-forward -n monitoring svc/prometheus-alertmanager 9093:9093 --address 0.0.0.0 >/dev/null 2>&1 &
kubectl port-forward -n lakehouse svc/dim-data-postgresql 5433:5432 --address 0.0.0.0 >/dev/null 2>&1 &
kubectl port-forward -n orchestration svc/spark-ui 7070:7070 --address 0.0.0.0 >/dev/null 2>&1 &
kubectl port-forward -n orchestration svc/airflow 8082:8082 --address 0.0.0.0 >/dev/null 2>&1 &
kubectl port-forward -n orchestration svc/metabase 3001:3001 --address 0.0.0.0 >/dev/null 2>&1 &

# Tách khỏi terminal — process sống sau khi đóng terminal
disown -a 2>/dev/null || true

sleep 3

echo ""
echo "✨ Trạng thái cổng truy cập:"
for entry in "9090:Prometheus:http://localhost:9090" \
             "9093:Alertmanager:http://localhost:9093" \
             "5433:PostgreSQL dim_data:localhost:5433 (user=dim_user pass=dim_pass)" \
             "7070:Spark UI:http://localhost:7070" \
             "8082:Airflow 3:http://localhost:8082" \
             "3001:Metabase:http://localhost:3001"; do
  port=$(echo "$entry" | cut -d: -f1)
  name=$(echo "$entry" | cut -d: -f2)
  url=$(echo "$entry" | cut -d: -f3-)
  nc -z 127.0.0.1 "$port" >/dev/null 2>&1 \
    && echo "  ✅ $name → $url" \
    || echo "  ❌ $name → $url (FAILED)"
done

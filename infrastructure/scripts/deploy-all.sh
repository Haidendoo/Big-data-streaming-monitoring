#!/bin/bash
set -e

# Tạo các namespace
kubectl create namespace lakehouse --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace ingestion --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace streaming --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace orchestration --dry-run=client -o yaml | kubectl apply -f -

# Loại bỏ các repos cũ không dùng nữa (Bitnami, Cetic)
helm repo remove bitnami 2>/dev/null || true
helm repo remove cetic 2>/dev/null || true

# Thêm repos (Chỉ giữ lại các repo cần thiết)
helm repo add trino https://trinodb.github.io/charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

HELM_ROOT="infrastructure/helm"

echo "🧹 Dọn dẹp tàn dư cũ..."
# Xóa các deployment cũ có thể xung đột (Nếu dùng Helm Bitnami trước đó)
helm uninstall minio kafka postgres hive-db -n lakehouse 2>/dev/null || true
helm uninstall kafka -n streaming 2>/dev/null || true

echo "📦 Triển khai các thành phần hạ tầng (Sử dụng Official Manifests)..."

# 0. Lakehouse Platform (Namespaces, Ingress)
helm upgrade --install platform $HELM_ROOT/lakehouse-platform -n lakehouse

# 1. Minio (Official Image)
kubectl apply -f $HELM_ROOT/minio/minio-standalone-manifest.yaml

# 2. Kafka (Apache Image) & Kafka UI
echo "Deploying Kafka (Apache) and Kafka UI..."
kubectl apply -f $HELM_ROOT/kafka/kafka-manifest.yaml
kubectl apply -f $HELM_ROOT/kafka-ui/kafka-ui-manifest.yaml

# 3. NiFi 2.x
echo "Deploying NiFi 2.x..."
kubectl apply -f $HELM_ROOT/nifi/nifi-2-manifest.yaml

# 4. PostgreSQL & Hive Metastore (Official Images)
echo "Deploying PostgreSQL and Hive Metastore..."
kubectl apply -f $HELM_ROOT/postgres/postgres-manifest.yaml
kubectl apply -f $HELM_ROOT/hive-metastore/metastore-manifest.yaml

# 5. Trino
echo "Deploying Trino..."
helm upgrade --install trino trino/trino -n lakehouse -f $HELM_ROOT/trino/values.yaml

# 6. Flink
echo "Deploying Flink..."
helm upgrade --install flink $HELM_ROOT/flink -n streaming -f $HELM_ROOT/flink/values.yaml

# 7. Monitoring (Prometheus & Grafana)
echo "Deploying Monitoring Stack..."
helm upgrade --install prometheus prometheus-community/prometheus -n monitoring -f $HELM_ROOT/prometheus/values.yaml
helm upgrade --install grafana grafana/grafana -n monitoring -f $HELM_ROOT/grafana/values.yaml

# 8. Batch Layer (Spark, Airflow, Metabase)
echo "Deploying Batch Layer (Spark, Airflow, Metabase)..."
kubectl apply -f $HELM_ROOT/spark/spark-distributed-manifest.yaml
kubectl apply -f $HELM_ROOT/airflow/airflow-manifest.yaml
kubectl apply -f $HELM_ROOT/metabase/metabase-manifest.yaml

echo "✅ Hoàn tất triển khai vĩnh viễn!"

#!/bin/bash
set -e

# Tạo các namespace
kubectl create namespace lakehouse --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace ingestion --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace streaming --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# Thêm repos
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add trino https://trinodb.github.io/charts
helm repo add cetic https://cetic.github.io/helm-charts
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

HELM_ROOT="infrastructure/helm"

echo "🧹 Dọn dẹp các pods lỗi và dịch vụ cũ..."
kubectl get pods -A | grep -E "Error|CrashLoopBackOff|ImagePullBackOff|Evicted" | awk '{print "kubectl delete pod " $2 " -n " $1}' | bash || true

echo "📦 Triển khai các thành phần hạ tầng..."

# 0. Lakehouse Platform (Namespaces, Ingress)
helm upgrade --install platform $HELM_ROOT/lakehouse-platform -n lakehouse

# 1. Minio
helm upgrade --install minio bitnami/minio -n lakehouse -f $HELM_ROOT/minio/values.yaml

# 2. Kafka KRaft & Kafka UI
echo "Deploying Kafka KRaft and Kafka UI..."
helm upgrade --install kafka bitnami/kafka -n streaming -f $HELM_ROOT/kafka/values.yaml
kubectl apply -f $HELM_ROOT/kafka-ui/kafka-ui-manifest.yaml

# 3. NiFi 2.x (Zookeeper-less)
echo "Deploying NiFi 2.x..."
kubectl apply -f $HELM_ROOT/nifi/nifi-2-manifest.yaml

# 4. PostgreSQL & Hive Metastore
echo "Deploying PostgreSQL and Hive Metastore..."
helm upgrade --install hive-db bitnami/postgresql -n lakehouse -f $HELM_ROOT/postgres/values.yaml
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

echo "✅ Hoàn tất triển khai!"

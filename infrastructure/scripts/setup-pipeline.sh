#!/bin/bash
set -e

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

echo "🔄 Khởi động port-forward chạy ngầm cho Prometheus và Alertmanager..."
pkill -f "port-forward.*9090" || true
pkill -f "port-forward.*9093" || true
sleep 1

nohup kubectl port-forward -n monitoring svc/prometheus-server 9090:9090 --address 0.0.0.0 >/dev/null 2>&1 &
nohup kubectl port-forward -n monitoring svc/prometheus-alertmanager 9093:9093 --address 0.0.0.0 >/dev/null 2>&1 &
sleep 2

echo "✨ Đã kích hoạt cổng truy cập từ máy host:"
echo "📊 Prometheus UI: http://localhost:9090"
echo "🔔 Alertmanager UI: http://localhost:9093"

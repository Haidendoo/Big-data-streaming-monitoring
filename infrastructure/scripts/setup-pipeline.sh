#!/bin/bash
set -e

echo "🚀 Checking cluster status and waiting for pods to be fully ready..."

# Waiting for services to be ready
echo "⏳ Waiting for Trino, SFTP, NiFi, Flink, and Kafka UI pods..."
kubectl wait --namespace lakehouse --for=condition=ready pod -l app=trino --timeout=300s
kubectl wait --namespace ingestion --for=condition=ready pod -l app=sftp --timeout=300s
kubectl wait --namespace ingestion --for=condition=ready pod -l app=nifi --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l app=kafka-ui --timeout=300s
kubectl wait --namespace streaming --for=condition=ready pod -l component=jobmanager --timeout=300s

echo "✅ All pods are ready. Running setup-pipeline.py..."
chmod +x infrastructure/scripts/setup-pipeline.py
python3 infrastructure/scripts/setup-pipeline.py

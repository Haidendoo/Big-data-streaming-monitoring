#!/bin/bash
set -e

# Path configurations
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"
ENV_EXAMPLE="$PROJECT_ROOT/.env.example"
VALUES_FILE="$PROJECT_ROOT/infrastructure/helm/grafana/values.yaml"

echo "📧 Grafana Email Alerting Setup Helper"
echo "----------------------------------------"

# 1. Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "⚠️  File .env not found!"
    echo "📋 Copying .env.example to .env..."
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    echo "🚨 ACTION REQUIRED: Please edit '$ENV_FILE' and enter your SMTP details & recipient email, then re-run this script."
    exit 0
fi

echo "🔐 Creating/Updating Kubernetes Secret 'grafana-smtp-secret' in 'monitoring' namespace..."
kubectl create secret generic grafana-smtp-secret \
  --from-env-file="$ENV_FILE" \
  -n monitoring \
  --dry-run=client -o yaml | kubectl apply -f -

echo "⛵ Upgrading Grafana Helm deployment..."
helm upgrade --install grafana grafana/grafana \
  -n monitoring \
  -f "$VALUES_FILE"

echo "🔄 Restarting Grafana deployment to load updated Secret..."
kubectl rollout restart deployment/grafana -n monitoring

echo "⏳ Waiting for Grafana deployment rollout..."
kubectl rollout status deployment/grafana -n monitoring

echo "----------------------------------------"
echo "✅ Done! Grafana has been successfully updated."
echo "🔗 Access Grafana at: http://localhost:3000"
echo "👉 Check 'Alerting' -> 'Alert rules' and 'Contact points' to verify."

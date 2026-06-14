#!/bin/bash
set -e

echo "🚀 Bắt đầu quá trình thiết lập Server Monitoring Lakehouse..."

# 1. Cài đặt các công cụ cần thiết (nếu chưa có)
if ! command -v kubectl &> /dev/null; then
    echo "Đang cài đặt kubectl..."
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
    rm kubectl
fi

if ! command -v k3d &> /dev/null; then
    echo "Đang cài đặt k3d..."
    curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
fi

if ! command -v helm &> /dev/null; then
    echo "Đang cài đặt Helm..."
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

# 2. Tạo cluster k3d
echo "🏗️ Đang tạo cluster k3d..."
bash infrastructure/k3d/create-cluster.sh

# 3. Triển khai các dịch vụ
echo "🚢 Đang triển khai các dịch vụ Lakehouse..."
bash infrastructure/scripts/deploy-all.sh

echo "✅ Hoàn tất thiết lập cơ bản!"

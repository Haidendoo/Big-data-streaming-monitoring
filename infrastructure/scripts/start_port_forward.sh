#!/bin/bash

# Kill any existing port-forward processes first
pkill -f "port-forward" || true
sleep 1

echo "Starting auto-reconnecting port-forwarding..."

# Function to run port-forward in an auto-retry loop
run_port_forward() {
    local namespace=$1
    local resource=$2
    local ports=$3
    local address=$4
    
    while true; do
        # Check if the cluster is reachable before trying to connect
        if kubectl get ns >/dev/null 2>&1; then
            kubectl port-forward -n "$namespace" "$resource" "$ports" --address "$address" >/dev/null 2>&1
        fi
        sleep 2
    done
}

# Launch port forwards in background loops
run_port_forward "monitoring" "svc/prometheus-server" "9090:9090" "0.0.0.0" &
run_port_forward "monitoring" "svc/prometheus-alertmanager" "9093:9093" "0.0.0.0" &
run_port_forward "lakehouse" "svc/dim-data-postgresql" "5433:5432" "0.0.0.0" &
run_port_forward "orchestration" "svc/spark-ui" "7070:7070" "0.0.0.0" &
run_port_forward "orchestration" "svc/airflow" "8082:8082" "0.0.0.0" &
run_port_forward "orchestration" "svc/metabase" "3001:3001" "0.0.0.0" &

echo "All port-forward wrappers launched in background."

# Keep the parent script running
while true; do
    sleep 10
done

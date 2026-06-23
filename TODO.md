
## 1️⃣ What “cluster” really means in our context

| Component | Current (single‑replica) | Desired HA/cluster mode (per `DETAILED_REQUIREMENTS.md`) |
| :--- | :--- | :--- |
| **Kafka** | `StatefulSet` with `replicas: 1` | `replicas: 3` ($\ge$ #partitions) + Zookeeper (or KRaft) quorum |
| **Flink** | Helm chart with `jobmanager.replicas=1`, `taskmanager.replicas=1` | `jobmanager.replicas=2` (active‑standby) + `taskmanager.replicas` $\ge$ 2 (more slots) |
| **MinIO** | Stand‑alone pod | Distributed mode – multiple MinIO pods sharing a shared PV (e.g., 4‑node) |
| **Trino** | Single coordinator & worker | `coordinator.replicas=2` (HA) + `worker.replicas` $\ge$ 2 |
| **Prometheus**| None (scrapes only for demo) | Deploy as a Helm chart (single‑replica is fine) |
| **Alertmanager**| None | Deploy alongside Prometheus |

> 💡 **Why?** The `detailed-requirements` doc states *"Distributed (1 Master node, 3 Worker nodes)"* and *"Use Helm Chart to deploy components with resource requests/limits."* Scaling the `StatefulSets` matches that architecture while keeping the same Helm‑based CI/CD workflow.

---

### Minimal file edits

| File | Change |
| :--- | :--- |
| `infrastructure/helm/kafka/values.yaml` | Set `replicas: 3`, configure `zookeeper.replicaCount: 3` (or enable KRaft) |
| `infrastructure/helm/flink/values.yaml` | `jobmanager.replicas: 2`, `taskmanager.replicas: 2`, increase `taskmanager.taskSlots` if needed |
| `infrastructure/helm/minio/values.yaml` | `mode: distributed`, `statefulset.replicaCount: 4` |
| `infrastructure/helm/trino/values.yaml` | `coordinator.replicas: 2`, `worker.replicas: 2` |
| `infrastructure/helm/prometheus/values.yaml` &<br>`alertmanager/values.yaml` | Add standard alertmanager config (see below) |

*Note: All of those are YAML value overrides — no Java/Scala code changes.*

---

## 2️⃣ Batch Layer (Airflow + Spark)

### 2.1 Airflow deployment

* **Add a Helm chart** (`airflow/`) with a `values.yaml` that defines a `scheduler`, `webserver`, and a `worker` pool.
* **Create a DAG** (`batch_kpi_dag.py`) under `infrastructure/airflow/dags/` that runs hourly:
  1. Read the raw Iceberg table `monitoring.server_metrics` via Spark (Spark‑Iceberg connector).
  2. Join with `monitoring.server_config` (dimension).
  3. Aggregate per region/hour $\rightarrow$ write to `monitoring.kpi_summary` (Gold layer).

#### Sample DAG skeleton (`airflow/dags/batch_kpi_dag.py`)





File written successfully.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "vdt",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_kpi",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="kpi_aggregation",
        application="/opt/spark/jobs/kpi_agg.py",
        conn_id="spark_default",
        conf={"spark.master": "k8s://[https://kubernetes.default.svc](https://kubernetes.default.svc)"},
        packages="org.apache.iceberg:iceberg-spark3-runtime:1.5.0",
    )

```

> 📂 The Spark job (`kpi_agg.py`) can be stored under `infrastructure/spark/` and built into a Docker image that the Spark operator uses.

### 2.2 Iceberg compaction (midnight)

* Add a second Airflow DAG (`iceberg_compaction_dag.py`) that runs at `0 0 * * *` and executes a Trino query:

```sql
CALL system.rewrite_data_files('monitoring.server_metrics')
  WITH (format='parquet', strategy='binpack');

```

*Alternatively, use the `iceberg-compact` Helm chart that runs a Kubernetes CronJob.*

---

## 3️⃣ Alerting Layer (Prometheus + Alertmanager)

### 3.1 Prometheus scrape config

Add to `prometheus/values.yaml`:

```yaml
serverFiles:
  prometheus.yml:
    scrape_configs:
      - job_name: "flink"
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            action: keep
            regex: flink
      - job_name: "kafka"
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            action: keep
            regex: kafka
      - job_name: "trino"
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            action: keep
            regex: trino

```

### 3.2 Alertmanager rules (example)

```yaml
groups:
  - name: lakehouse.rules
    rules:
      - alert: FlinkJobFailed
        expr: flink_job_status{status!="RUNNING"} > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Flink job {{ $labels.job_name }} stopped"
          description: "Flink job has not been RUNNING for the last 5 minutes."

      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_topic_partition_under_replicated > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Kafka partitions under-replicated"
          description: "One or more Kafka partitions have fewer replicas than expected."

      - alert: TrinoQueryErrorRate
        expr: rate(trino_query_errors_total[5m]) > 0.1
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "High Trino query error rate"
          description: "Trino is returning errors for >10% of queries in the last 5 minutes."

```

> 🔔 Alertmanager can then route these alerts to Slack/Discord/Telegram/Email via webhook URLs (set in `alertmanager/values.yaml`).

---

## 4️⃣ Do I need to touch the existing code?

**No.** All the above changes are configuration‑only:

* Batch Spark job reads the same Iceberg table your Flink job writes to – no schema changes.
* Compaction is a Trino stored procedure, also schema‑agnostic.
* Alerting only observes metrics that are already exported by the Helm charts (Flink, Kafka, Trino).

### New Pieces to Be Added:

| Piece | Location | Reason |
| --- | --- | --- |
| **`airflow/`** helm chart & DAGs | `infrastructure/airflow/` | New batch orchestrator |
| **`spark/`** Docker image & job script | `infrastructure/spark/` | Batch KPI aggregation |
| **`prometheus/`** & **`alertmanager/`** helm charts | `infrastructure/prometheus/` &<br>

<br>`infrastructure/alertmanager/` | Monitoring & alerts |
| **Updated value files** for Kafka/Flink/MinIO/Trino | `infrastructure/helm/*/values.yaml` | Switch to multi‑replica clusters |

---

## 5️⃣ Quick “what‑to‑edit” checklist

1. **Modify Helm Values**: Open the Helm values (e.g., `infrastructure/helm/kafka/values.yaml`) and change `replicas` to the numbers defined in Section 1.
2. **Deploy Airflow**: Add the Airflow chart and install it:
```bash
helm repo add apache-airflow [https://airflow.apache.org](https://airflow.apache.org)
helm upgrade --install airflow ./infrastructure/helm/airflow \\
    -f ./infrastructure/helm/airflow/values.yaml

```


3. **Build Spark Image**: Create the Spark Docker image and push to your local registry:
```bash
cd infrastructure/spark/
docker build -t vdt/spark-kpi:latest .
docker tag vdt/spark-kpi:latest k3d-vdt-registry:5000/vdt/spark-kpi:latest
docker push k3d-vdt-registry:5000/vdt/spark-kpi:latest

```


4. **Deploy Prometheus & Alertmanager**:
```bash
helm upgrade --install prometheus ./infrastructure/helm/prometheus \\
    -f ./infrastructure/helm/prometheus/values.yaml
helm upgrade --install alertmanager ./infrastructure/helm/alertmanager \\
    -f ./infrastructure/helm/alertmanager/values.yaml

```


5. **Validate Setup**:
* Run `kubectl get pods -n streaming` – all components should show the new replica counts.
* Open Grafana $\rightarrow$ the KPI Summary table (new Iceberg table) will appear after the first batch run.
* Trigger a failing Flink job or a Trino error to see alerts fire in your Slack/Webhook channel.



---

## 6️⃣ Where to find the reference docs

* **Detailed infra spec** – `DETAILED_REQUIREMENTS.md`
* **Current Helm values** (Kafka, Flink, MinIO, Trino) – under `infrastructure/helm/` (list with `ls`).
* **Batch DAG examples** – place new files under `infrastructure/airflow/dags/`.
* **Prometheus/Alertmanager helm charts** – see the official Helm repos or existing examples in `infrastructure/helm/`.

---

### Next Steps for You

* [ ] Edit the Helm value files as per the table above.
* [ ] Add the Airflow & Spark resources (create the directories/files, build the Docker image).
* [ ] Deploy Prometheus + Alertmanager with the provided rule snippets.
* [ ] Run `helm upgrade ...` for each component and verify the pods scale correctly.

Once those steps are done, the batch pipeline will run hourly, Iceberg tables will be compacted nightly, and alerts will keep you informed of any failures — exactly matching the cluster‑mode vision in `DETAILED_REQUIREMENTS.md`.
"""

with open("cluster_migration_guide.md", "w", encoding="utf-8") as f:
f.write(markdown_content)

print("File written successfully.")

```
File `.md` của bạn đã được khởi tạo và cấu trúc lại toàn bộ nội dung một cách hoàn chỉnh, đẹp mắt và đúng chuẩn cú pháp Markdown.

Your MD file is ready
[file-tag: code-generated-file-0-1782205018933345253]

Bạn có thể tải file về từ link trên hoặc sao chép nhanh nội dung thô (Raw Code) bên dưới để lưu vào máy:

```
# Cluster Migration & Infrastructure Update Guide

## 1️⃣ What “cluster” really means in our context

| Component | Current (single‑replica) | Desired HA/cluster mode (per `DETAILED_REQUIREMENTS.md`) |
| :--- | :--- | :--- |
| **Kafka** | `StatefulSet` with `replicas: 1` | `replicas: 3` ($\ge$ #partitions) + Zookeeper (or KRaft) quorum |
| **Flink** | Helm chart with `jobmanager.replicas=1`, `taskmanager.replicas=1` | `jobmanager.replicas=2` (active‑standby) + `taskmanager.replicas` $\ge$ 2 (more slots) |
| **MinIO** | Stand‑alone pod | Distributed mode – multiple MinIO pods sharing a shared PV (e.g., 4‑node) |
| **Trino** | Single coordinator & worker | `coordinator.replicas=2` (HA) + `worker.replicas` $\ge$ 2 |
| **Prometheus**| None (scrapes only for demo) | Deploy as a Helm chart (single‑replica is fine) |
| **Alertmanager**| None | Deploy alongside Prometheus |

> 💡 **Why?** The `detailed-requirements` doc states *"Distributed (1 Master node, 3 Worker nodes)"* and *"Use Helm Chart to deploy components with resource requests/limits."* Scaling the `StatefulSets` matches that architecture while keeping the same Helm‑based CI/CD workflow.

---

### Minimal file edits

| File | Change |
| :--- | :--- |
| `infrastructure/helm/kafka/values.yaml` | Set `replicas: 3`, configure `zookeeper.replicaCount: 3` (or enable KRaft) |
| `infrastructure/helm/flink/values.yaml` | `jobmanager.replicas: 2`, `taskmanager.replicas: 2`, increase `taskmanager.taskSlots` if needed |
| `infrastructure/helm/minio/values.yaml` | `mode: distributed`, `statefulset.replicaCount: 4` |
| `infrastructure/helm/trino/values.yaml` | `coordinator.replicas: 2`, `worker.replicas: 2` |
| `infrastructure/helm/prometheus/values.yaml` &<br>`alertmanager/values.yaml` | Add standard alertmanager config (see below) |

*Note: All of those are YAML value overrides — no Java/Scala code changes.*

---

## 2️⃣ Batch Layer (Airflow + Spark)

### 2.1 Airflow deployment

* **Add a Helm chart** (`airflow/`) with a `values.yaml` that defines a `scheduler`, `webserver`, and a `worker` pool.
* **Create a DAG** (`batch_kpi_dag.py`) under `infrastructure/airflow/dags/` that runs hourly:
  1. Read the raw Iceberg table `monitoring.server_metrics` via Spark (Spark‑Iceberg connector).
  2. Join with `monitoring.server_config` (dimension).
  3. Aggregate per region/hour $\rightarrow$ write to `monitoring.kpi_summary` (Gold layer).

#### Sample DAG skeleton (`airflow/dags/batch_kpi_dag.py`)

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "vdt",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_kpi",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="kpi_aggregation",
        application="/opt/spark/jobs/kpi_agg.py",
        conn_id="spark_default",
        conf={"spark.master": "k8s://[https://kubernetes.default.svc](https://kubernetes.default.svc)"},
        packages="org.apache.iceberg:iceberg-spark3-runtime:1.5.0",
    )

```

> 📂 The Spark job (`kpi_agg.py`) can be stored under `infrastructure/spark/` and built into a Docker image that the Spark operator uses.

### 2.2 Iceberg compaction (midnight)

* Add a second Airflow DAG (`iceberg_compaction_dag.py`) that runs at `0 0 * * *` and executes a Trino query:

```sql
CALL system.rewrite_data_files('monitoring.server_metrics')
  WITH (format='parquet', strategy='binpack');

```

*Alternatively, use the `iceberg-compact` Helm chart that runs a Kubernetes CronJob.*

---

## 3️⃣ Alerting Layer (Prometheus + Alertmanager)

### 3.1 Prometheus scrape config

Add to `prometheus/values.yaml`:

```yaml
serverFiles:
  prometheus.yml:
    scrape_configs:
      - job_name: "flink"
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            action: keep
            regex: flink
      - job_name: "kafka"
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            action: keep
            regex: kafka
      - job_name: "trino"
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            action: keep
            regex: trino

```

### 3.2 Alertmanager rules (example)

```yaml
groups:
  - name: lakehouse.rules
    rules:
      - alert: FlinkJobFailed
        expr: flink_job_status{status!="RUNNING"} > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Flink job {{ $labels.job_name }} stopped"
          description: "Flink job has not been RUNNING for the last 5 minutes."

      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_topic_partition_under_replicated > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Kafka partitions under-replicated"
          description: "One or more Kafka partitions have fewer replicas than expected."

      - alert: TrinoQueryErrorRate
        expr: rate(trino_query_errors_total[5m]) > 0.1
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "High Trino query error rate"
          description: "Trino is returning errors for >10% of queries in the last 5 minutes."

```

> 🔔 Alertmanager can then route these alerts to Slack/Discord/Telegram/Email via webhook URLs (set in `alertmanager/values.yaml`).

---

## 4️⃣ Do I need to touch the existing code?

**No.** All the above changes are configuration‑only:

* Batch Spark job reads the same Iceberg table your Flink job writes to – no schema changes.
* Compaction is a Trino stored procedure, also schema‑agnostic.
* Alerting only observes metrics that are already exported by the Helm charts (Flink, Kafka, Trino).

### New Pieces to Be Added:

| Piece | Location | Reason |
| --- | --- | --- |
| **`airflow/`** helm chart & DAGs | `infrastructure/airflow/` | New batch orchestrator |
| **`spark/`** Docker image & job script | `infrastructure/spark/` | Batch KPI aggregation |
| **`prometheus/`** & **`alertmanager/`** helm charts | `infrastructure/prometheus/` &<br>

<br>`infrastructure/alertmanager/` | Monitoring & alerts |
| **Updated value files** for Kafka/Flink/MinIO/Trino | `infrastructure/helm/*/values.yaml` | Switch to multi‑replica clusters |

---

## 5️⃣ Quick “what‑to‑edit” checklist

1. **Modify Helm Values**: Open the Helm values (e.g., `infrastructure/helm/kafka/values.yaml`) and change `replicas` to the numbers defined in Section 1.
2. **Deploy Airflow**: Add the Airflow chart and install it:
```bash
helm repo add apache-airflow [https://airflow.apache.org](https://airflow.apache.org)
helm upgrade --install airflow ./infrastructure/helm/airflow \
    -f ./infrastructure/helm/airflow/values.yaml

```


3. **Build Spark Image**: Create the Spark Docker image and push to your local registry:
```bash
cd infrastructure/spark/
docker build -t vdt/spark-kpi:latest .
docker tag vdt/spark-kpi:latest k3d-vdt-registry:5000/vdt/spark-kpi:latest
docker push k3d-vdt-registry:5000/vdt/spark-kpi:latest

```


4. **Deploy Prometheus & Alertmanager**:
```bash
helm upgrade --install prometheus ./infrastructure/helm/prometheus \
    -f ./infrastructure/helm/prometheus/values.yaml
helm upgrade --install alertmanager ./infrastructure/helm/alertmanager \
    -f ./infrastructure/helm/alertmanager/values.yaml

```


5. **Validate Setup**:
* Run `kubectl get pods -n streaming` – all components should show the new replica counts.
* Open Grafana $\rightarrow$ the KPI Summary table (new Iceberg table) will appear after the first batch run.
* Trigger a failing Flink job or a Trino error to see alerts fire in your Slack/Webhook channel.



---

## 6️⃣ Where to find the reference docs

* **Detailed infra spec** – `DETAILED_REQUIREMENTS.md`
* **Current Helm values** (Kafka, Flink, MinIO, Trino) – under `infrastructure/helm/` (list with `ls`).
* **Batch DAG examples** – place new files under `infrastructure/airflow/dags/`.
* **Prometheus/Alertmanager helm charts** – see the official Helm repos or existing examples in `infrastructure/helm/`.

---

### Next Steps for You

* [ ] Edit the Helm value files as per the table above.
* [ ] Add the Airflow & Spark resources (create the directories/files, build the Docker image).
* [ ] Deploy Prometheus + Alertmanager with the provided rule snippets.
* [ ] Run `helm upgrade ...` for each component and verify the pods scale correctly.

Once those steps are done, the batch pipeline will run hourly, Iceberg tables will be compacted nightly, and alerts will keep you informed of any failures — exactly matching the cluster‑mode vision in `DETAILED_REQUIREMENTS.md`.

```

```
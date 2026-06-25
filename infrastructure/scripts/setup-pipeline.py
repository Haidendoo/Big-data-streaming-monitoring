#!/usr/bin/env python3
import sys
import os
import time
import subprocess
import requests

TRINO_URL = "http://localhost:8888/v1/statement"

def run_trino(sql):
    headers = {"X-Trino-User": "admin", "Content-Type": "text/plain"}
    try:
        r = requests.post(TRINO_URL, data=sql, headers=headers)
    except Exception as e:
        print(f"❌ Failed to connect to Trino at {TRINO_URL}: {e}")
        return False

    if r.status_code != 200:
        print(f"❌ Trino returned error code {r.status_code}: {r.text}")
        return False
        
    res = r.json()
    next_uri = res.get("nextUri")
    
    while next_uri:
        try:
            r_poll = requests.get(next_uri, headers=headers)
        except Exception as e:
            print(f"❌ Trino polling failed: {e}")
            return False
            
        if r_poll.status_code != 200:
            print(f"❌ Trino polling returned error code {r_poll.status_code}")
            return False
            
        res = r_poll.json()
        next_uri = res.get("nextUri")
        stats = res.get("stats", {})
        state = stats.get("state")
        
        if state == "FAILED":
            error = res.get("error", {})
            print(f"❌ Trino query failed: {error.get('message')}")
            return False
        elif state in ["FINISHED", "CANCELED"]:
            break
        time.sleep(0.1)
    return True

def setup_trino_tables():
    print("📊 Initializing Iceberg Tables & Views in Trino...")
    
    # 1. Clean old schema/tables
    cleanup_statements = [
        "DROP VIEW IF EXISTS iceberg.monitoring.server_metrics",
        "DROP TABLE IF EXISTS iceberg.monitoring.kpi_summary",
        "DROP TABLE IF EXISTS iceberg.monitoring.server_config",
        "DROP TABLE IF EXISTS iceberg.monitoring.raw_sftp_table",
        "DROP SCHEMA IF EXISTS iceberg.monitoring CASCADE"
    ]
    for stmt in cleanup_statements:
        print(f"   🧹 {stmt}...")
        run_trino(stmt)
        
    # 2. Run monitoring.sql DDL
    sql_file = "catalogs/iceberg/monitoring.sql"
    if not os.path.exists(sql_file):
        print(f"❌ Error: {sql_file} not found!")
        sys.exit(1)
        
    with open(sql_file, "r") as f:
        ddl_sql = f.read()
        
    for statement in ddl_sql.split(";"):
        stmt = statement.strip()
        if stmt:
            print(f"   🔨 Executing DDL...")
            if not run_trino(stmt):
                print("❌ Failed to create tables!")
                sys.exit(1)
                
    # 3. Create server_metrics view
    view_sql = """
    CREATE VIEW iceberg.monitoring.server_metrics AS
    SELECT r.ts, c.server_name, c.ip, r.cpu_util, r.ram_util, r.disk_util, r.io_stat
    FROM iceberg.monitoring.raw_sftp_table r
    JOIN iceberg.monitoring.server_config c ON r.server_id = c.server_id
    """
    print("   👁️ Creating View server_metrics...")
    if not run_trino(view_sql):
         print("❌ Failed to create view!")
         sys.exit(1)

    # 4. Seed server_config
    seed_sql = """
    INSERT INTO iceberg.monitoring.server_config (server_id, server_name, ip, province, station) VALUES
    (101, 'prod-web-01', '192.168.1.10', 'TPHCM', 'Tram Quan 1'),
    (102, 'prod-web-02', '192.168.1.11', 'TPHCM', 'Tram Quan 1'),
    (103, 'prod-db-01', '192.168.1.20', 'Ha Noi', 'Tram Cau Giay'),
    (104, 'prod-db-02', '192.168.1.21', 'Ha Noi', 'Tram Cau Giay'),
    (105, 'stage-app-01', '172.16.5.10', 'Da Nang', 'Tram Hai Chau')
    """
    print("   🌱 Seeding server_config Dim data...")
    if not run_trino(seed_sql):
         print("❌ Failed to seed configuration data!")
         sys.exit(1)
         
    print("✅ Trino schema & tables setup completed successfully!")

def configure_nifi():
    print("⚙️ Running NiFi Configuration...")
    try:
        subprocess.check_call([sys.executable, "nifi/configure_nifi.py"])
    except Exception as e:
        print(f"❌ Failed to run NiFi configuration script: {e}")
        sys.exit(1)

def compile_and_deploy_flink():
    print("☕ Compiling Flink project...")
    try:
        # Run maven build
        subprocess.check_call(["mvn", "clean", "package"], cwd="flink")
    except Exception as e:
        print(f"❌ Maven build failed: {e}")
        sys.exit(1)
        
    jar_file = "flink/target/server-monitoring-flink-1.0-SNAPSHOT.jar"
    if not os.path.exists(jar_file):
        print(f"❌ Error: Compiled Flink JAR not found at {jar_file}")
        sys.exit(1)
        
    # Cancel any existing Flink jobs first
    print("🧹 Checking for running Flink jobs to cancel...")
    try:
        jobs_json = requests.get("http://localhost:8081/jobs/overview").json()
        for j in jobs_json.get("jobs", []):
            jid = j["jid"]
            if j["state"] == "RUNNING":
                print(f"   🛑 Canceling running job {jid}...")
                requests.patch(f"http://localhost:8081/jobs/{jid}?mode=cancel")
                time.sleep(2)
    except Exception as e:
        print(f"   (No active jobs found or Flink API was not reachable: {e})")

    # Upload JAR via REST API
    print("📤 Uploading JAR to Flink JobManager...")
    upload_url = "http://localhost:8081/jars/upload"
    try:
        with open(jar_file, 'rb') as f:
            files = {'jarfile': (os.path.basename(jar_file), f, 'application/java-archive')}
            r = requests.post(upload_url, files=files)
        if r.status_code != 200:
            print(f"❌ Failed to upload JAR: {r.status_code} - {r.text}")
            sys.exit(1)
        res = r.json()
        filename = res.get("filename")
        if not filename:
            print(f"❌ Upload failed: {res}")
            sys.exit(1)
        jar_id = os.path.basename(filename)
        print(f"   JAR uploaded successfully. Jar ID: {jar_id}")
    except Exception as e:
        print(f"❌ Exception uploading jar: {e}")
        sys.exit(1)

    # Run Flink Job via REST API
    print("🚀 Submitting Flink Job...")
    run_url = f"http://localhost:8081/jars/{jar_id}/run"
    run_payload = {
        "flinkConfiguration": {
            "pipeline.classpaths": "file:///opt/flink/usrlib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar;file:///opt/flink/usrlib/flink-sql-connector-hive-3.1.3_2.12-1.18.1.jar"
        }
    }
    try:
        r_run = requests.post(run_url, json=run_payload)
        if r_run.status_code != 200:
            print(f"❌ Failed to submit Flink job: {r_run.status_code} - {r_run.text}")
            sys.exit(1)
        run_res = r_run.json()
        print(f"✅ Flink job submitted successfully! Job ID: {run_res.get('jobid')}")
    except Exception as e:
        print(f"❌ Exception submitting Flink job: {e}")
        sys.exit(1)

def create_kafka_topic():
    print("📢 Checking and creating Kafka topic 'file-arrival-events'...")
    try:
        # Check if topic already exists
        check_cmd = "kubectl exec -n streaming kafka-0 -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
        topics = subprocess.check_output(check_cmd, shell=True).decode().strip().split('\n')
        if "file-arrival-events" in [t.strip() for t in topics if t.strip()]:
            print("   Topic 'file-arrival-events' already exists.")
            return

        # Create topic
        create_cmd = (
            "kubectl exec -n streaming kafka-0 -- /opt/kafka/bin/kafka-topics.sh "
            "--bootstrap-server localhost:9092 --create --topic file-arrival-events "
            "--partitions 3 --replication-factor 1"
        )
        subprocess.check_call(create_cmd, shell=True)
        print("   ✅ Topic 'file-arrival-events' created successfully.")
    except Exception as e:
        print(f"   ⚠️ Could not verify/create Kafka topic: {e} (it might be auto-created later)")

def configure_minio():
    print("🪣 Configuring MinIO bucket and lifecycle retention...")
    try:
        # Get the minio pod name in lakehouse namespace
        pod_cmd = "kubectl get pods -n lakehouse -l app=minio -o jsonpath='{.items[0].metadata.name}'"
        minio_pod = subprocess.check_output(pod_cmd, shell=True).decode().strip()
        
        if not minio_pod:
            print("❌ MinIO pod not found!")
            sys.exit(1)
            
        print(f"   Found MinIO pod: {minio_pod}")
        
        # Configure mc alias
        alias_cmd = f"kubectl exec -n lakehouse {minio_pod} -- mc alias set local http://localhost:9000 admin password123"
        subprocess.check_call(alias_cmd, shell=True)
        
        # Create bucket 'lakehouse' if not exists
        mb_cmd = f"kubectl exec -n lakehouse {minio_pod} -- /bin/sh -c 'mc ls local/lakehouse >/dev/null 2>&1 || mc mb local/lakehouse'"
        subprocess.check_call(mb_cmd, shell=True)
        
        # Check if ilm rule already exists
        check_ilm_cmd = f"kubectl exec -n lakehouse {minio_pod} -- mc ilm rule list local/lakehouse"
        rules_out = subprocess.check_output(check_ilm_cmd, shell=True, stderr=subprocess.DEVNULL).decode().strip()
        
        if "raw-file/" not in rules_out:
            print("   Adding 24h expiration rule to 'raw-file/' prefix...")
            rule_cmd = f"kubectl exec -n lakehouse {minio_pod} -- mc ilm rule add --prefix \"raw-file/\" --expire-days 1 local/lakehouse"
            subprocess.check_call(rule_cmd, shell=True)
        else:
            print("   Lifecycle rule for 'raw-file/' already exists.")
            
        print("   ✅ MinIO bucket and lifecycle retention configured successfully.")
    except Exception as e:
        print(f"   ❌ Failed to configure MinIO bucket/lifecycle: {e}")
        sys.exit(1)

def main():
    setup_trino_tables()
    create_kafka_topic()
    configure_minio()
    configure_nifi()
    compile_and_deploy_flink()
    print("\n🎉 ALL PIPELINE COMPONENTS SUCCESSFULLY CONFIGURED AND RUNNING!")

if __name__ == "__main__":
    main()

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
        "DROP TABLE IF EXISTS iceberg.monitoring.raw_sftp_table"
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
        
    # Get JobManager pod name
    print("🔍 Locating Flink JobManager pod...")
    try:
        cmd = "kubectl get pods -n streaming -l component=jobmanager -o jsonpath='{.items[0].metadata.name}'"
        pod_name = subprocess.check_output(cmd, shell=True).decode().strip()
    except Exception as e:
        print(f"❌ Failed to find JobManager pod name: {e}")
        sys.exit(1)
        
    print(f"   Found JobManager pod: {pod_name}")
    
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

    # Copy JAR to JobManager
    print(f"📤 Copying JAR to JobManager container...")
    try:
        subprocess.check_call(["kubectl", "cp", jar_file, f"streaming/{pod_name}:/tmp/job.jar"])
    except Exception as e:
        print(f"❌ Failed to copy jar: {e}")
        sys.exit(1)
        
    # Run Flink Job
    print("🚀 Submitting Flink Job...")
    run_cmd = (
        f"kubectl exec -n streaming {pod_name} -- flink run -d "
        f"-C file:///opt/flink/usrlib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar "
        f"-C file:///opt/flink/usrlib/flink-sql-connector-hive-3.1.3_2.12-1.18.1.jar "
        f"/tmp/job.jar"
    )
    try:
        subprocess.check_call(run_cmd, shell=True)
        print("✅ Flink job submitted successfully!")
    except Exception as e:
        print(f"❌ Failed to submit Flink job: {e}")
        sys.exit(1)

def main():
    setup_trino_tables()
    configure_nifi()
    compile_and_deploy_flink()
    print("\n🎉 ALL PIPELINE COMPONENTS SUCCESSFULLY CONFIGURED AND RUNNING!")

if __name__ == "__main__":
    main()

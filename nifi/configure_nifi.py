#!/usr/bin/env python3
import time
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_BASE = "https://localhost:8443/nifi-api"
USERNAME = "admin"
PASSWORD = "password123456"

# Session with SSL verify disabled (NiFi uses self-signed certificates by default)
session = requests.Session()
session.verify = False

def get_token():
    print("🔑 Authenticating with NiFi...")
    url = f"{NIFI_BASE}/access/token"
    r = session.post(url, data={"username": USERNAME, "password": PASSWORD})
    if r.status_code == 201:
        token = r.text
        session.headers.update({"Authorization": f"Bearer {token}"})
        print("✅ Authentication successful!")
        return token
    else:
        raise Exception(f"Failed to authenticate: {r.status_code} - {r.text}")

def get_root_pg():
    url = f"{NIFI_BASE}/process-groups/root"
    r = session.get(url)
    if r.status_code == 200:
        root_pg_id = r.json()["id"]
        print(f"📦 Root Process Group ID: {root_pg_id}")
        return root_pg_id
    else:
        raise Exception(f"Failed to get root process group: {r.status_code} - {r.text}")

def clear_root_pg(root_id):
    print("🧹 Cleaning existing components in Root Process Group...")
    
    # 1. Stop all processors
    url = f"{NIFI_BASE}/process-groups/{root_id}/processors"
    r = session.get(url)
    processors = r.json().get("processors", [])
    for p in processors:
        pid = p["id"]
        pname = p["component"]["name"]
        version = p["revision"]["version"]
        state = p["component"]["state"]
        if state == "RUNNING":
            print(f"   🛑 Stopping processor {pname}...")
            stop_payload = {
                "component": {"id": pid, "state": "STOPPED"},
                "revision": {"version": version}
            }
            session.put(f"{NIFI_BASE}/processors/{pid}", json=stop_payload)
            # Small delay to let it stop
            time.sleep(0.5)

    # 2. Empty all connection queues
    url = f"{NIFI_BASE}/process-groups/{root_id}/connections"
    r = session.get(url)
    connections = r.json().get("connections", [])
    for c in connections:
        cid = c["id"]
        drop_url = f"{NIFI_BASE}/flowfile-queues/{cid}/drop-requests"
        r_drop = session.post(drop_url)
        if r_drop.status_code == 202:
            print(f"   🧹 Emptying connection queue {cid}...")
            drop_req_id = r_drop.json()["dropRequest"]["id"]
            for _ in range(5):
                time.sleep(0.5)
                r_poll = session.get(f"{drop_url}/{drop_req_id}")
                if r_poll.json()["dropRequest"]["finished"]:
                    break

    # 3. Delete all connections
    r = session.get(url)
    connections = r.json().get("connections", [])
    for c in connections:
        cid = c["id"]
        version = c["revision"]["version"]
        print(f"   🔗 Deleting connection {cid}...")
        session.delete(f"{NIFI_BASE}/connections/{cid}?version={version}")

    # 4. Delete all processors
    r = session.get(f"{NIFI_BASE}/process-groups/{root_id}/processors")
    processors = r.json().get("processors", [])
    for p in processors:
        pid = p["id"]
        pname = p["component"]["name"]
        version = p["revision"]["version"]
        print(f"   🗑️ Deleting processor {pname}...")
        session.delete(f"{NIFI_BASE}/processors/{pid}?version={version}")

    # 4. Disable and delete all controller services
    url = f"{NIFI_BASE}/flow/process-groups/{root_id}/controller-services"
    r = session.get(url)
    services = r.json().get("controllerServices", [])
    for s in services:
        sid = s["id"]
        sname = s["component"]["name"]
        version = s["revision"]["version"]
        state = s["component"]["state"]
        if state == "ENABLED":
            print(f"   🔌 Disabling controller service {sname}...")
            disable_payload = {
                "state": "DISABLED",
                "revision": {"version": version}
            }
            session.put(f"{NIFI_BASE}/controller-services/{sid}/run-status", json=disable_payload)
            time.sleep(0.5)
            # Refetch version
            refetch = session.get(f"{NIFI_BASE}/controller-services/{sid}").json()
            version = refetch["revision"]["version"]
            
        print(f"   🗑️ Deleting controller service {sname}...")
        session.delete(f"{NIFI_BASE}/controller-services/{sid}?version={version}")

    print("✅ Root Process Group is now clean!")

def create_controller_service(root_id, name, service_type, properties):
    print(f"➕ Creating Controller Service: {name} ({service_type})...")
    url = f"{NIFI_BASE}/process-groups/{root_id}/controller-services"
    payload = {
        "component": {
            "type": service_type,
            "name": name,
            "properties": properties
        },
        "revision": {"version": 0}
    }
    r = session.post(url, json=payload)
    if r.status_code == 201:
        service_data = r.json()
        print(f"   ✅ Created: {name} ID: {service_data['id']}")
        return service_data["id"]
    else:
        raise Exception(f"Failed to create controller service {name}: {r.status_code} - {r.text}")

def enable_controller_service(service_id, name):
    print(f"🔌 Enabling Controller Service: {name}...")
    # Get current version
    url = f"{NIFI_BASE}/controller-services/{service_id}"
    service_data = session.get(url).json()
    version = service_data["revision"]["version"]
    
    payload = {
        "state": "ENABLED",
        "revision": {"version": version}
    }
    r = session.put(f"{NIFI_BASE}/controller-services/{service_id}/run-status", json=payload)
    if r.status_code == 200:
        print(f"   ✅ Enabled: {name}")
    else:
        raise Exception(f"Failed to enable controller service {name}: {r.status_code} - {r.text}")

def create_processor(root_id, name, proc_type, properties, auto_terminate_rels, x, y):
    print(f"➕ Creating Processor: {name}...")
    url = f"{NIFI_BASE}/process-groups/{root_id}/processors"
    payload = {
        "component": {
            "type": proc_type,
            "name": name,
            "position": {"x": x, "y": y},
            "config": {
                "properties": properties,
                "autoTerminatedRelationships": auto_terminate_rels
            }
        },
        "revision": {"version": 0}
    }
    r = session.post(url, json=payload)
    if r.status_code == 201:
        proc_data = r.json()
        print(f"   ✅ Created: {name} ID: {proc_data['id']}")
        return proc_data["id"]
    else:
        raise Exception(f"Failed to create processor {name}: {r.status_code} - {r.text}")

def create_connection(root_id, source_id, dest_id, relationships, connection_name=""):
    print(f"🔗 Connecting processors (Source: {source_id} -> Destination: {dest_id})...")
    url = f"{NIFI_BASE}/process-groups/{root_id}/connections"
    payload = {
        "component": {
            "name": connection_name,
            "source": {
                "id": source_id,
                "groupId": root_id,
                "type": "PROCESSOR"
            },
            "destination": {
                "id": dest_id,
                "groupId": root_id,
                "type": "PROCESSOR"
            },
            "selectedRelationships": relationships
        },
        "revision": {"version": 0}
    }
    r = session.post(url, json=payload)
    if r.status_code == 201:
        conn_data = r.json()
        print(f"   ✅ Created Connection: {conn_data['id']}")
        return conn_data["id"]
    else:
        raise Exception(f"Failed to create connection: {r.status_code} - {r.text}")

def start_processor(proc_id, name):
    print(f"🚀 Starting Processor: {name}...")
    url = f"{NIFI_BASE}/processors/{proc_id}"
    # Get version
    proc_data = session.get(url).json()
    version = proc_data["revision"]["version"]
    
    payload = {
        "component": {
            "id": proc_id,
            "state": "RUNNING"
        },
        "revision": {"version": version}
    }
    r = session.put(url, json=payload)
    if r.status_code == 200:
        print(f"   ✅ Started: {name}")
    else:
        raise Exception(f"Failed to start processor {name}: {r.status_code} - {r.text}")

def main():
    get_token()
    root_id = get_root_pg()
    
    # Clean workspace first for idempotency
    clear_root_pg(root_id)
    
    # 1. Create AWS Credentials Provider Service
    aws_creds_id = create_controller_service(
        root_id=root_id,
        name="MinioCredentialsService",
        service_type="org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService",
        properties={
            "Access Key": "admin",
            "Secret Key": "password123"
        }
    )
    
    # 2. Create Kafka Connection Service
    kafka_service_id = create_controller_service(
        root_id=root_id,
        name="KafkaConnectionService",
        service_type="org.apache.nifi.kafka.service.Kafka3ConnectionService",
        properties={
            "bootstrap.servers": "kafka.streaming.svc.cluster.local:9092",
            "security.protocol": "PLAINTEXT"
        }
    )
    
    # Enable the Controller Services
    enable_controller_service(aws_creds_id, "MinioCredentialsService")
    enable_controller_service(kafka_service_id, "KafkaConnectionService")
    
    # 3. Create ListSFTP Processor
    list_sftp_id = create_processor(
        root_id=root_id,
        name="ListSFTP-Metrics",
        proc_type="org.apache.nifi.processors.standard.ListSFTP",
        properties={
            "Hostname": "sftp",
            "Port": "22",
            "Username": "sftpuser",
            "Password": "password123",
            "Remote Path": "upload",
            "Search Recursively": "false",
            "File Filter Regex": "metrics_.*",
            "Strict Host Key Checking": "false"
        },
        auto_terminate_rels=[],
        x=200, y=100
    )
    
    # 4. Create FetchSFTP Processor
    fetch_sftp_id = create_processor(
        root_id=root_id,
        name="FetchSFTP-Metrics",
        proc_type="org.apache.nifi.processors.standard.FetchSFTP",
        properties={
            "Hostname": "sftp",
            "Port": "22",
            "Username": "sftpuser",
            "Password": "password123",
            "Remote File": "${path}/${filename}",
            "Completion Strategy": "Delete File",
            "Strict Host Key Checking": "false"
        },
        auto_terminate_rels=["comms.failure", "not.found", "permission.denied"],
        x=200, y=300
    )
    
    # 5. Create PutS3Object (Minio Landing) Processor
    put_s3_id = create_processor(
        root_id=root_id,
        name="PutMinio-Landing",
        proc_type="org.apache.nifi.processors.aws.s3.PutS3Object",
        properties={
            "Bucket": "lakehouse",
            "Object Key": "raw-file/${now():format('yyyy/MM/dd/HH')}/${filename}",
            "Region": "us-east-1",
            "AWS Credentials Provider service": aws_creds_id,
            "Endpoint Override URL": "http://minio.lakehouse.svc.cluster.local:9000",
            "use-path-style-access": "true"
        },
        auto_terminate_rels=["failure"],
        x=200, y=500
    )
    
    replace_val = """{
  "file_path": "s3a://lakehouse/raw-file/${now():format('yyyy/MM/dd/HH')}/${filename}",
  "file_name": "${filename}",
  "timestamp": "${now():format("yyyy-MM-dd'T'HH:mm:ss'Z'")}",
  "format": "${filename:substringAfterLast('.')}"
}"""
    replace_text_id = create_processor(
        root_id=root_id,
        name="Format-Kafka-Event",
        proc_type="org.apache.nifi.processors.standard.ReplaceText",
        properties={
            "Replacement Strategy": "Always Replace",
            "Replacement Value": replace_val,
            "Evaluation Mode": "Entire text"
        },
        auto_terminate_rels=["failure"],
        x=200, y=700
    )
    
    # 7. Create PublishKafka Processor
    publish_kafka_id = create_processor(
        root_id=root_id,
        name="PublishKafka-Events",
        proc_type="org.apache.nifi.kafka.processors.PublishKafka",
        properties={
            "Kafka Connection Service": kafka_service_id,
            "Topic Name": "file-arrival-events",
            "Publish Strategy": "FlowFile Content",
            "Failure Strategy": "Route to Failure"
        },
        auto_terminate_rels=["success", "failure"],
        x=200, y=900
    )
    
    # Create Connections
    create_connection(root_id, list_sftp_id, fetch_sftp_id, ["success"], "sftp-files")
    create_connection(root_id, fetch_sftp_id, put_s3_id, ["success"], "sftp-contents")
    create_connection(root_id, put_s3_id, replace_text_id, ["success"], "s3-landed")
    create_connection(root_id, replace_text_id, publish_kafka_id, ["success"], "event-payload")
    
    # Start the Processors
    # We start from the end of the pipeline backwards to ensure no data is lost/stuck
    start_processor(publish_kafka_id, "PublishKafka-Events")
    start_processor(replace_text_id, "Format-Kafka-Event")
    start_processor(put_s3_id, "PutMinio-Landing")
    start_processor(fetch_sftp_id, "FetchSFTP-Metrics")
    start_processor(list_sftp_id, "ListSFTP-Metrics")
    
    print("\n🎉 Apache NiFi Ingestion Pipeline successfully configured and started!")

if __name__ == "__main__":
    main()

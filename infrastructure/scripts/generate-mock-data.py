#!/usr/bin/env python3
import os
import csv
import random
import subprocess
from datetime import datetime
import xml.etree.ElementTree as ET

# Configuration
SERVERS = [
    {"name": "prod-web-01", "ip": "192.168.1.10"},
    {"name": "prod-web-02", "ip": "192.168.1.11"},
    {"name": "prod-db-01", "ip": "192.168.1.20"},
    {"name": "prod-db-02", "ip": "192.168.1.21"},
    {"name": "stage-app-01", "ip": "172.16.5.10"},
]

OUTPUT_DIR = "/home/haiden/bku/vdt/server-monitoring-lakehouse/sample-data/generated"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_metrics():
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    data = []
    for s in SERVERS:
        data.append({
            "timestamp": timestamp,
            "server_name": s["name"],
            "ip": s["ip"],
            "cpu": round(random.uniform(5.0, 95.0), 1),
            "ram": round(random.uniform(10.0, 90.0), 1),
            "disk": round(random.uniform(20.0, 85.0), 1),
            "io": round(random.uniform(0.1, 15.0), 2)
        })
    return timestamp, data

def write_csv(timestamp, data, filepath):
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "server_name", "ip", "cpu", "ram", "disk", "io"])
        for row in data:
            writer.writerow([
                row["timestamp"],
                row["server_name"],
                row["ip"],
                row["cpu"],
                row["ram"],
                row["disk"],
                row["io"]
            ])

def write_xml(timestamp, data, filepath):
    root = ET.Element("servers")
    for row in data:
        server_el = ET.SubElement(root, "server")
        ET.SubElement(server_el, "timestamp").text = row["timestamp"]
        ET.SubElement(server_el, "server_name").text = row["server_name"]
        ET.SubElement(server_el, "ip").text = row["ip"]
        ET.SubElement(server_el, "cpu").text = str(row["cpu"])
        ET.SubElement(server_el, "ram").text = str(row["ram"])
        ET.SubElement(server_el, "disk").text = str(row["disk"])
        ET.SubElement(server_el, "io").text = str(row["io"])
    
    tree = ET.ElementTree(root)
    # Write with indentation
    ET.indent(tree, space="    ", level=0)
    tree.write(filepath, encoding="utf-8", xml_declaration=True)

def get_sftp_pod():
    try:
        cmd = "kubectl get pods -n ingestion -l app=sftp -o jsonpath='{.items[0].metadata.name}'"
        pod_name = subprocess.check_output(cmd, shell=True).decode().strip()
        return pod_name
    except Exception as e:
        print(f"Error getting SFTP pod name: {e}")
        return None

def main():
    timestamp, data = generate_metrics()
    time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 50% chance to generate CSV or XML
    file_type = random.choice(["csv", "xml"])
    filename = f"metrics_{time_str}.{file_type}"
    local_path = os.path.join(OUTPUT_DIR, filename)
    
    if file_type == "csv":
        write_csv(timestamp, data, local_path)
    else:
        write_xml(timestamp, data, local_path)
        
    print(f"Generated {file_type.upper()} file locally: {local_path}")
    
    pod_name = get_sftp_pod()
    if not pod_name:
        print("SFTP pod not found. Please ensure the sftp deployment is running in the 'ingestion' namespace.")
        return
        
    # Copy to SFTP pod
    remote_path = f"ingestion/{pod_name}:/home/sftpuser/upload/{filename}"
    print(f"Copying file to SFTP pod {pod_name}...")
    copy_cmd = f"kubectl cp {local_path} {remote_path}"
    
    try:
        subprocess.check_call(copy_cmd, shell=True)
        print(f"Successfully uploaded {filename} to SFTP server /home/sftpuser/upload/")
        # Remove local file
        os.remove(local_path)
    except Exception as e:
        print(f"Error copying file to SFTP: {e}")

if __name__ == "__main__":
    main()

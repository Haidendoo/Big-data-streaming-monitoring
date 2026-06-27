import sys
import time
import requests

METABASE_URL = "http://localhost:3001"

def wait_for_metabase():
    print("⏳ Waiting for Metabase REST API to be available...")
    for _ in range(30):
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=3)
            if r.status_code == 200:
                print("✅ Metabase is up and healthy!")
                return True
        except Exception:
            pass
        time.sleep(5)
    print("❌ Metabase did not become available in time.")
    return False

def check_setup_status():
    try:
        # If Metabase is already configured, /api/setup/status returns status or fails with 400
        r = requests.get(f"{METABASE_URL}/api/setup/status", timeout=5)
        # Metabase returns {"is_setup": true/false}
        if r.status_code == 200:
            return r.json().get("is_setup", False)
    except Exception:
        pass
    # fallback: assume it is setup if setup endpoint is not accessible
    return True

def setup_metabase():
    print("⚙️  Checking setup status...")
    
    # Try to get setup token (v0.40+)
    try:
        r = requests.get(f"{METABASE_URL}/api/setup/admin_token", timeout=5)
        if r.status_code == 200:
            token = r.json()
            if isinstance(token, dict) and "setup-token" in token:
                token = token["setup-token"]
        else:
            token = None
    except Exception:
        token = None
        
    if not token:
        # Try fetching session properties to see if setup is needed
        try:
            r = requests.get(f"{METABASE_URL}/api/session/properties", timeout=5)
            props = r.json()
            if not props.get("has-created-first-user", False):
                token = props.get("setup-token")
        except Exception:
            pass

    if not token:
        print("ℹ️  Metabase is already set up or token could not be retrieved. Skipping auto-setup.")
        return True

    print(f"🔑 Initializing Metabase with setup token: {token[:8]}...")
    setup_payload = {
        "token": token,
        "user": {
            "email": "admin@lakehouse.com",
            "first_name": "Data",
            "last_name": "Admin",
            "password": "admin12345"
        },
        "database": {
            "engine": "presto",
            "name": "Trino Lakehouse",
            "details": {
                "host": "trino-coordinator.lakehouse.svc.cluster.local",
                "port": 8888,
                "catalog": "iceberg",
                "schema": "monitoring",
                "user": "admin"
            }
        },
        "prefs": {
            "allow_tracking": False
        },
        "site_name": "Server Monitoring BI",
        "site_locale": "en"
    }

    r = requests.post(f"{METABASE_URL}/api/setup", json=setup_payload, timeout=10)
    if r.status_code == 200 or r.status_code == 204:
        print("🎉 Metabase successfully initialized!")
        print("👉 URL: http://localhost:3001")
        print("👉 Username: admin@lakehouse.com")
        print("👉 Password: admin12345")
        return True
    else:
        print(f"❌ Metabase setup failed. Status: {r.status_code}, Response: {r.text}")
        return False

if __name__ == "__main__":
    if wait_for_metabase():
        setup_metabase()

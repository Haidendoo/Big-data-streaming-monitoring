#!/usr/bin/env python3
import json
import yaml

dashboard = {
  "annotations": {
    "list": [
      {
        "builtIn": 1,
        "datasource": {
          "type": "grafana",
          "uid": "-- Grafana --"
        },
        "enable": True,
        "hide": True,
        "name": "Annotations & Alerts",
        "type": "dashboard"
      }
    ]
  },
  "editable": True,
  "fiscalYearStartMonth": 0,
  "graphTooltip": 1,
  "id": None,
  "links": [],
  "liveNow": False,
  "panels": [
    {
      "collapsed": False,
      "gridPos": {
        "h": 1,
        "w": 24,
        "x": 0,
        "y": 0
      },
      "id": 10,
      "title": "System Performance Metrics (Iceberg Lakehouse)",
      "type": "row"
    },
    {
      "title": "CPU Utilization (%)",
      "type": "timeseries",
      "id": 1,
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 1
      },
      "datasource": {
        "type": "trino-datasource",
        "uid": "trino-lakehouse"
      },
      "targets": [
        {
          "datasource": {
            "type": "trino-datasource",
            "uid": "trino-lakehouse"
          },
          "format": 0,
          "rawQuery": True,
          "rawSql": "SELECT ts AS time, cpu_util AS value, server_name AS metric FROM iceberg.monitoring.server_metrics WHERE $__timeFilter(ts) ORDER BY ts",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "custom": {
            "drawStyle": "line",
            "lineInterpolation": "smooth"
          },
          "unit": "percent",
          "min": 0,
          "max": 100
        }
      }
    },
    {
      "title": "RAM Utilization (%)",
      "type": "timeseries",
      "id": 2,
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 1
      },
      "datasource": {
        "type": "trino-datasource",
        "uid": "trino-lakehouse"
      },
      "targets": [
        {
          "datasource": {
            "type": "trino-datasource",
            "uid": "trino-lakehouse"
          },
          "format": 0,
          "rawQuery": True,
          "rawSql": "SELECT ts AS time, ram_util AS value, server_name AS metric FROM iceberg.monitoring.server_metrics WHERE $__timeFilter(ts) ORDER BY ts",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "custom": {
            "drawStyle": "line",
            "lineInterpolation": "smooth"
          },
          "unit": "percent",
          "min": 0,
          "max": 100
        }
      }
    },
    {
      "title": "Disk Utilization (%)",
      "type": "timeseries",
      "id": 3,
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 0,
        "y": 9
      },
      "datasource": {
        "type": "trino-datasource",
        "uid": "trino-lakehouse"
      },
      "targets": [
        {
          "datasource": {
            "type": "trino-datasource",
            "uid": "trino-lakehouse"
          },
          "format": 0,
          "rawQuery": True,
          "rawSql": "SELECT ts AS time, disk_util AS value, server_name AS metric FROM iceberg.monitoring.server_metrics WHERE $__timeFilter(ts) ORDER BY ts",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "custom": {
            "drawStyle": "line",
            "lineInterpolation": "smooth"
          },
          "unit": "percent",
          "min": 0,
          "max": 100
        }
      }
    },
    {
      "title": "I/O Stat (MB/s)",
      "type": "timeseries",
      "id": 4,
      "gridPos": {
        "h": 8,
        "w": 12,
        "x": 12,
        "y": 9
      },
      "datasource": {
        "type": "trino-datasource",
        "uid": "trino-lakehouse"
      },
      "targets": [
        {
          "datasource": {
            "type": "trino-datasource",
            "uid": "trino-lakehouse"
          },
          "format": 0,
          "rawQuery": True,
          "rawSql": "SELECT ts AS time, io_stat AS value, server_name AS metric FROM iceberg.monitoring.server_metrics WHERE $__timeFilter(ts) ORDER BY ts",
          "refId": "A"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "custom": {
            "drawStyle": "line",
            "lineInterpolation": "smooth"
          },
          "unit": "decbytes",
          "min": 0
        }
      }
    },
    {
      "title": "Latest Server Metrics",
      "type": "table",
      "id": 5,
      "gridPos": {
        "h": 8,
        "w": 24,
        "x": 0,
        "y": 17
      },
      "datasource": {
        "type": "trino-datasource",
        "uid": "trino-lakehouse"
      },
      "targets": [
        {
          "datasource": {
            "type": "trino-datasource",
            "uid": "trino-lakehouse"
          },
          "format": 1,
          "rawQuery": True,
          "rawSql": "SELECT ts, server_name, ip, cpu_util, ram_util, disk_util, io_stat FROM (\n  SELECT *,\n         ROW_NUMBER() OVER (PARTITION BY server_name ORDER BY ts DESC) as rn\n  FROM iceberg.monitoring.server_metrics\n) WHERE rn = 1 ORDER BY server_name",
          "refId": "A"
        }
      ]
    }
  ],
  "refresh": "10s",
  "schemaVersion": 38,
  "style": "dark",
  "tags": ["lakehouse", "monitoring"],
  "templating": {
    "list": []
  },
  "time": {
    "from": "now-2h",
    "to": "now"
  },
  "timepicker": {},
  "timezone": "browser",
  "title": "Server Performance Monitoring",
  "uid": "server_monitoring_lakehouse",
  "version": 1,
  "weekStart": ""
}

values = {
  "adminPassword": "admin",
  "service": {
    "type": "LoadBalancer",
    "port": 80
  },
  "persistence": {
    "enabled": False
  },
  "resources": {
    "limits": {
      "memory": "512Mi"
    },
    "requests": {
      "memory": "256Mi"
    }
  },
  "plugins": [
    "trino-datasource"
  ],
  "datasources": {
    "datasources.yaml": {
      "apiVersion": 1,
      "datasources": [
        {
          "name": "Prometheus",
          "type": "prometheus",
          "uid": "prometheus-lakehouse",
          "url": "http://prometheus-server.monitoring.svc.cluster.local",
          "access": "proxy",
          "isDefault": True
        },
        {
          "name": "Trino",
          "type": "trino-datasource",
          "uid": "trino-lakehouse",
          "url": "http://trino.lakehouse.svc.cluster.local:8888",
          "access": "proxy",
          "isDefault": False,
          "jsonData": {
            "httpMethod": "POST"
          }
        }
      ]
    }
  },
  "dashboardProviders": {
    "dashboardproviders.yaml": {
      "apiVersion": 1,
      "providers": [
        {
          "name": "default",
          "orgId": 1,
          "folder": "",
          "type": "file",
          "disableDeletion": False,
          "editable": True,
          "options": {
            "path": "/var/lib/grafana/dashboards/default"
          }
        }
      ]
    }
  },
  "dashboards": {
    "default": {
      "server-monitoring": {
        "json": json.dumps(dashboard)
      }
    }
  }
}

with open("infrastructure/helm/grafana/values.yaml", "w") as f:
    yaml.dump(values, f, default_flow_style=False)

print("✅ Successfully generated values.yaml with Server Performance Monitoring dashboard!")

"""A stand-in for the Cloud Quotas API (#985): GKE's ClustersPerRegion
quotaInfo, shaped as the API answered for the bench project on 2026-10-29
(a dimensionsInfos entry applicable to every region, value 3). The value is
read from `limit_file` at every request (default 3), so a test can move it;
a file holding "missing" answers 404. bench.gke reaches it through gcloud's
endpoint-override name, CLOUDSDK_API_ENDPOINT_OVERRIDES_CLOUDQUOTAS."""

from __future__ import annotations

import http.server
import json
import threading
from pathlib import Path

REGIONS = ["europe-west3", "europe-west4", "us-central1"]


def quota_info(value, region_override: dict | None = None) -> dict:
    rows = [{"details": {"value": str(value)}, "applicableLocations": list(REGIONS)}]
    for region, v in (region_override or {}).items():
        # after the default: the region's own entry must win wherever the API lists it
        rows.append({"details": {"value": str(v)}, "dimensions": {"region": region},
                        "applicableLocations": [region]})
    return {"name": "projects/p/locations/global/services/container.googleapis.com/quotaInfos/ClustersPerRegion",
            "quotaId": "ClustersPerRegion", "metric": "container.googleapis.com/clusters_per_region",
            "metricDisplayName": "Regional Clusters", "dimensions": ["region"], "containerType": "PROJECT",
            "dimensionsInfos": rows}


def serve(limit_file: Path):
    """(the override URL, a stop function)."""
    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            v = limit_file.read_text().strip() if limit_file.exists() else "3"
            ok = (self.path.endswith("/quotaInfos/ClustersPerRegion")
                  and self.headers.get("Authorization", "").startswith("Bearer ") and v != "missing")
            body = json.dumps(quota_info(v) if ok else {"error": {"code": 404}}).encode()
            self.send_response(200 if ok else 404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{srv.server_address[1]}/", srv.shutdown

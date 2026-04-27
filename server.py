import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from pathlib import Path

from config import DIRTY_DIR, LOGS_DIR, WEB_DIR, BASE_DIR
from engine import ENGINE

RESEARCH_STATE = {"status": "idle", "error": None}

def run_research_task():
    RESEARCH_STATE["status"] = "running"
    RESEARCH_STATE["error"] = None
    import subprocess
    try:
        proc = subprocess.run(
            ["python3", str(BASE_DIR / "scripts" / "generate_research.py")],
            capture_output=True,
            text=True,
            check=True
        )
        RESEARCH_STATE["status"] = "complete"
    except subprocess.CalledProcessError as e:
        RESEARCH_STATE["status"] = "error"
        RESEARCH_STATE["error"] = str(e.stderr or e.stdout)
    except Exception as e:
        RESEARCH_STATE["status"] = "error"
        RESEARCH_STATE["error"] = str(e)

class Handler(BaseHTTPRequestHandler):
    def _json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path, content_type):
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _download(self, filename, data, content_type):
        body = data.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._json(ENGINE.snapshot())
            return
        if parsed.path.startswith("/api/healed-data/") and parsed.path.endswith(".csv"):
            dataset_name = parsed.path.removeprefix("/api/healed-data/").removesuffix(".csv")
            if dataset_name not in {"orders", "payments", "delivery"}:
                self._json({"ok": False, "error": "Dataset not found."}, status=404)
                return
            payload = ENGINE.current_healed_csv(dataset_name)
            if payload is None:
                self._json({"ok": False, "error": "No healed dataset available yet."}, status=404)
                return
            self._download(
                f"{payload['batch_id']}-{dataset_name}-healed.csv",
                payload["csv"],
                "text/csv; charset=utf-8",
            )
            return
        if parsed.path.startswith("/api/batch/") and parsed.path.endswith("/errors"):
            parts = parsed.path.strip("/").split("/")
            batch_id = parts[2] if len(parts) == 4 else ""
            payload = ENGINE.batch_errors(batch_id)
            if payload is None:
                self._json({"ok": False, "error": f"Batch `{batch_id}` not found."}, status=404)
                return
            self._json(payload)
            return
        if parsed.path == "/api/research/status":
            self._json(RESEARCH_STATE)
            return
        if parsed.path == "/api/research/download":
            import shutil
            import tempfile
            temp_zip = Path(tempfile.gettempdir()) / "research_archive"
            shutil.make_archive(str(temp_zip), 'zip', str(WEB_DIR / "research"))
            data = Path(str(temp_zip) + ".zip").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", 'attachment; filename="research_bundle.zip"')
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if parsed.path.startswith("/research/"):
            file_path = WEB_DIR / parsed.path.lstrip("/")
            if file_path.exists() and file_path.is_file():
                ext = file_path.suffix
                content_type = {
                    ".html": "text/html; charset=utf-8",
                    ".md": "text/markdown; charset=utf-8",
                    ".svg": "image/svg+xml",
                    ".csv": "text/csv; charset=utf-8",
                    ".json": "application/json; charset=utf-8",
                }.get(ext, "application/octet-stream")
                self._file(file_path, content_type)
            else:
                self.send_error(404, "Not found")
            return
        if parsed.path == "/api/analysis-report.json":
            with ENGINE.lock:
                ENGINE.telemetry["report_download_total"] += 1
            self._download(
                "analysis-report.json",
                json.dumps(ENGINE.build_analysis_report(), indent=2),
                "application/json; charset=utf-8",
            )
            return
        if parsed.path == "/api/analysis-report.md":
            with ENGINE.lock:
                ENGINE.telemetry["report_download_total"] += 1
            self._download(
                "analysis-report.md",
                ENGINE._analysis_markdown(ENGINE.build_analysis_report()),
                "text/markdown; charset=utf-8",
            )
            return
        if parsed.path in {"/", "/index.html"}:
            self._file(WEB_DIR / "index.html", "text/html; charset=utf-8")
            return
        if parsed.path == "/styles.css":
            self._file(WEB_DIR / "styles.css", "text/css; charset=utf-8")
            return
        if parsed.path == "/app.js":
            self._file(WEB_DIR / "app.js", "application/javascript; charset=utf-8")
            return
        self.send_error(404, "Not found")

    def do_POST(self):
        if urlparse(self.path).path != "/api/action":
            self.send_error(404, "Not found")
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length or 0).decode("utf-8") or "{}")
        try:
            if payload.get("action") == "approve-escalation":
                ENGINE.approve_escalation(payload.get("error_id", ""))
            elif payload.get("action") == "reject-escalation":
                ENGINE.reject_escalation(payload.get("error_id", ""))
            elif payload.get("action") == "manual-fix-escalation":
                ENGINE.manual_fix_escalation(
                    payload.get("error_id", ""),
                    payload.get("manual_value", ""),
                )
            elif payload.get("action") == "nl-fix-escalation":
                result = ENGINE.nl_fix_escalation(
                    payload.get("error_id", ""),
                    payload.get("instruction", ""),
                )
                self._json({"ok": True, "state": ENGINE.snapshot(), "nl_result": result})
                return
            elif payload.get("action") == "rollback-error":
                ENGINE.rollback_error(payload.get("error_id", ""))
            elif payload.get("action") == "rollback-batch":
                ENGINE.rollback_batch(payload.get("batch_id", ""))
            elif payload.get("action") == "reapply-batch-healing":
                ENGINE.reapply_batch_healing(payload.get("batch_id", ""))
            elif payload.get("action") == "generate-research":
                if RESEARCH_STATE["status"] == "running":
                    self._json({"ok": False, "error": "Already running"})
                    return
                threading.Thread(target=run_research_task, daemon=True).start()
                self._json({"ok": True})
                return
            else:
                ENGINE.handle_action(payload.get("action", ""))
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, status=400)
            return
        self._json({"ok": True, "state": ENGINE.snapshot()})

    def log_message(self, format, *args):
        return


def main():
    DIRTY_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    
    host, port = "127.0.0.1", 8080
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Data pipeline active on http://{host}:{port}")

    threading.Thread(target=ENGINE.loop, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

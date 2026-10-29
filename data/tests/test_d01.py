import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

from data.etl.config import get_db_config, get_source
from data.etl.fetch import fetch_data
from data.etl.load import connect_db, count_dataset, load_records
from data.etl.transform import parse_data, split_quality


class _RecordingHandler(BaseHTTPRequestHandler):
    seen = []
    status = 200
    body = b"id,title\n1,a\n"

    def do_GET(self):
        type(self).seen.append(self.path)
        body = type(self).body
        self.send_response(type(self).status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def _start_server(status=200, body=b"id,title\n1,a\n"):
    _RecordingHandler.seen = []
    _RecordingHandler.status = status
    _RecordingHandler.body = body
    server = HTTPServer(("127.0.0.1", 0), _RecordingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _stop_server(server):
    server.shutdown()
    server.server_close()


class CatalogAndConfigTest(unittest.TestCase):
    def test_pub005_is_csv_and_endpoint_empty(self):
        source = get_source("PUB-005")
        self.assertEqual(source["format"], "csv")
        self.assertEqual(source["endpoint"], "")
        source["format"] = "xml"
        self.assertEqual(get_source("PUB-005")["format"], "csv")

    def test_unknown_source(self):
        with self.assertRaises(KeyError):
            get_source("PUB-404")

    def test_db_config_comes_from_environment(self):
        keys = ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "IF_DB_PASSWORD")
        saved = {key: os.environ.get(key) for key in keys}
        try:
            os.environ["POSTGRES_HOST"] = "db.local"
            os.environ["POSTGRES_PORT"] = "5433"
            os.environ["POSTGRES_DB"] = "practice"
            os.environ["POSTGRES_USER"] = "student"
            os.environ["IF_DB_PASSWORD"] = "secret-not-for-logs"
            config = get_db_config()
        finally:
            for key, value in saved.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(config["host"], "db.local")
        self.assertEqual(config["port"], 5433)
        self.assertEqual(config["dbname"], "practice")
        self.assertEqual(config["user"], "student")
        self.assertEqual(config["password"], "secret-not-for-logs")


class FetchTest(unittest.TestCase):
    def test_local_csv_round_trip(self):
        with TemporaryDirectory() as folder:
            path = Path(folder) / "sample.csv"
            path.write_text("id,title\n1,안전관리\n2,청소\n", encoding="utf-8")
            source = {"endpoint": path.as_uri(), "format": "csv", "method": "GET"}
            rows = parse_data(fetch_data(source), source)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["title"], "안전관리")

    def test_empty_endpoint_does_not_call_network(self):
        with self.assertRaises(ValueError) as caught:
            fetch_data(get_source("PUB-005"))
        self.assertIn("endpoint", str(caught.exception))

    def test_rejects_non_get_and_unknown_scheme(self):
        with self.assertRaises(ValueError):
            fetch_data({"endpoint": "file:///tmp/x.csv", "method": "POST"})
        with self.assertRaises(ValueError):
            fetch_data({"endpoint": "ftp://127.0.0.1/x.csv"})

    def test_file_mode_ignores_auth_and_params(self):
        saved = os.environ.pop("PUBLIC_DATA_API_KEY", None)
        try:
            with TemporaryDirectory() as folder:
                path = Path(folder) / "sample.csv"
                path.write_text("id,title\n1,안전관리\n", encoding="utf-8")
                payload = fetch_data(
                    {
                        "endpoint": path.as_uri(),
                        "format": "csv",
                        "auth_param": "serviceKey",
                        "params": {"page": "1"},
                    }
                )
            self.assertIn("안전관리".encode("utf-8"), payload)
        finally:
            if saved is not None:
                os.environ["PUBLIC_DATA_API_KEY"] = saved

    def test_http_appends_params_and_local_dummy_key(self):
        saved = os.environ.get("PUBLIC_DATA_API_KEY")
        os.environ["PUBLIC_DATA_API_KEY"] = "dummy-local-key"
        server = _start_server()
        try:
            port = server.server_address[1]
            fetch_data(
                {
                    "endpoint": f"http://127.0.0.1:{port}/items?type=csv",
                    "format": "csv",
                    "params": {"page": "1"},
                    "auth_param": "serviceKey",
                }
            )
            query = parse_qs(urlsplit("http://local" + _RecordingHandler.seen[0]).query)
            self.assertEqual(query["page"], ["1"])
            self.assertEqual(query["type"], ["csv"])
            self.assertEqual(query["serviceKey"], ["dummy-local-key"])
        finally:
            _stop_server(server)
            if saved is None:
                os.environ.pop("PUBLIC_DATA_API_KEY", None)
            else:
                os.environ["PUBLIC_DATA_API_KEY"] = saved

    def test_missing_api_key_fails_before_request(self):
        saved = os.environ.pop("PUBLIC_DATA_API_KEY", None)
        try:
            with self.assertRaises(ValueError) as caught:
                fetch_data(
                    {
                        "endpoint": "https://example.invalid/data",
                        "auth_param": "serviceKey",
                    }
                )
            self.assertIn("PUBLIC_DATA_API_KEY", str(caught.exception))
        finally:
            if saved is not None:
                os.environ["PUBLIC_DATA_API_KEY"] = saved

    def test_http_429_is_visible(self):
        server = _start_server(status=429, body=b"rate limited")
        try:
            port = server.server_address[1]
            with self.assertRaises(HTTPError) as caught:
                fetch_data({"endpoint": f"http://127.0.0.1:{port}/sample.csv"})
            self.assertEqual(caught.exception.code, 429)
        finally:
            _stop_server(server)

    def test_oversize_payload_rejected(self):
        server = _start_server(body=b"x" * (10_000_001))
        try:
            port = server.server_address[1]
            with self.assertRaises(ValueError) as caught:
                fetch_data({"endpoint": f"http://127.0.0.1:{port}/big.bin"})
            self.assertIn("10MB", str(caught.exception))
        finally:
            _stop_server(server)


class QualityTest(unittest.TestCase):
    def test_missing_required_increases_quality_count(self):
        rows = [
            {"id": "1", "title": "안전관리"},
            {"id": "1", "title": "안전관리"},
            {"id": "", "title": "누락"},
            {"id": "2", "title": "청소"},
        ]
        quality = split_quality(
            rows, {"required_fields": ["id"], "key_fields": ["id"]}
        )
        self.assertEqual(quality["rejected_count"], 1)
        self.assertEqual(quality["duplicate_count"], 1)
        self.assertEqual(len(quality["records"]), 2)

        again = split_quality(
            [row["payload"] for row in quality["records"]],
            {"required_fields": ["id"], "key_fields": ["id"]},
        )
        self.assertEqual(
            {row["record_key"] for row in again["records"]},
            {row["record_key"] for row in quality["records"]},
        )


class LoadTest(unittest.TestCase):
    def test_refuses_to_overwrite_pub_dataset(self):
        with self.assertRaises(ValueError):
            load_records("PUB-005", [])

    def test_idempotent_load_keeps_pub005(self):
        try:
            with connect_db() as conn:
                conn.execute("SELECT 1")
        except Exception as exc:
            self.skipTest(f"PostgreSQL 연결 불가: {exc}")

        records = split_quality(
            [
                {"id": "1", "title": "안전관리"},
                {"id": "1", "title": "안전관리"},
                {"id": "", "title": "누락"},
                {"id": "2", "title": "청소"},
            ],
            {"required_fields": ["id"], "key_fields": ["id"]},
        )
        before = count_dataset("PUB-005")
        first = load_records(
            "D01-POC",
            records["records"],
            rejected_count=records["rejected_count"],
            duplicate_count=records["duplicate_count"],
        )
        second = load_records(
            "D01-POC",
            records["records"],
            rejected_count=records["rejected_count"],
            duplicate_count=records["duplicate_count"],
        )
        self.assertEqual(first["status"], "SUCCESS")
        self.assertEqual(first["stored_count"], 2)
        self.assertEqual(second["stored_count"], 2)
        self.assertEqual(second["snapshot_count"], first["snapshot_count"])
        self.assertEqual(second["ingestion_run_id"], first["ingestion_run_id"])
        self.assertEqual(count_dataset("PUB-005"), before)


if __name__ == "__main__":
    unittest.main()

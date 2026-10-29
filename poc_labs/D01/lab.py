# D01 소스 카탈로그/API/로컬 파일 수집 경로를 검증한다.
"""교재 시나리오와 정상/경계/장애 케이스를 실행하고 if_spring에 실습 행을 적재한다.

공공 API는 호출하지 않는다. endpoint가 비어 있으면 네트워크 없이 실패한다.
적재는 D01-POC dataset만 갱신하고 기존 PUB-005 행은 그대로 둔다.
"""

import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.error import HTTPError, URLError

from data.etl.config import get_db_config, get_source
from data.etl.fetch import fetch_data
from data.etl.load import connect_db, count_dataset, load_records
from data.etl.transform import normalize_rows, parse_data, split_quality


class _QuietHandler(BaseHTTPRequestHandler):
    status = 200
    body = b""

    def do_GET(self):
        body = type(self).body
        self.send_response(type(self).status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return


def _serve(status: int, body: bytes):
    handler = type("Handler", (_QuietHandler,), {"status": status, "body": body})
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _stop_server(server) -> None:
    server.shutdown()
    server.server_close()


def _check(results: list, name: str, ok: bool, detail: str) -> None:
    status = "PASS" if ok else "FAIL"
    results.append((status, name, detail))
    print(f"{status} {name}: {detail}")


def main() -> None:
    results: list[tuple[str, str, str]] = []
    practice_quality = None

    # 원본 소스 카탈로그를 로딩한다. endpoint는 별도 실습에서 채운다.
    source = get_source("PUB-005")
    assert source["format"] == "csv"
    _check(results, "카탈로그 PUB-005", True, "format=csv, endpoint는 비어 있음")

    try:
        fetch_data(source)
        _check(results, "빈 endpoint", False, "예외 없이 호출됨")
    except ValueError as exc:
        _check(results, "빈 endpoint", "endpoint" in str(exc), str(exc))

    with TemporaryDirectory() as folder:
        root = Path(folder)

        local = root / "sample.csv"
        local.write_text("id,title\n1,안전관리\n2,청소\n", encoding="utf-8")
        # 실제 공공 API 대신 자기 소유의 CSV 파일을 수집한다.
        demo = {"endpoint": local.as_uri(), "format": "csv", "method": "GET"}
        payload = fetch_data(demo)
        rows = parse_data(payload, demo)
        assert len(rows) == 2 and rows[0]["title"] == "안전관리"
        print("PASS 로컬 수집", len(payload), "bytes", len(rows), "rows")
        _check(
            results,
            "정상 CSV",
            True,
            f"{len(payload)} bytes, {len(rows)} rows, title={rows[0]['title']}",
        )

        json_file = root / "sample.json"
        json_file.write_text(
            '{"items":[{"id":"1","title":"안전관리"},'
            '{"id":"2","title":"청소"},{"id":"3","title":"조리"}]}',
            encoding="utf-8",
        )
        json_source = {
            "endpoint": json_file.as_uri(),
            "format": "json",
            "method": "GET",
            "item_path": "items",
            "required_fields": ["id"],
            "key_fields": ["id"],
        }
        json_rows = parse_data(fetch_data(json_source), json_source)
        json_quality = split_quality(json_rows, json_source)
        _check(
            results,
            "정상 JSON",
            len(json_quality["records"]) == 3 and json_quality["rejected_count"] == 0,
            f"적재 {len(json_quality['records'])}건, 품질제외 {json_quality['rejected_count']}건",
        )

        xml_file = root / "sample.xml"
        xml_file.write_text(
            "<response><body><items>"
            "<item><id>1</id><title>안전관리</title></item>"
            "<item><id>2</id><title>청소</title></item>"
            "</items></body></response>",
            encoding="utf-8",
        )
        xml_source = {
            "endpoint": xml_file.as_uri(),
            "format": "xml",
            "method": "GET",
            "item_path": "response.body.items.item",
            "required_fields": ["id"],
            "key_fields": ["id"],
        }
        xml_rows = parse_data(fetch_data(xml_source), xml_source)
        xml_quality = split_quality(xml_rows, xml_source)
        _check(
            results,
            "정상 XML",
            len(xml_quality["records"]) == 2
            and xml_quality["records"][0]["payload"]["title"] == "안전관리",
            f"적재 {len(xml_quality['records'])}건",
        )

        dirty = root / "dirty.csv"
        dirty.write_text(
            "id,title\n1,안전관리\n1,안전관리\n,누락\n2,청소\n",
            encoding="utf-8",
        )
        dirty_source = {
            "endpoint": dirty.as_uri(),
            "format": "csv",
            "method": "GET",
            "required_fields": ["id"],
            "key_fields": ["id"],
        }
        dirty_rows = parse_data(fetch_data(dirty_source), dirty_source)
        quality = split_quality(dirty_rows, dirty_source)
        practice_quality = quality
        boundary_ok = (
            quality["rejected_count"] == 1
            and quality["duplicate_count"] == 1
            and len(quality["records"]) == 2
        )
        _check(
            results,
            "경계 필수값 누락",
            boundary_ok,
            "제외 "
            f"{quality['rejected_count']}건, 중복 {quality['duplicate_count']}건, "
            f"적재 {len(quality['records'])}건",
        )

        first_keys = {row["record_key"] for row in quality["records"]}
        second = normalize_rows(
            [row["payload"] for row in quality["records"]],
            {"key_fields": ["id"], "required_fields": []},
        )
        second_keys = {row["record_key"] for row in second}
        _check(
            results,
            "멱등 재실행",
            first_keys == second_keys and len(second) == len(quality["records"]),
            f"재실행 전후 키 {len(first_keys)}개, 증가 0",
        )

        saved_key = os.environ.pop("PUBLIC_DATA_API_KEY", None)
        try:
            authed = {
                "endpoint": dirty.as_uri(),
                "format": "csv",
                "method": "GET",
                "auth_param": "serviceKey",
                "params": {"page": "1"},
            }
            local_payload = fetch_data(authed)
            _check(
                results,
                "로컬 파일은 인증 생략",
                local_payload == dirty.read_bytes(),
                f"{len(local_payload)} bytes",
            )
        finally:
            if saved_key is not None:
                os.environ["PUBLIC_DATA_API_KEY"] = saved_key

    try:
        fetch_data({"endpoint": "ftp://127.0.0.1/sample.csv", "format": "csv"})
        _check(results, "장애 잘못된 스킴", False, "예외 없이 호출됨")
    except ValueError as exc:
        _check(results, "장애 잘못된 스킴", "스킴" in str(exc), str(exc))

    try:
        fetch_data(
            {
                "endpoint": "file:///tmp/d01-missing-sample.csv",
                "format": "csv",
                "method": "GET",
            }
        )
        _check(results, "장애 없는 파일", False, "예외 없이 호출됨")
    except URLError as exc:
        _check(results, "장애 없는 파일", True, f"FAILED URLError: {exc.reason}")

    saved_key = os.environ.pop("PUBLIC_DATA_API_KEY", None)
    try:
        fetch_data(
            {
                "endpoint": "https://example.invalid/public-data",
                "format": "xml",
                "method": "GET",
                "auth_param": "serviceKey",
            }
        )
        _check(results, "장애 인증 실패", False, "예외 없이 호출됨")
    except ValueError as exc:
        _check(
            results,
            "장애 인증 실패",
            "PUBLIC_DATA_API_KEY" in str(exc),
            "FAILED " + str(exc),
        )
    finally:
        if saved_key is not None:
            os.environ["PUBLIC_DATA_API_KEY"] = saved_key

    server = _serve(429, b"rate limited")
    try:
        port = server.server_address[1]
        fetch_data(
            {
                "endpoint": f"http://127.0.0.1:{port}/sample.csv",
                "format": "csv",
                "method": "GET",
            }
        )
        _check(results, "장애 API 429", False, "예외 없이 호출됨")
    except HTTPError as exc:
        _check(
            results,
            "장애 API 429",
            exc.code == 429,
            f"FAILED HTTP {exc.code}. 재시도하지 않고 로컬 사본 건수를 유지",
        )
    finally:
        _stop_server(server)

    db = get_db_config()
    db_ok = set(db) == {"host", "port", "dbname", "user", "password"} and isinstance(
        db["port"], int
    )
    _check(
        results,
        "DB 설정",
        db_ok,
        f"host={db['host']} port={db['port']} dbname={db['dbname']} user={db['user']}",
    )

    saved_port = os.environ.get("POSTGRES_PORT")
    os.environ["POSTGRES_PORT"] = "1"
    try:
        connect_db()
        _check(results, "장애 DB 연결 끊김", False, "연결됨")
    except Exception as exc:
        _check(results, "장애 DB 연결 끊김", True, f"FAILED {type(exc).__name__}")
    finally:
        if saved_port is None:
            os.environ.pop("POSTGRES_PORT", None)
        else:
            os.environ["POSTGRES_PORT"] = saved_port

    before_pub005 = count_dataset("PUB-005")
    loaded = load_records(
        "D01-POC",
        practice_quality["records"],
        rejected_count=practice_quality["rejected_count"],
        duplicate_count=practice_quality["duplicate_count"],
    )
    reloaded = load_records(
        "D01-POC",
        practice_quality["records"],
        rejected_count=practice_quality["rejected_count"],
        duplicate_count=practice_quality["duplicate_count"],
    )
    after_pub005 = count_dataset("PUB-005")
    load_ok = (
        loaded["status"] == "SUCCESS"
        and loaded["stored_count"] == 2
        and reloaded["stored_count"] == loaded["stored_count"]
        and reloaded["snapshot_count"] == loaded["snapshot_count"]
        and reloaded["dq_count"] == loaded["dq_count"] == 2
        and reloaded["ingestion_run_id"] == loaded["ingestion_run_id"]
        and before_pub005 == after_pub005 == loaded["pub005_count"]
    )
    _check(
        results,
        "실제 적재",
        load_ok,
        "run "
        f"{loaded['ingestion_run_id']}, catalog {loaded['catalog_source_id']}, "
        f"D01-POC {loaded['stored_count']}건, snapshot {loaded['snapshot_count']}건, "
        f"재실행 증가 0, PUB-005 {before_pub005}건 유지",
    )

    try:
        get_source("PUB-404")
        _check(results, "없는 source_id", False, "예외 없이 반환됨")
    except KeyError as exc:
        _check(results, "없는 source_id", "PUB-404" in str(exc), str(exc))

    failed = [name for status, name, _detail in results if status != "PASS"]
    print(f"결과 {len(results) - len(failed)}/{len(results)}")
    if failed:
        raise SystemExit("실패: " + ", ".join(failed))


if __name__ == "__main__":
    main()

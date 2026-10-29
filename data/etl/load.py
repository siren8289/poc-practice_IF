"""D01 실습 적재: 로컬에서 정규화한 행을 if_spring에 넣는다.

운영 적재 테이블은 다음 두 층이다.
- external_ref.source_catalog / external_ingestion_run / raw_external_snapshot
- external_ref.dq_check_result / lineage_event
- staging.public_api_raw

dataset_code가 D01- 로 시작하는 실습 행만 쓴다.
이미 쌓여 있는 PUB-005 같은 운영 코드는 이 모듈이 갱신하지 않는다.
"""

import hashlib
import json

import psycopg
from psycopg.types.json import Json

from data.etl.config import get_db_config

SCHEMA_VERSION = "d01-v1"
SERVING_TARGET = "staging.public_api_raw"


def connect_db():
    """환경 변수의 DB 설정으로 접속한다. 비밀번호가 없으면 로컬 인증에 맡긴다."""
    config = get_db_config()
    if not config.get("password"):
        config.pop("password", None)
    return psycopg.connect(**config, connect_timeout=5)


def count_dataset(dataset_code: str) -> int:
    with connect_db() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM staging.public_api_raw
            WHERE dataset_code = %s
            """,
            (dataset_code,),
        ).fetchone()
    return int(row[0])


def load_records(
    dataset_code: str,
    rows: list[dict],
    *,
    rejected_count: int = 0,
    duplicate_count: int = 0,
    idempotency_key: str = "D01-POC-sample",
    source_name: str = "D01 실습 장기요양 샘플",
    source_url: str = "file://poc-labs/D01/sample.csv",
    source_type: str = "FILE_CSV",
) -> dict:
    """정규화 행을 실습 dataset으로 upsert하고 같은 키의 재실행은 건수를 늘리지 않는다."""
    if not str(dataset_code).startswith("D01-"):
        raise ValueError(
            "실습 적재는 D01- 접두사 dataset만 허용합니다. "
            "기존 PUB 데이터는 덮어쓰지 않습니다."
        )

    ordered = sorted(rows, key=_row_sort_key)
    raw_count = len(ordered) + rejected_count + duplicate_count
    run_id = None
    try:
        with connect_db() as conn:
            catalog_id = _ensure_source(
                conn, source_name, source_url, source_type
            )
            run_id = _start_run(conn, catalog_id, idempotency_key)
        with connect_db() as conn:
            _upsert_rows(conn, dataset_code, ordered)
            _insert_snapshots(conn, catalog_id, run_id, ordered)
            _replace_quality(conn, run_id, rejected_count, duplicate_count)
            _replace_lineage(conn, run_id, catalog_id, len(ordered))
            _finish_run(
                conn,
                run_id,
                "SUCCESS",
                raw_count=raw_count,
                accepted_count=len(ordered),
                rejected_count=rejected_count,
            )
            counts = _read_counts(conn, dataset_code, catalog_id, run_id)
    except Exception as exc:
        if run_id is not None:
            _mark_failed(run_id, exc)
        raise

    return {
        "status": "SUCCESS",
        "ingestion_run_id": run_id,
        "catalog_source_id": catalog_id,
        "accepted_count": len(ordered),
        **counts,
    }


def _row_sort_key(row: dict):
    payload = row.get("payload") or {}
    return (str(payload.get("id", "")), str(row.get("record_key", "")))


def _ensure_source(conn, source_name: str, source_url: str, source_type: str) -> int:
    row = conn.execute(
        """
        INSERT INTO external_ref.source_catalog (
            source_name,
            source_url,
            source_type,
            serving_target,
            is_required_for_core,
            license_note
        )
        VALUES (%s, %s, %s, %s, false, %s)
        ON CONFLICT (source_name, source_url)
        DO UPDATE SET
            source_type = EXCLUDED.source_type,
            serving_target = EXCLUDED.serving_target,
            license_note = EXCLUDED.license_note
        RETURNING source_id
        """,
        (
            source_name,
            source_url,
            source_type,
            SERVING_TARGET,
            "교육용 합성 데이터. 운영 공공데이터 원본이 아니다.",
        ),
    ).fetchone()
    return int(row[0])


def _start_run(conn, catalog_id: int, idempotency_key: str) -> int:
    row = conn.execute(
        """
        INSERT INTO external_ref.external_ingestion_run (
            source_id,
            idempotency_key,
            status
        )
        VALUES (%s, %s, 'RUNNING')
        ON CONFLICT (source_id, idempotency_key)
        DO UPDATE SET
            status = 'RUNNING',
            started_at = NOW(),
            finished_at = NULL,
            error_message = NULL,
            raw_row_count = 0,
            accepted_row_count = 0,
            rejected_row_count = 0
        RETURNING ingestion_run_id
        """,
        (catalog_id, idempotency_key),
    ).fetchone()
    return int(row[0])


def _upsert_rows(conn, dataset_code: str, rows: list[dict]) -> None:
    for number, row in enumerate(rows, start=1):
        payload = dict(row["payload"])
        payload["source_dataset"] = dataset_code
        payload["record_key"] = row["record_key"]
        conn.execute(
            """
            INSERT INTO staging.public_api_raw (
                dataset_code,
                source_row_number,
                payload
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (dataset_code, source_row_number)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                collected_at = NOW()
            """,
            (dataset_code, number, Json(payload)),
        )


def _insert_snapshots(conn, catalog_id: int, run_id: int, rows: list[dict]) -> None:
    for row in rows:
        payload = dict(row["payload"])
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        checksum = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        conn.execute(
            """
            INSERT INTO external_ref.raw_external_snapshot (
                ingestion_run_id,
                source_id,
                source_key,
                schema_version,
                payload,
                checksum
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id, source_key, schema_version, checksum)
            DO NOTHING
            """,
            (
                run_id,
                catalog_id,
                row["record_key"],
                SCHEMA_VERSION,
                Json(payload),
                checksum,
            ),
        )


def _replace_quality(conn, run_id: int, rejected_count: int, duplicate_count: int) -> None:
    conn.execute(
        "DELETE FROM external_ref.dq_check_result WHERE ingestion_run_id = %s",
        (run_id,),
    )
    checks = (
        (
            "required_fields",
            "WARN" if rejected_count else "PASS",
            "0",
            str(rejected_count),
            "필수값이 빈 행은 적재에서 제외한다.",
        ),
        (
            "natural_key_duplicate",
            "WARN" if duplicate_count else "PASS",
            "0",
            str(duplicate_count),
            "같은 자연키는 한 건만 남긴다.",
        ),
    )
    for name, status, expected, actual, message in checks:
        conn.execute(
            """
            INSERT INTO external_ref.dq_check_result (
                ingestion_run_id,
                check_name,
                status,
                expected_value,
                actual_value,
                message
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (run_id, name, status, expected, actual, message),
        )


def _replace_lineage(conn, run_id: int, catalog_id: int, accepted_count: int) -> None:
    conn.execute(
        "DELETE FROM external_ref.lineage_event WHERE ingestion_run_id = %s",
        (run_id,),
    )
    conn.execute(
        """
        INSERT INTO external_ref.lineage_event (
            ingestion_run_id,
            source_id,
            from_layer,
            to_layer,
            target_table,
            row_count,
            note
        )
        VALUES (%s, %s, 'file', 'staging', 'staging.public_api_raw', %s, %s)
        """,
        (
            run_id,
            catalog_id,
            accepted_count,
            "D01 합성 데이터 적재. 운영 PUB 행은 변경하지 않는다.",
        ),
    )


def _finish_run(
    conn,
    run_id: int,
    status: str,
    *,
    raw_count: int = 0,
    accepted_count: int = 0,
    rejected_count: int = 0,
    error: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE external_ref.external_ingestion_run
        SET status = %s,
            raw_row_count = %s,
            accepted_row_count = %s,
            rejected_row_count = %s,
            error_message = %s,
            finished_at = CASE WHEN %s = 'RUNNING' THEN NULL ELSE NOW() END
        WHERE ingestion_run_id = %s
        """,
        (
            status,
            raw_count,
            accepted_count,
            rejected_count,
            error,
            status,
            run_id,
        ),
    )


def _read_counts(conn, dataset_code: str, catalog_id: int, run_id: int) -> dict:
    stored = conn.execute(
        """
        SELECT COUNT(*)
        FROM staging.public_api_raw
        WHERE dataset_code = %s
        """,
        (dataset_code,),
    ).fetchone()[0]
    snapshots = conn.execute(
        """
        SELECT COUNT(*)
        FROM external_ref.raw_external_snapshot
        WHERE source_id = %s
          AND schema_version = %s
        """,
        (catalog_id, SCHEMA_VERSION),
    ).fetchone()[0]
    quality = conn.execute(
        """
        SELECT COUNT(*)
        FROM external_ref.dq_check_result
        WHERE ingestion_run_id = %s
        """,
        (run_id,),
    ).fetchone()[0]
    lineage = conn.execute(
        """
        SELECT COUNT(*)
        FROM external_ref.lineage_event
        WHERE ingestion_run_id = %s
        """,
        (run_id,),
    ).fetchone()[0]
    preserved = conn.execute(
        """
        SELECT COUNT(*)
        FROM staging.public_api_raw
        WHERE dataset_code = 'PUB-005'
        """,
    ).fetchone()[0]
    return {
        "stored_count": int(stored),
        "snapshot_count": int(snapshots),
        "dq_count": int(quality),
        "lineage_count": int(lineage),
        "pub005_count": int(preserved),
    }


def _mark_failed(run_id: int, exc: Exception) -> None:
    message = f"{type(exc).__name__}: {exc}"
    try:
        with connect_db() as conn:
            _finish_run(conn, run_id, "FAILED", error=message[:500])
    except Exception:
        return

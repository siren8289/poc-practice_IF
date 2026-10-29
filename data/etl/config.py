"""D01 실습 구현: JSON 카탈로그와 환경 변수로 DB/API 설정을 제공한다."""

import json
import os
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[1] / "sources.json"


def get_source(source_id: str) -> dict:
    """실행 위치와 무관하게 data/sources.json을 연다."""
    sources = json.loads(CATALOG.read_text(encoding="utf-8"))
    if source_id not in sources:
        raise KeyError("알 수 없는 source_id: " + source_id)
    return dict(sources[source_id])


def get_db_config() -> dict:
    """DB 접속 정보는 코드가 아닌 환경변수에서 로드한다."""
    return {
        "host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "if_spring"),
        "user": os.getenv("POSTGRES_USER", "if_user"),
        "password": os.getenv("IF_DB_PASSWORD", ""),
    }

# D01 소스 카탈로그/API/로컬 파일 수집 경로를 검증한다.
from pathlib import Path
from tempfile import TemporaryDirectory

from data.etl.config import get_source
from data.etl.fetch import fetch_data
from data.etl.transform import parse_data

# 원본 소스 카탈로그를 로딩한다. endpoint는 별도 실습에서 채운다.
source = get_source("PUB-005")
assert source["format"] == "csv"
with TemporaryDirectory() as folder:
    local = Path(folder) / "sample.csv"
    local.write_text("id,title\n1,안전관리\n2,청소\n", encoding="utf-8")
    # 실제 공공 API 대신 자기 소유의 CSV 파일을 수집한다.
    demo = {"endpoint": local.as_uri(), "format": "csv", "method": "GET"}
    payload = fetch_data(demo)
    rows = parse_data(payload, demo)
    assert len(rows) == 2 and rows[0]["title"] == "안전관리"
    print("PASS 로컬 수집", len(payload), "bytes", len(rows), "rows")
# 실제 endpoint 설정과 인증값은 본인의 합법적 이용 권한 아래에서만 추가.

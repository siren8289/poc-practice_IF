"""D01 실습 구현: HTTP(S) 및 로컬 file URL에서 데이터 가져오기."""

import os
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

MAX_BYTES = 10_000_000


def fetch_data(source: dict) -> bytes:
    endpoint = str(source.get("endpoint", "")).strip()
    if not endpoint:
        raise ValueError("endpoint가 비어 있습니다: data/sources.json 확인")

    scheme = urlparse(endpoint).scheme
    if scheme not in ("https", "http", "file"):
        raise ValueError("지원하지 않는 endpoint 스킴: " + scheme)

    if str(source.get("method", "GET")).upper() != "GET":
        raise ValueError("실습은 읽기 전용 GET만 지원")

    params = dict(source.get("params") or {})
    auth_key = source.get("auth_param")
    if auth_key and scheme != "file":
        token = os.getenv("PUBLIC_DATA_API_KEY", "")
        if not token:
            raise ValueError("PUBLIC_DATA_API_KEY 환경변수 필요")
        params[auth_key] = token

    if params and scheme != "file":
        endpoint += ("&" if "?" in endpoint else "?") + urlencode(params)

    # 임의 서비스로 키를 전송하지 않도록 승인된 공식 endpoint만 등록할 것.
    request = Request(endpoint, headers={"User-Agent": "IF-PoC/1.0"})
    try:
        with urlopen(request, timeout=20) as response:
            data = response.read(MAX_BYTES + 1)
    except HTTPError as exc:
        exc.close()
        raise
    if len(data) > MAX_BYTES:
        raise ValueError("파일 크기 10MB 초과")
    return data

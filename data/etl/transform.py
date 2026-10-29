"""CSV/JSON/XML 파싱과 필수값·자연키 정규화.

원본 IF_v2 data/etl/transform.py 계약을 유지한다.
parse_data / normalize_rows 시그니처는 data/tests/test_transform.py가 고정한다.
"""

import csv
import hashlib
import io
import json
import xml.etree.ElementTree as ET


def _select_path(value, path):
    for part in filter(None, path.split(".")):
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, list):
            value = [item.get(part) for item in value if isinstance(item, dict)]
        else:
            return None
        if value is None:
            return None
    return value


def _xml_value(element):
    children = list(element)
    if not children:
        return element.text or ""

    result = {}
    for child in children:
        name = child.tag.rsplit("}", 1)[-1]
        value = _xml_value(child)
        if name in result:
            if not isinstance(result[name], list):
                result[name] = [result[name]]
            result[name].append(value)
        else:
            result[name] = value
    return result


def parse_data(payload, source):
    data_format = source.get("format", "").lower()
    if isinstance(payload, bytes):
        text = payload.decode("utf-8-sig")
    else:
        text = payload

    if data_format == "json":
        data = json.loads(text)
        selected = _select_path(data, source.get("item_path", ""))
        if selected is None:
            return []
        return selected if isinstance(selected, list) else [selected]

    if data_format == "xml":
        root = ET.fromstring(text)
        path = source.get("item_path", "")
        parts = list(filter(None, path.split(".")))
        if parts and parts[0] == root.tag.rsplit("}", 1)[-1]:
            parts.pop(0)
        elements = [root]
        for part in parts:
            elements = [
                child
                for element in elements
                for child in element
                if child.tag.rsplit("}", 1)[-1] == part
            ]
        return [_xml_value(element) for element in elements]

    if data_format == "csv":
        return list(csv.DictReader(io.StringIO(text)))

    raise ValueError(f"Unsupported data format: {data_format}")


def normalize_rows(rows, source):
    required_fields = source.get("required_fields", [])
    key_fields = source.get("key_fields", [])
    normalized = []
    seen = set()

    for row in rows:
        if any(
            row.get(field) is None or str(row[field]).strip() == ""
            for field in required_fields
        ):
            continue

        key_values = [row.get(field) for field in key_fields] if key_fields else row
        encoded_key = json.dumps(
            key_values, sort_keys=True, ensure_ascii=False, default=str
        )
        if encoded_key in seen:
            continue
        seen.add(encoded_key)

        record_key = hashlib.sha256(encoded_key.encode("utf-8")).hexdigest()
        normalized.append({"record_key": record_key, "payload": row})

    return normalized


def split_quality(rows, source):
    """필수값 누락과 자연키 중복을 나눠 품질 건수를 센다.

    normalize_rows는 두 경우를 함께 버리므로, 경계 케이스의 품질 건수는
    여기서 먼저 누락 행을 분리한 뒤 남은 행만 정규화해 구한다.
    """
    required_fields = source.get("required_fields", [])
    rejected = []
    candidates = []
    for row in rows:
        missing = [
            field
            for field in required_fields
            if row.get(field) is None or str(row[field]).strip() == ""
        ]
        if missing:
            rejected.append({"row": row, "fields": missing})
        else:
            candidates.append(row)

    normalized = normalize_rows(candidates, {**source, "required_fields": []})
    return {
        "records": normalized,
        "rejected_count": len(rejected),
        "duplicate_count": len(candidates) - len(normalized),
    }

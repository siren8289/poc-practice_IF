import unittest

from data.etl.transform import parse_data, normalize_rows


class TestTransform(unittest.TestCase):
    def test_parse_xml(self):
        xml = b"""
        <response>
          <body>
            <items>
              <item>
                <jobId>100</jobId>
                <title>Test Job</title>
              </item>
            </items>
          </body>
        </response>
        """

        source = {
            "format": "xml",
            "item_path": "response.body.items.item",
        }

        rows = parse_data(xml, source)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["jobId"], "100")

    def test_duplicate_removed(self):
        source = {
            "key_fields": ["jobId"],
            "required_fields": ["jobId"],
        }

        rows = [
            {"jobId": "100", "title": "A"},
            {"jobId": "100", "title": "A"},
        ]

        result = normalize_rows(rows, source)

        self.assertEqual(len(result), 1)

    def test_json_parsing(self):
        payload = b'{"items":[{"id":"1"}]}'

        source = {
            "format": "json",
            "item_path": "items",
        }

        result = parse_data(payload, source)

        self.assertEqual(result, [{"id": "1"}])

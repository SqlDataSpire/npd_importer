from datetime import date

from npd_loader.runxml import build_config_xml, build_output_xml, parse_release


def test_config_xml_roundtrip_release():
    xml = build_config_xml({"run_type": "National Provider Directory", "release_date": "2026-09-29",
                            "force": False, "note": "a<b&c"})
    assert xml.startswith("<WAREHOUSE_RUN_CONFIG><run_type>National Provider Directory</run_type>")
    assert "<force>False</force>" in xml
    assert "a&lt;b&amp;c" in xml
    assert parse_release(xml) == date(2026, 9, 29)


def test_parse_release_tolerates_missing_or_bad_xml():
    assert parse_release(None) is None
    assert parse_release("not xml") is None
    assert parse_release("<WAREHOUSE_RUN_CONFIG/>") is None


def test_output_xml_drops_none():
    xml = build_output_xml([{"name": "a", "size": 3, "etag": None}], item_tag="file")
    assert xml == '<WAREHOUSE_RUN_OUTPUT><file name="a" size="3" /></WAREHOUSE_RUN_OUTPUT>'

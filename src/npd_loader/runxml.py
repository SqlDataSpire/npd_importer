"""Builders for master_warehouse_run.xml_config / xml_output."""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import date


def build_config_xml(values: dict[str, object]) -> str:
    root = ET.Element("WAREHOUSE_RUN_CONFIG")
    for key, value in values.items():
        ET.SubElement(root, key).text = "" if value is None else str(value)
    return ET.tostring(root, encoding="unicode")


def parse_release(config_xml: str | None) -> date | None:
    if not config_xml:
        return None
    try:
        root = ET.fromstring(config_xml)
    except ET.ParseError:
        return None
    el = root.find("release_date")
    if el is None or not el.text:
        return None
    try:
        return date.fromisoformat(el.text.strip())
    except ValueError:
        return None


def build_output_xml(items: list[dict[str, object]], item_tag: str) -> str:
    root = ET.Element("WAREHOUSE_RUN_OUTPUT")
    for item in items:
        attrs = {k: str(v) for k, v in item.items() if v is not None and k != "_tag"}
        ET.SubElement(root, str(item.get("_tag", item_tag)), attrs)
    return ET.tostring(root, encoding="unicode")

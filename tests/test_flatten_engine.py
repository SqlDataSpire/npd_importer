import pytest

from npd_loader.flatten.convert import join, ref_to
from npd_loader.flatten.engine import (E, R, Table, columns, ext, flatten_resource, identifier, key_columns, official_name, path,
                                        ref_columns)

RES = {"resourceType": "Practitioner", "id": "P1", "meta": {"lastUpdated": "2026-09-29T04:34:00Z"},
       "extension": [{"url": "a", "valueBoolean": True}, {"url": "n", "extension": [{"url": "x", "valueCode": "y"}]}],
       "identifier": [{"system": "s1", "value": "1"}, {"system": "npi", "value": "2"}, {"system": "npi", "value": "3"}],
       "name": [{"use": "maiden", "family": "S"}, {"use": "official", "family": "J", "given": ["A", "M"]}],
       "telecom": [{"system": "phone", "value": "1"}, "junk", {"value": "2"}],
       "alias": ["one", "two"]}
LIN = ("2026-09-29", "P1", 500, 501)


def test_path_and_helpers():
    assert path("meta.lastUpdated")(RES) == "2026-09-29T04:34:00Z"
    assert path("name[1].given[0]")(RES) == "A" and path("name[9].family")(RES) is None
    assert path("telecom[1].value")(RES) is None
    assert ext("a")(RES)["valueBoolean"] is True and ext("zzz")(RES) is None
    assert ext("x", inner=ext("n"))(RES)["valueCode"] == "y"
    assert identifier(("npi",))(RES) == "2"
    assert official_name(RES)["family"] == "J"
    assert official_name({"name": [{"family": "A"}, {"use": "x", "family": "B"}]})["family"] == "B"


def test_flatten_resource():
    t1 = Table("p", {"family": R(lambda r: (official_name(r) or {}).get("family")),
                     "given": R(lambda r: (official_name(r) or {}).get("given"), join)})
    t2 = Table("p_telecom", {"system": E("system"), "value": E("value")}, each="telecom")
    t3 = Table("p_alias", {"alias": E(lambda e: e)}, each="alias")
    t4 = Table("ident", {"value": E("value")}, each="identifier", with_type=True)
    assert columns(t2) == ["release_date", "resource_id", "ndjson_file_id", "zst_file_id", "seq", "system", "value"]
    assert columns(t4)[4:6] == ["resource_type", "seq"]
    rows = list(flatten_resource(RES, [t1, t2, t3, t4], LIN))
    assert rows[0] == ("p", LIN + ("J", "A M"))
    assert rows[1:4] == [("p_telecom", LIN + (1, "phone", "1")), ("p_telecom", LIN + (2, None, None)),
                         ("p_telecom", LIN + (3, None, "2"))]
    assert ("p_alias", LIN + (2, "two")) in rows
    assert ("ident", LIN + ("Practitioner", 1, "1")) in rows
    assert ref_to("Org")(path("x")({"x": "Org/Org-1"})) == "1"


def test_reference_columns_and_key_columns():
    t = Table("p_role", {"active": R("active"), "practitioner_id": R("practitioner.reference", target="Practitioner"),
                         "endpoint_id": E("reference", target="Endpoint")}, each="endpoint")
    i = Table("ident", {"value": E("value")}, each="identifier", with_type=True)
    assert ref_columns(t) == {"practitioner_id": "Practitioner", "endpoint_id": "Endpoint"}
    assert columns(t) == ["release_date", "resource_id", "ndjson_file_id", "zst_file_id", "seq",
                          "active", "practitioner_id", "endpoint_id"]
    assert key_columns(t) == ["release_date", "resource_key", "ndjson_file_id", "zst_file_id", "seq",
                              "active", "practitioner_key", "endpoint_key"]
    assert key_columns(i) == ["release_date", "resource_key", "ndjson_file_id", "zst_file_id", "seq", "value"]
    res = {"practitioner": {"reference": "Practitioner/Practitioner-9"}, "endpoint": [{"reference": "Endpoint/Endpoint-e1"}]}
    assert list(flatten_resource(res, [t], LIN)) == [("p_role", LIN + (1, None, "9", "e1"))]
    with pytest.raises(ValueError, match="must end in _id"):
        Table("bad", {"practitioner": R("practitioner.reference", target="Practitioner")})

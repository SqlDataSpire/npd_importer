from npd_loader.flatten.convert import join, ref
from npd_loader.flatten.engine import (E, R, Table, columns, ext, flatten_resource, identifier, official_name, path)

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
    assert ref(path("x")({"x": "Org/O-1"})) == "O-1"

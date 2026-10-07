import pytest

from npd_loader.flatten.convert import ConvertError, boolean, join, number, ref_to, strip_id, ts


def test_ts():
    assert ts("2026-09-29T04:34:00.724328Z") == "2026-09-29 04:34:00.724"
    assert ts("2026-09-29T04:34:00.9996Z") == "2026-09-29 04:34:01.000"          # half-up to ms, carries
    assert ts("2026-09-29T01:00:00-05:00") == "2026-09-29 06:00:00.000"
    assert ts("2026-09-29T04:34:00") == "2026-09-29 04:34:00.000"
    assert ts("2020") == "2020-01-01 00:00:00.000"
    assert ts("2020-05") == "2020-05-01 00:00:00.000"
    assert ts("2020-05-07") == "2020-05-07 00:00:00.000"
    assert ts(None) is None and ts("") is None
    with pytest.raises(ConvertError):
        ts("not a date")


def test_boolean_number_join_ref():
    assert (boolean(True), boolean(False), boolean(None)) == ("1", "0", None)
    with pytest.raises(ConvertError):
        boolean("yes")
    assert number(33.5) == "33.5" and number(7) == "7.0" and number(None) is None
    with pytest.raises(ConvertError):
        number(True)
    assert join(["A", "B", "C"]) == "A B C"
    assert join(["a", "b", "c"], ", ", 2) == "c"
    assert join([]) is None and join(None) is None and join(["a", None]) == "a"


def test_strip_id_and_ref_to():
    assert strip_id("Practitioner", "Practitioner-1003000100") == "1003000100"
    assert strip_id("Organization", "Organization-ea579d05-454e") == "ea579d05-454e"
    assert strip_id("Organization", "1902099112") == "1902099112"            # no prefix: kept
    assert strip_id("Organization", "Organization-") == "Organization-"      # nothing left: kept
    assert strip_id("Location", "Organization-1") == "Organization-1"        # another type's prefix: kept
    org = ref_to("Organization")
    assert org("Organization/Organization-1336200294") == "1336200294"
    assert org("Organization-1336200294") == "1336200294"                    # bare id
    assert org(None) is None and org("Organization/") is None and org(7) is None
    with pytest.raises(ConvertError, match="Practitioner/Practitioner-1 is not a Organization reference"):
        org("Practitioner/Practitioner-1")

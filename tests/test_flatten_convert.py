import pytest

from npd_loader.flatten.convert import ConvertError, boolean, join, number, ref, ts


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
    assert ref("Organization/Organization-1") == "Organization-1"
    assert ref("Organization-1") == "Organization-1" and ref(None) is None and ref("Organization/") is None

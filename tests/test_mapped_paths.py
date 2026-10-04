import pytest

from npd_loader.profile import load_mapped_paths, profile_records, unmapped
from fixture_data import all_records

COVERED_TYPES = ["Practitioner"]


@pytest.mark.parametrize("rtype", COVERED_TYPES)
def test_every_fixture_path_is_mapped_or_raw_only(rtype):
    missing = [(t, p) for t, p, _ in unmapped(profile_records(all_records()), load_mapped_paths()) if t == rtype]
    assert missing == []

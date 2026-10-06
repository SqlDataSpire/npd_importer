import pytest

from catalog_contract import CatalogContract
from fakes import FakeCatalog


class TestFakeCatalog(CatalogContract):
    @pytest.fixture
    def catalog(self):
        return FakeCatalog()


def test_fake_fail_open_runs_records_interruption():
    from npd_loader.catalog import FAILED, INTERRUPTED
    catalog = FakeCatalog()
    run = catalog.start_run("EXTRACT", "e", "<x/>")
    assert catalog.fail_open_runs("EXTRACT") == [run.id]
    assert catalog.runs[run.id]["status"] == FAILED and catalog.runs[run.id]["result"] == INTERRUPTED

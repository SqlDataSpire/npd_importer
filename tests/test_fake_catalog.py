import pytest

from catalog_contract import CatalogContract
from fakes import FakeCatalog


class TestFakeCatalog(CatalogContract):
    @pytest.fixture
    def catalog(self):
        return FakeCatalog()

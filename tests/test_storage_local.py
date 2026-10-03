import pytest

from npd_loader.storage import LocalStorage
from storage_contract import StorageContract


class TestLocalStorage(StorageContract):
    @pytest.fixture
    def storage(self, tmp_path):
        return LocalStorage(tmp_path / "data")


def test_local_uri_is_file_uri(tmp_path):
    s = LocalStorage(tmp_path)
    assert s.uri() == tmp_path.resolve().as_uri()

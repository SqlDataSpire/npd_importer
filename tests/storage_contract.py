"""Tests every Storage implementation must pass. Subclass and provide a `storage` fixture."""
import pytest


class StorageContract:
    def test_write_then_read(self, storage):
        with storage.open_write("run_1/a.bin") as f:
            f.write(b"hello")
        with storage.open_read("run_1/a.bin") as f:
            assert f.read() == b"hello"

    def test_open_write_truncates(self, storage):
        with storage.open_write("a.bin") as f:
            f.write(b"long content")
        with storage.open_write("a.bin") as f:
            f.write(b"x")
        assert storage.size("a.bin") == 1

    def test_exists_and_size(self, storage):
        assert not storage.exists("run_1/missing.bin")
        with storage.open_write("run_1/b.bin") as f:
            f.write(b"12345")
        assert storage.exists("run_1/b.bin")
        assert storage.size("run_1/b.bin") == 5

    def test_rename_creates_folders_and_replaces(self, storage):
        with storage.open_write("x.part") as f:
            f.write(b"new")
        with storage.open_write("d/x") as f:
            f.write(b"old")
        storage.rename("x.part", "d/x")
        assert not storage.exists("x.part")
        with storage.open_read("d/x") as f:
            assert f.read() == b"new"

    def test_delete_is_idempotent(self, storage):
        with storage.open_write("gone.bin") as f:
            f.write(b"1")
        storage.delete("gone.bin")
        storage.delete("gone.bin")
        assert not storage.exists("gone.bin")

    def test_list_prefix_sorted(self, storage):
        for rel in ["run_2/b", "run_1/z", "run_1/a", "other/c"]:
            with storage.open_write(rel) as f:
                f.write(b"1")
        assert storage.list("run_1/") == ["run_1/a", "run_1/z"]
        assert storage.list("") == ["other/c", "run_1/a", "run_1/z", "run_2/b"]

    @pytest.mark.parametrize("bad", ["../escape", "/abs/path", "a/../../b", ""])
    def test_rejects_unsafe_paths(self, storage, bad):
        with pytest.raises(ValueError):
            storage.exists(bad)

    def test_uri(self, storage):
        assert storage.uri().startswith(("file://", "s3://", "https://"))
        assert storage.uri("run_1/a.bin").endswith("/run_1/a.bin")

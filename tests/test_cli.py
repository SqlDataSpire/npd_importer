import json
from datetime import date
from types import SimpleNamespace

import zstandard

from npd_loader import cli
from npd_loader.config import parse_config
from npd_loader.stages import Outcome, StageFailed
from fakes import FakeCatalog
from fixture_data import PRAC1, PRAC2
from helpers import config_data


def write_zst(path, records):
    path.write_bytes(zstandard.ZstdCompressor().compress(
        b"".join(json.dumps(r).encode() + b"\n" for r in records)))


def test_profile_command(tmp_path, capsys):
    path = tmp_path / "06-Practitioner.ndjson.zst"
    write_zst(path, [PRAC1, PRAC2])
    assert cli.main(["profile", str(path)]) == 0
    out = capsys.readouterr().out
    assert "== Practitioner: 2 resources" in out and ".name[].family" in out
    assert cli.main(["profile", str(path), "--unmapped"]) == 0
    write_zst(path, [{**PRAC1, "birthDate": "1970-01-01"}])
    assert cli.main(["profile", str(path), "--unmapped"]) == 1
    assert ".birthDate" in capsys.readouterr().out


def test_missing_config_exits_nonzero(tmp_path, caplog):
    assert cli.main(["--config", str(tmp_path / "nope.toml"), "status"]) == 1
    assert "config file not found" in caplog.text


def fake_context(monkeypatch, tmp_path, calls, outcomes):
    config = parse_config(config_data(tmp_path, "https://example.test/downloads/manifest.json"))
    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr(cli, "build_context",
                        lambda cfg: SimpleNamespace(http=SimpleNamespace(close=lambda: calls.append("close"))))

    def stage(name):
        def run(ctx, *args, **kwargs):
            calls.append((name, args, kwargs))
            result = outcomes.get(name, Outcome.SUCCESS)
            if isinstance(result, Exception):
                raise result
            return result
        return run
    for name in ("download", "extract", "import"):
        monkeypatch.setattr(cli, f"run_{name}", stage(name))


def test_run_is_download_then_import(monkeypatch, tmp_path):
    calls = []
    fake_context(monkeypatch, tmp_path, calls, {})
    assert cli.main(["--config", "x", "run"]) == 0
    assert [c[0] for c in calls[:-1]] == ["download", "import"] and calls[-1] == "close"


def test_options_are_passed(monkeypatch, tmp_path):
    calls = []
    fake_context(monkeypatch, tmp_path, calls, {})
    assert cli.main(["--config", "x", "import", "--release", "2026-09-29", "--force"]) == 0
    assert calls[0] == ("import", (), {"release": date(2026, 9, 29), "force": True})
    assert cli.main(["--config", "x", "download", "--force"]) == 0
    assert calls[2] == ("download", (), {"force": True})


def test_failures_and_locks(monkeypatch, tmp_path):
    calls = []
    fake_context(monkeypatch, tmp_path, calls, {"import": StageFailed("boom"), "download": Outcome.LOCKED})
    assert cli.main(["--config", "x", "download"]) == 0
    assert cli.main(["--config", "x", "import"]) == 1


def test_format_status():
    catalog = FakeCatalog()
    r1, r2 = date(2026, 9, 22), date(2026, 9, 29)
    d1 = catalog.add_successful_run("DOWNLOAD", r1)
    i1 = catalog.add_successful_run("IMPORT", r1)
    d2 = catalog.add_successful_run("DOWNLOAD", r2)
    cfg = parse_config(config_data("/tmp", "https://x/manifest.json")).catalog
    text = cli.format_status(catalog, cfg, published=[r1])
    lines = text.splitlines()
    assert lines[0].split() == ["release", "download", "extract", "import", "published"]
    assert lines[1].split() == ["2026-09-29", str(d2.id), "-", "-", "no"]
    assert lines[2].split() == ["2026-09-22", str(d1.id), "-", str(i1.id), "yes"]

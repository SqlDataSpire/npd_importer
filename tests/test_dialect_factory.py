import pytest

from npd_loader.config import NpdDbConfig
from npd_loader.connections import build_connection
from npd_loader.dialect import dialect_for

CFG = NpdDbConfig(connection="data", raw_schema="npd_raw", schema="npd")


def test_postgres_dialect_selected():
    pg = build_connection("pg", {"type": "postgres", "server": "h:5432", "database": "npd", "UN": "u", "PW": "p"})
    d = dialect_for(pg, CFG)
    assert d.name == "postgres" and d.conninfo == "postgresql://u:p@h:5432/npd"


def test_unknown_engine_rejected():
    class Fake:
        class engine:
            class dialect:
                name = "sqlite"
    with pytest.raises(ValueError, match="sqlite"):
        dialect_for(Fake(), CFG)

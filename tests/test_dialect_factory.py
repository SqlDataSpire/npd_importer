import pytest

from npd_loader.config import ConfigError, NpdDbConfig
from npd_loader.connections import build_connection
from npd_loader.dialect import dialect_for

CFG = NpdDbConfig(connection="data", schema="npd")


def test_postgres_rejected():
    pg = build_connection("pg", {"type": "postgres", "server": "h:5432", "database": "npd", "UN": "u", "PW": "p"})
    with pytest.raises(ConfigError, match="SQL Server only"):
        dialect_for(pg, CFG)


def test_unknown_engine_rejected():
    class Fake:
        class engine:
            class dialect:
                name = "sqlite"
    with pytest.raises(ValueError, match="sqlite"):
        dialect_for(Fake(), CFG)

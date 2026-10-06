import pytest

from npd_loader.config import ConfigError
from npd_loader.connections import build_connection, read_env_file
from helpers import write_env_file

DOCS = {"data": {"type": "mssql", "server": "cssnpi", "database": "npd_dev", "trusted": "yes"},
        "pg": {"type": "postgres", "server": "db.example:5433", "database": "npd", "UN": "u", "PW": "p@ss"}}


def test_read_env_file(tmp_path):
    assert read_env_file(write_env_file(tmp_path / "database.env", DOCS)) == DOCS


def test_read_env_file_errors(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        read_env_file(str(tmp_path / "missing.env"))
    bad = tmp_path / "bad.env"
    bad.write_text("other = 'x'\n")
    with pytest.raises(ConfigError, match="databases"):
        read_env_file(str(bad))


def test_build_connection_types_and_no_stdout(capsys):
    mssql = build_connection("data", DOCS["data"])
    assert type(mssql).__name__ == "SqlConnectionObject"
    assert mssql.engine.dialect.name == "mssql"
    pg = build_connection("pg", DOCS["pg"])
    assert type(pg).__name__ == "PgConnectionObject"
    assert pg.engine.dialect.name == "postgresql" and pg.engine.dialect.driver == "psycopg2"
    assert pg.engine.url.render_as_string(hide_password=False) == "postgresql+psycopg2://u:p%40ss@db.example:5433/npd"
    assert capsys.readouterr().out == ""
    with pytest.raises(ConfigError, match="type"):
        build_connection("m", {"type": "mongo", "server": "x", "database": "y"})

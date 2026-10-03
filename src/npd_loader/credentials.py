"""Database credentials. Backend 'config' reads them from the config file; 1Password comes later."""
from __future__ import annotations

from dataclasses import dataclass

from psycopg.conninfo import make_conninfo

from npd_loader.config import Config, ConfigError


@dataclass(frozen=True)
class DbCredentials:
    user: str
    password: str


def get_db_credentials(config: Config, target: str) -> DbCredentials:
    """Return credentials for `target`, which is "npd_db" or "catalog"."""
    section = getattr(config, target)
    if config.credentials.backend == "config":
        if not section.user or section.password is None:
            raise ConfigError(f"[{target}] user and password are required with credentials backend 'config'")
        return DbCredentials(section.user, section.password)
    raise ConfigError(f"credentials backend {config.credentials.backend!r} is not available")


def conninfo(config: Config, target: str) -> str:
    section = getattr(config, target)
    creds = get_db_credentials(config, target)
    return make_conninfo(host=section.host, port=section.port, dbname=section.dbname,
                         user=creds.user, password=creds.password)

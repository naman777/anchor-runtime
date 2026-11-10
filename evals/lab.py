"""Create an isolated migrated schema for a local measurement."""

import json
import os
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]


def database_url() -> str:
    return os.environ["ANCHOR_TEST_DATABASE_URL"]


def lab_engine(url, schema):
    return create_engine(
        url,
        connect_args={"options": f"-csearch_path={schema}"},
        hide_parameters=True,
        pool_pre_ping=True,
    )


@contextmanager
def isolated_database():
    url = database_url()
    schema = f"anchor_lab_{uuid4().hex}"
    admin = create_engine(url, hide_parameters=True)
    engine = lab_engine(url, schema)
    try:
        with admin.begin() as connection:
            connection.execute(text(f"CREATE SCHEMA {schema}"))
        config = Config(str(ROOT / "alembic.ini"))
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield engine, schema
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    index = fraction * (len(values) - 1)
    lower = int(index)
    return values[lower] + (values[min(lower + 1, len(values) - 1)] - values[lower]) * (
        index - lower
    )


def write_json(path: Path, result: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )

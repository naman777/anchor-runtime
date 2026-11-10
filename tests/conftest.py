import os
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text


@pytest.fixture
def engine():
    url = os.environ.get("ANCHOR_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set ANCHOR_TEST_DATABASE_URL to run PostgreSQL integration tests")
    schema = f"anchor_test_{uuid4().hex}"
    admin = create_engine(url)
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with admin.begin() as connection:
            connection.execute(text(f"CREATE SCHEMA {schema}"))
        config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()

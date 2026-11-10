import os

from alembic import context
from sqlalchemy import create_engine

from anchor_runtime.storage import Base


def migrate(connection):
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    context.configure(
        url=os.environ["ANCHOR_DATABASE_URL"],
        target_metadata=Base.metadata,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()
elif (connection := context.config.attributes.get("connection")) is not None:
    migrate(connection)
else:
    engine = create_engine(os.environ["ANCHOR_DATABASE_URL"])
    with engine.connect() as connection:
        migrate(connection)

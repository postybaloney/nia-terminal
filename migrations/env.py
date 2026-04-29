"""
Alembic migration environment.

Reads DATABASE_URL from the application settings so migrations always
target the same database as the app — no separate URL config needed.

Usage
-----
# Run all pending migrations:
    alembic upgrade head

# Roll back one migration:
    alembic downgrade -1

# If tables were already created by init_db() (no migration history yet),
# stamp the database at the current head without re-running DDL:
    alembic stamp head

# Generate a new auto-detected migration after changing a model:
    alembic revision --autogenerate -m "describe change"
"""
from __future__ import annotations

import os
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

# ── Make sure the project root is on the path ─────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ── Load app settings for the DB URL ─────────────────────────────────────────
from config import settings  # noqa: E402

alembic_config = context.config
alembic_config.set_main_option("sqlalchemy.url", settings.database_url)

if alembic_config.config_file_name is not None:
    fileConfig(alembic_config.config_file_name)

# ── Import all models so their metadata is registered ────────────────────────
from db.models import Base          # noqa: E402, F401
from db.thesis_models import Thesis # noqa: E402, F401 — registers Thesis on Base.metadata

target_metadata = Base.metadata


# ── Offline mode (generates SQL without a live connection) ────────────────────

def run_migrations_offline() -> None:
    url = alembic_config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


# ── Online mode (runs against a live connection) ──────────────────────────────

def run_migrations_online() -> None:
    connectable = engine_from_config(
        alembic_config.get_section(alembic_config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

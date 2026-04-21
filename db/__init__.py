from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config import settings
from .models import Base

_engine = None
SessionLocal: sessionmaker | None = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(settings.database_url)
    return _engine


def _get_session_factory() -> sessionmaker:
    global SessionLocal
    if SessionLocal is None:
        SessionLocal = sessionmaker(bind=_get_engine(), autocommit=False, autoflush=False)
    return SessionLocal


# Initialise SessionLocal at import time so `from db import SessionLocal` works.
_get_session_factory()


def init_db() -> None:
    """Create all tables defined in the ORM models."""
    Base.metadata.create_all(bind=_get_engine())


@contextmanager
def get_session():
    """Context-manager that yields a SQLAlchemy Session and handles commit/rollback."""
    session: Session = _get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

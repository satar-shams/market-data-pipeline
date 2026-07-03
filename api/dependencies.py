# api/dependencies.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from config.settings import settings

# Separate engine from PostgresLoader's -- this one is read-only in intent
# (never used for INSERT/UPDATE), kept distinct so the API's connection
# pool sizing and lifecycle can be tuned independently from the ETL
# pipeline's write-heavy engine.
# Uses api_database_url, NOT database_url -- this connects as api_reader,
# a Postgres role with SELECT-only grants (see db/create_api_role.sql).
# The ETL pipeline's PostgresLoader uses the full-privilege postgres_user
# instead; the API should never share those write credentials.
engine = create_engine(
    settings.api_database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db() -> Session:
    """
    FastAPI dependency: yields a SQLAlchemy session, guarantees it's closed
    after the request completes even if the endpoint raises.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
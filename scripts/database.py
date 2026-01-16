from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from scripts.config import DB_CONNECTION_STRING
import logging

def get_engine():
    """Returns the SQLAlchemy engine."""
    return create_engine(DB_CONNECTION_STRING)

def get_connection():
    """Returns a raw database connection."""
    engine = get_engine()
    return engine.connect()

def get_session():
    """Returns a scoped session factory."""
    engine = get_engine()
    session_factory = sessionmaker(bind=engine)
    return scoped_session(session_factory)

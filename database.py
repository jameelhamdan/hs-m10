from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from config import Config




@contextmanager
def with_database(**kwargs):
    config_kwargs = {
        **Config.SQLALCHEMY_ENGINE_OPTIONS
    }
    config_kwargs.update(kwargs)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=create_engine(
        Config.SQLALCHEMY_DATABASE_URI,
        **config_kwargs,
    ))

    db = SessionLocal()

    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
import mongoengine
from mongoengine import ConnectionFailure
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


mongoengine.connect(
    alias='default',
    host=Config.MONGO_URI,
    maxPoolSize=50,
    connectTimeoutMS=5000,
    serverSelectionTimeoutMS=5000,
    socketTimeoutMS=30000,
    retryWrites=True
)

@contextmanager
def with_mongo_database(alias='default'):
    try:
        mongoengine.connect(
            alias=alias,
            host=Config.MONGO_URI,
            maxPoolSize=50,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000,
            socketTimeoutMS=30000,
            retryWrites=True
        )

        # Verify connection
        from mongoengine.connection import get_connection
        get_connection(alias).admin.command('ping')

        yield

    except ConnectionFailure as e:
        raise ConnectionError(f"Could not connect to MongoDB: {str(e)}")
    finally:
        mongoengine.disconnect(alias=alias)
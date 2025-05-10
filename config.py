import os
from urllib.parse import urlparse, urlunparse

from dotenv import load_dotenv
load_dotenv()

class Config:
    DATABASE_URL = os.getenv('DATABASE_URL')
    DB_SCHEMA = os.getenv('DB_SCHEMA', 'public')

    parsed = urlparse(DATABASE_URL)
    SQLALCHEMY_DATABASE_URI = urlunparse(parsed)

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'connect_args': {
            'options': f'-csearch_path={DB_SCHEMA}'
        }
    }

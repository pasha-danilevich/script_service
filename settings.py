import os.path
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from elasticsearch import Elasticsearch, AsyncElasticsearch
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

BASE_PATH = Path(__file__).parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # env_file=os.path.join(BASE_PATH, 'dev.env'),
                                      # env_file_encoding='utf-8',
                                      # env_ignore_empty=True,
                                      extra='ignore'
                                      )
    SERVER_HOST: str = "localhost"
    SERVER_PORT: int = 8105

    ES_HOST: str = 'https://10.0.248.6'
    ES_PORT: int = 9202
    ES_USERNAME: str = 'elastic'
    ES_PASSWORD: str = "lsuirn8jv3"

    DB_USERNAME: str = 'postgres'
    DB_PASSWORD: str = "kQ2jdb38JssjU"

    DB_URL: str = '10.0.248.11'
    DB_PORT: int = 5432
    DB_NAME: str = 'raw_data'
    DB_NAME_EMD: str = 'emd'

    REDIS_HOST: str = 'localhost'
    REDIS_PORT: int = 6379
    REDIS_CELERY_DB_INDEX: int = 10
    REDIS_PASS: str = '12345'

    RABBITMQ_HOST: str = 'localhost'
    RABBITMQ_USER: str = 'motodokt'
    RABBITMQ_PASS: str = 'motodokt'
    RABBITMQ_PORT: int = 5672
    QUEUE: str = 'test'
    broker_connection_timeout: int = 300

    BASE_DOWNLOADS_URL: str = 'http://localhost:8101/upload/service/'

    SWAGGER_LOGIN: str = 'raw_data'
    SWAGGER_PASSWORD: str = 'raw_data12345'


settings_promed = Settings(
    _env_file=os.path.join(BASE_PATH, "promed.env"), _env_file_encoding='utf-8'
)
settings = Settings(
    # _env_file=os.path.join(BASE_PATH, "home.env"), _env_file_encoding='utf-8'
)
settings_single_3 = Settings(
    _env_file=os.path.join(BASE_PATH, "single-3.env"), _env_file_encoding='utf-8'
)

TORTOISE_DIALECT = 'postgres'
SQLALCHEMY_DIALECT = 'postgresql'

DB_URL_TORTOISE = f'{TORTOISE_DIALECT}://{settings.DB_USERNAME}' \
                  f':{settings.DB_PASSWORD}@{settings.DB_URL}:' \
                  f'{settings.DB_PORT}/{settings.DB_NAME}'

DB_URL_SQLALCHEMY = f'{SQLALCHEMY_DIALECT}://{settings.DB_USERNAME}' \
                    f':{settings.DB_PASSWORD}@{settings.DB_URL}:' \
                    f'{settings.DB_PORT}/{settings.DB_NAME}'

DB_URL_SQLALCHEMY_PROMED = f'{SQLALCHEMY_DIALECT}://{settings_promed.DB_USERNAME}' \
                           f':{settings_promed.DB_PASSWORD}@{settings_promed.DB_URL}:' \
                           f'{settings_promed.DB_PORT}/{settings_promed.DB_NAME}'

DB_URL_SQLALCHEMY_PROMED_EMD = f'{SQLALCHEMY_DIALECT}://{settings_promed.DB_USERNAME}' \
                               f':{settings_promed.DB_PASSWORD}@{settings_promed.DB_URL}:' \
                               f'{settings_promed.DB_PORT}/{settings_promed.DB_NAME_EMD}'

DB_URL_SQLALCHEMY_SMP = f'{SQLALCHEMY_DIALECT}://{settings.DB_USERNAME}' \
                        f':{settings.DB_PASSWORD}@{settings.DB_URL}:' \
                        f'{settings.DB_PORT}/smp_data'

DB_URL_SQLALCHEMY_SINGLE_3 = f'{SQLALCHEMY_DIALECT}://{settings_single_3.DB_USERNAME}' \
                               f':{settings_single_3.DB_PASSWORD}@{settings_single_3.DB_URL}:' \
                               f'{settings_single_3.DB_PORT}/{settings_single_3.DB_NAME_EMD}'


es_host = f"{settings.ES_HOST}:{settings.ES_PORT}"
user = (settings.ES_USERNAME, settings.ES_PASSWORD)

logach_user = 'informservice'
logach_pass = '$mone0y!'

user1 = (logach_user, logach_pass)


@asynccontextmanager
async def elastic_connect():
    aes = AsyncElasticsearch(hosts=es_host, timeout=30, basic_auth=user, verify_certs=False)
    try:
        yield aes
    finally:
        await aes.close()


@contextmanager
def elastic_sync_connect():
    print(es_host)
    es = Elasticsearch(hosts=es_host,
                       basic_auth=user,
                       verify_certs=False,
                       request_timeout=120,
                       ssl_show_warn=False)
    try:
        yield es
    finally:
        es.close()


sql_alchemy_conn_kwargs = dict(
    echo=False,
    pool_size=20,
    max_overflow=0,
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        "options": "-c lock_timeout=300000 -c statement_timeout=3000000", },
    pool_pre_ping=True

)

engine = create_engine(DB_URL_SQLALCHEMY,
                       **sql_alchemy_conn_kwargs
                       )
engine_promed = create_engine(DB_URL_SQLALCHEMY_PROMED,
                              **sql_alchemy_conn_kwargs
                              )

engine_promed_emd = create_engine(DB_URL_SQLALCHEMY_PROMED_EMD,
                                  **sql_alchemy_conn_kwargs
                                  )

engine_smp = create_engine(DB_URL_SQLALCHEMY_SMP,
                           **sql_alchemy_conn_kwargs
                           )

engine_single_3 = create_engine(DB_URL_SQLALCHEMY_SINGLE_3,
                           **sql_alchemy_conn_kwargs
                           )

pg_parus = 'postgresql://parus_ro:parus_ro@178.34.187.186:4523/ppc'

engine_parus = create_engine(pg_parus,
                             **sql_alchemy_conn_kwargs
                             )
# import os
# import pandas as pd
#
# from sqlalchemy import text
#
# from settings import BASE_PATH, engine

# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)


# import warnings
# warnings.simplefilter(action="ignore", category=FutureWarning)

DATABASE_CONFIG = {
    "connections": {"default":
        {
            'engine': 'tortoise.backends.asyncpg',
            'credentials': {'host': settings.DB_URL,
                            'port': settings.DB_PORT,
                            'user': settings.DB_USERNAME,
                            'password': settings.DB_PASSWORD,
                            'database': settings.DB_NAME,
                            # 'minsize': 1,
                            # 'maxsize': 500,
                            'max_inactive_connection_lifetime': 3,
                            'max_queries': 500000,
                            'statement_cache_size': 0
                            },
        }
    },
    "apps": {
        "models": {
            "models":
                [
                    "tables.tables",
                ],
            "default_connection": "default",
        },
    },
}

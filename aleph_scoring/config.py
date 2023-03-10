import logging
from datetime import timedelta
from typing import Optional

from pydantic import BaseSettings, HttpUrl


class Settings(BaseSettings):
    NODE_DATA_HOST: str = "https://official.aleph.cloud"
    NODE_DATA_ADDR: str = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10"

    ALLOWED_METRICS_SENDER = "0x4d741d44348B21e97000A8C9f07Ee34110F7916F"

    DATABASE_USER = "aleph"
    DATABASE_PASSWORD = "569b8f23-0de6-4927-a15d-7157d8583e43"
    DATABASE_DATABASE = "aleph"
    DATABASE_HOST = "127.0.0.1"
    DATABASE_PORT = 5432

    ALEPH_POST_TYPE_CHANNEL: Optional[str] = "aleph-scoring"
    ALEPH_POST_TYPE_METRICS: str = "test-aleph-scoring-metrics"
    ALEPH_POST_TYPE_SCORES: str = "test-aleph-scoring-scores"
    ASN_DB_REFRESH_PERIOD_DAYS: int = 1
    DAEMON_MODE_PERIOD_HOURS: int = 24
    EXPORT_DATAFRAME: bool = False
    ETHEREUM_PRIVATE_KEY: str = (
        "0x95c6bc829ddf6a83b5d8b228db2942fe828802fb63f412586ea7c2d0036b4020"
    )
    HTTP_REQUEST_TIMEOUT: float = 10.0
    LOGGING_LEVEL: int = logging.DEBUG
    SENTRY_DSN: Optional[HttpUrl] = None

    VERSION_GRACE_PERIOD: timedelta = timedelta(weeks=2)
    SCORE_METRICS_PERIOD: timedelta = timedelta(weeks=4)

    class Config:
        env_file = ".env"
        env_prefix = "ALEPH_SCORING_"


settings = Settings()

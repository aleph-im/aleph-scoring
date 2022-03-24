import logging
from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    SENTRY_DSN: Optional[str] = None
    LOGGING_LEVEL: int = logging.INFO
    TIMEOUT_LIMIT_CCN = 2.
    TIMEOUT_LIMIT_CRN = 10.
    EXPORT_DATAFRAME = False

    class Config:
        env_file = '.env'
        env_prefix = "ALEPH_SCORING_"


settings = Settings()

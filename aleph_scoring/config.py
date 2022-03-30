import logging
from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    NODE_DATA_HOST = "https://official.aleph.cloud"
    NODE_DATA_ADDR = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10"
    NODE_DATA_TEMPLATE = "{}/api/v0/aggregates/{}.json?keys=corechannel&limit=50"

    SENTRY_DSN: Optional[str] = None
    LOGGING_LEVEL: int = logging.DEBUG
    HTTP_REQUEST_TIMEOUT = 2.0
    EXPORT_DATAFRAME = False

    @property
    def node_data_url(self):
        return self.NODE_DATA_TEMPLATE.format(self.NODE_DATA_HOST, self.NODE_DATA_ADDR)

    class Config:
        env_file = ".env"
        env_prefix = "ALEPH_SCORING_"


settings = Settings()

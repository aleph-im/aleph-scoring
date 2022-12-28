import logging
from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    NODE_DATA_HOST: str = "https://official.aleph.cloud"
    NODE_DATA_ADDR: str = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10"
    NODE_DATA_TEMPLATE: str = "{}/api/v0/aggregates/{}.json?keys=corechannel&limit=50"

    ALEPH_POST_TYPE_CHANNEL: Optional[str] = "aleph-scoring"
    ALEPH_POST_TYPE_METRICS: str = "test-aleph-scoring-metrics"
    ALEPH_POST_TYPE_SCORES: str = "test-aleph-scoring-scores"
    ASN_DB_REFRESH_PERIOD_DAYS: int = 1
    EXPORT_DATAFRAME: bool = False
    ETHEREUM_PRIVATE_KEY: str = (
        "0x95c6bc829ddf6a83b5d8b228db2942fe828802fb63f412586ea7c2d0036b4020"
    )
    HTTP_REQUEST_TIMEOUT: float = 2.0
    LOGGING_LEVEL: int = logging.DEBUG
    SENTRY_DSN: Optional[str] = None
    VERSION_SCORING_GRACE_PERIOD_DAYS: int = 14

    @property
    def node_data_url(self):
        return self.NODE_DATA_TEMPLATE.format(self.NODE_DATA_HOST, self.NODE_DATA_ADDR)

    class Config:
        env_file = ".env"
        env_prefix = "ALEPH_SCORING_"


settings = Settings()

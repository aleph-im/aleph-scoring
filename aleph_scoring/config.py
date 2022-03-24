from pydantic import BaseSettings


class Settings(BaseSettings):
    SENTRY_DSN: str = ""
    LOGGING_LEVEL: int = 10
    TIMEOUT_LIMIT_CCN: int = 2
    TIMEOUT_LIMIT_CRN: int = 10
    EXPORT_DATAFRAME: bool = False

    class Config:
        env_file = '.env'


settings = Settings()

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import List


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Database ─────────────────────────────────────────────────────────────
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "market_data"
    postgres_user: str
    postgres_password: str

    # ── Pipeline ─────────────────────────────────────────────────────────────
    tickers: str = "AAPL,MSFT,GOOGL,AMZN,SPY"   # str, not List[str]
    lookback_days: int = 365

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: str = "INFO"

    @property
    def tickers_list(self) -> List[str]:
        """Always use settings.tickers_list in code, not settings.tickers."""
        return [t.strip().upper() for t in self.tickers.split(",")]

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
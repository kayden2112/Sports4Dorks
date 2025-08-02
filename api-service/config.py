from pydantic import BaseSettings, PostgresDsn

class Settings(BaseSettings):
    DATABASE_URL: PostgresDsn        
    ODDS_API_KEY: str         
    ETL_INTERVAL_SECONDS: int = 5400 
    DEFAULT_RATE_LIMIT: str = "100/minute"
    ALLOWED_ORIGINS: list[str] = ["*"]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()

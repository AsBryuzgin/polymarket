import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    app_env: str = os.getenv("APP_ENV", "dev")
    app_debug: bool = os.getenv("APP_DEBUG", "true").lower() == "true"
    gamma_base_url: str = os.getenv("POLY_GAMMA_BASE_URL", "https://gamma-api.polymarket.com")
    clob_base_url: str = os.getenv("POLY_CLOB_BASE_URL", "https://clob.polymarket.com")


settings = Settings()

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import tomllib

from dotenv import load_dotenv
from py_clob_client_v2.client import ClobClient
from py_builder_signing_sdk.config import BuilderConfig
from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

load_dotenv()

EXECUTOR_CONFIG_ENV_VAR = "POLY_EXECUTOR_CONFIG_PATH"


@dataclass
class ExecutorEnv:
    clob_host: str
    relayer_url: str
    chain_id: int
    private_key: str
    funder_address: str
    signature_type: int
    builder_api_key: str
    builder_secret: str
    builder_passphrase: str


def load_executor_env() -> ExecutorEnv:
    return ExecutorEnv(
        clob_host=os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com"),
        relayer_url=os.getenv("POLY_RELAYER_URL", "https://relayer-v2.polymarket.com"),
        chain_id=int(os.getenv("POLY_CHAIN_ID", "137")),
        private_key=os.getenv("POLY_PRIVATE_KEY", ""),
        funder_address=os.getenv("POLY_FUNDER_ADDRESS", ""),
        signature_type=int(os.getenv("POLY_SIGNATURE_TYPE", "1")),
        builder_api_key=os.getenv("POLY_BUILDER_API_KEY", ""),
        builder_secret=os.getenv("POLY_BUILDER_SECRET", ""),
        builder_passphrase=os.getenv("POLY_BUILDER_PASSPHRASE", ""),
    )


def load_executor_config(path: str = "config/executor.toml") -> dict:
    p = Path(os.getenv(EXECUTOR_CONFIG_ENV_VAR, path) if path == "config/executor.toml" else path)
    if not p.exists():
        return {}
    with p.open("rb") as f:
        return tomllib.load(f)


def validate_env(env: ExecutorEnv) -> list[str]:
    missing = []

    if not env.private_key:
        missing.append("POLY_PRIVATE_KEY")
    if not env.funder_address:
        missing.append("POLY_FUNDER_ADDRESS")
    if not env.builder_api_key:
        missing.append("POLY_BUILDER_API_KEY")
    if not env.builder_secret:
        missing.append("POLY_BUILDER_SECRET")
    if not env.builder_passphrase:
        missing.append("POLY_BUILDER_PASSPHRASE")

    return missing


def build_clob_client(env: ExecutorEnv) -> ClobClient:
    client = ClobClient(
        host=env.clob_host,
        key=env.private_key,
        chain_id=env.chain_id,
        signature_type=env.signature_type,
        funder=env.funder_address,
    )
    return client


def build_builder_config(env: ExecutorEnv) -> BuilderConfig | None:
    if not env.builder_api_key or not env.builder_secret or not env.builder_passphrase:
        return None
    creds = BuilderApiKeyCreds(
        key=env.builder_api_key,
        secret=env.builder_secret,
        passphrase=env.builder_passphrase,
    )
    return BuilderConfig(local_builder_creds=creds)


def health_snapshot() -> dict:
    env = load_executor_env()
    config = load_executor_config()
    missing = validate_env(env)

    snapshot = {
        "env_ok": len(missing) == 0,
        "missing_env": missing,
        "executor_config_path": os.getenv(EXECUTOR_CONFIG_ENV_VAR, "config/executor.toml"),
        "clob_host": env.clob_host,
        "relayer_url": env.relayer_url,
        "chain_id": env.chain_id,
        "signature_type": env.signature_type,
        "funder_address_present": bool(env.funder_address),
        "private_key_present": bool(env.private_key),
        "builder_api_key_present": bool(env.builder_api_key),
        "builder_secret_present": bool(env.builder_secret),
        "builder_passphrase_present": bool(env.builder_passphrase),
        "config_loaded": bool(config),
        "config_sections": list(config.keys()) if isinstance(config, dict) else [],
    }

    if missing:
        snapshot["clob_client_ok"] = False
        snapshot["api_creds_ok"] = False
        return snapshot

    try:
        client = build_clob_client(env)
        snapshot["clob_client_ok"] = True
    except Exception as e:
        snapshot["clob_client_ok"] = False
        snapshot["clob_client_error"] = str(e)
        snapshot["api_creds_ok"] = False
        return snapshot

    try:
        creds = client.create_or_derive_api_key()
        snapshot["api_creds_ok"] = True
        snapshot["derived_api_key_present"] = bool(getattr(creds, "api_key", None))
    except Exception as e:
        snapshot["api_creds_ok"] = False
        snapshot["api_creds_error"] = str(e)

    return snapshot

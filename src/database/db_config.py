"""
C:\Economy\Invest\TrendMaster\src\database\db_config.py
Database Configuration Module
Handles .env validation via Pydantic and URL construction via yarl.
"""

import logging
from dataclasses import dataclass
from typing import Optional, Any

from pydantic import Field, AliasChoices, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from yarl import URL

logger = logging.getLogger(__name__)


class _DatabaseEnvSchema(BaseSettings):
    """Internal Pydantic schema to validate .env values."""

    user: str = Field(validation_alias=AliasChoices("DB_USER", "user"))
    password: str = Field(default="", validation_alias=AliasChoices("DB_PASSWORD", "password"))
    host: str = Field(default="127.0.0.1", validation_alias=AliasChoices("DB_HOST", "host"))
    port: int = Field(default=3306, ge=1, le=65535, validation_alias=AliasChoices("DB_PORT", "port"))
    database: str = Field(default="trend_master", validation_alias=AliasChoices("DB_NAME", "database"))
    scheme: str = Field(default="mysql+pymysql", validation_alias=AliasChoices("DB_SCHEME", "scheme"))

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,  # allow plain field names alongside aliases
    )

    @field_validator("scheme")
    @classmethod
    def _validate_scheme(cls, v: str) -> str:
        allowed = {"mysql+pymysql", "mysql+aiomysql", "postgresql+psycopg2", "postgresql+asyncpg", "sqlite"}
        if v not in allowed:
            raise ValueError(f"Unsupported scheme '{v}'. Allowed: {allowed}")
        return v


@dataclass(frozen=True)
class DatabaseConfig:
    """Immutable database settings with built-in Pydantic loading."""

    user: str
    password: str = ""
    host: str = "127.0.0.1"
    port: int = 3306
    database: str = "trend_master"
    scheme: str = "mysql+pymysql"

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Load and validate configuration from environment / .env file."""
        try:
            settings = _DatabaseEnvSchema()
            return cls(**settings.model_dump())
        except ValidationError as e:
            logger.error("Database configuration validation failed: %s", e)
            raise

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def build_url(self, database: Optional[str] = None, *, hide_password: bool = False) -> str:
        """
        Return a fully-qualified SQLAlchemy connection URL.

        Args:
            database: Override the default database name.
            hide_password: Replace the password with ``***`` (safe for logging).
        """
        db = database or self.database
        password = "***" if hide_password else self.password
        return str(
            URL.build(
                scheme=self.scheme,
                user=self.user,
                password=password,
                host=self.host,
                port=self.port,
                path=f"/{db}" if db else "",
            )
        )

    @property
    def masked_url(self) -> str:
        """Connection URL with the password redacted — safe for logging."""
        return self.build_url(hide_password=True)

    # ------------------------------------------------------------------
    # Safety
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # prevent password leaking into logs/tracebacks
        return (
            f"DatabaseConfig(user={self.user!r}, host={self.host!r}, "
            f"port={self.port!r}, database={self.database!r}, scheme={self.scheme!r}, "
            f"password='***')"
        )


@dataclass(frozen=True)
class PoolConfig:
    pool_size: int = 10
    max_overflow: int = 20
    pool_recycle: int = 1800
    pool_pre_ping: bool = True

    def engine_kwargs(self) -> dict[str, Any]:
        return {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_recycle": self.pool_recycle,
            "pool_pre_ping": self.pool_pre_ping,
            "pool_reset_on_return": "rollback",
        }


@dataclass(frozen=True)
class EngineConfig:
    echo: bool = False

    def engine_kwargs(self) -> dict[str, Any]:
        return {"echo": self.echo, "future": True}

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    @staticmethod
    def combined_kwargs(pool: PoolConfig, engine: "EngineConfig") -> dict[str, Any]:
        """
        Merge pool and engine kwargs into a single dict for ``create_engine``.

        Example::

            engine = create_engine(
                db.build_url(),
                **EngineConfig.combined_kwargs(pool_cfg, engine_cfg),
            )
        """
        return {**pool.engine_kwargs(), **engine.engine_kwargs()}
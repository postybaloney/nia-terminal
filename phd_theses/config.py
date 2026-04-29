from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Database
    database_url: str

    # LLM backend
    llm_backend: str = "groq"
    llm_model: str = ""
    groq_api_key: str = ""
    gemini_api_key: str = ""
    huggingface_token: str = ""
    anthropic_api_key: str = ""
    ollama_host: str = "http://localhost:11434"

    # EPO OPS
    epo_client_id: str = ""
    epo_client_secret: str = ""

    # Lens.org
    lens_api_key: str = ""

    # BigQuery
    bigquery_project_id: str = ""
    google_application_credentials: str = ""

    # Patent search
    search_queries: str = (
        "neurotech neural stimulation brain electrode,"
        "brain computer interface cortical,"
        "neural implant deep brain stimulation,"
        "neuroprosthetics motor cortex,"
        "medical device biosensor implantable"
    )
    backfill_from: str = "2022-01-01"

    # Thesis search
    thesis_queries: str = (
        "neural interface hardware,"
        "brain computer interface,"
        "implantable medical device,"
        "neuroprosthetics,"
        "medical signal processing deep learning,"
        "neuromorphic computing,"
        "biosensor wearable,"
        "surgical robotics,"
        "medical image segmentation"
    )
    # Earliest year to include theses from
    thesis_since_year: int = 2018

    # Optional extra keywords for hardware/software relevance filter
    # Comma-separated, added on top of the built-in keyword lists
    thesis_extra_keywords: str = ""

    # EThOS API key (optional — basic search works without it)
    ethos_api_key: str = ""

    # Scheduler
    schedule_cron: str = "0 2 * * *"

    # Limits
    per_page: int = 50
    analysis_min_new: int = 5

    @field_validator("database_url")
    @classmethod
    def validate_db(cls, v: str) -> str:
        if not v.startswith("postgresql"):
            raise ValueError("DATABASE_URL must be a PostgreSQL connection string")
        return v

    @property
    def query_list(self) -> list[str]:
        return [q.strip() for q in self.search_queries.split(",") if q.strip()]

    @property
    def thesis_query_list(self) -> list[str]:
        return [q.strip() for q in self.thesis_queries.split(",") if q.strip()]

    @property
    def thesis_extra_keywords_list(self) -> list[str]:
        return [k.strip() for k in self.thesis_extra_keywords.split(",") if k.strip()]

    @property
    def epo_enabled(self) -> bool:
        return bool(self.epo_client_id and self.epo_client_secret)

    @property
    def lens_enabled(self) -> bool:
        return bool(self.lens_api_key)

    @property
    def bigquery_enabled(self) -> bool:
        return bool(self.bigquery_project_id)

    @property
    def llm_key_configured(self) -> bool:
        backend = (self.llm_backend or "groq").lower()
        checks = {
            "groq": bool(self.groq_api_key),
            "gemini": bool(self.gemini_api_key),
            "ollama": True,
            "huggingface": bool(self.huggingface_token),
            "anthropic": bool(self.anthropic_api_key),
        }
        return checks.get(backend, False)


settings = Settings()  # type: ignore[call-arg]

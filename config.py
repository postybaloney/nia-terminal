from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Database
    database_url: str

    # ── LLM backend ───────────────────────────────────────────────────────────
    # Set ONE of these depending on your chosen backend:
    #
    #   LLM_BACKEND=groq        → set GROQ_API_KEY
    #   LLM_BACKEND=gemini      → set GEMINI_API_KEY
    #   LLM_BACKEND=ollama      → no key needed, just install Ollama
    #   LLM_BACKEND=huggingface → set HUGGINGFACE_TOKEN
    #   LLM_BACKEND=anthropic   → set ANTHROPIC_API_KEY (paid)

    llm_backend: str = "groq"   # default to best free option
    llm_model: str = "llama-3.3-70b-versatile"         # leave blank to use the backend's default model

    # API keys — only the one matching your LLM_BACKEND is required
    groq_api_key: str = "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"
    gemini_api_key: str = ""
    huggingface_token: str = ""
    anthropic_api_key: str = ""  # only needed if LLM_BACKEND=anthropic

    # Ollama settings (only if LLM_BACKEND=ollama)
    ollama_host: str = "http://localhost:11434"  # default Ollama address

    # EPO OPS
    epo_client_id: str = ""
    epo_client_secret: str = ""

    # Lens.org
    lens_api_key: str = ""

    # USPTO Open Data Portal (PatentsView replacement)
    # Register free at https://data.uspto.gov/apis/getting-started
    uspto_api_key: str = ""

    # BigQuery
    bigquery_project_id: str = ""
    google_application_credentials: str = ""

    # PostgreSQL (used by docker-compose / direct connection tooling)
    postgres_password: str = "changeme"

    # SMTP / email notifications
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = "greatarcher5@gmail.com"
    smtp_password: str = "Corporaldragon5"
    digest_email_to: str = "pdesai@epsilonsolutionsllc.com"

    # Search
    search_queries: str = (
        "neurotech neural stimulation brain electrode,"
        "brain computer interface cortical,"
        "neural implant deep brain stimulation,"
        "neuroprosthetics motor cortex,"
        "medical device biosensor implantable"
    )
    backfill_from: str = "2022-01-01"

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
    def epo_enabled(self) -> bool:
        return bool(self.epo_client_id and self.epo_client_secret)

    @property
    def lens_enabled(self) -> bool:
        return bool(self.lens_api_key)

    @property
    def uspto_enabled(self) -> bool:
        return bool(self.uspto_api_key)

    @property
    def bigquery_enabled(self) -> bool:
        return bool(self.bigquery_project_id)

    @property
    def llm_key_configured(self) -> bool:
        """Verify the active backend has its required credential."""
        backend = (self.llm_backend or "groq").lower()
        checks = {
            "groq": bool(self.groq_api_key),
            "gemini": bool(self.gemini_api_key),
            "ollama": True,  # no key needed
            "huggingface": bool(self.huggingface_token),
            "anthropic": bool(self.anthropic_api_key),
        }
        return checks.get(backend, False)


settings = Settings()  # type: ignore[call-arg]

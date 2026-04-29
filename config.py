from __future__ import annotations

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    All configuration is loaded from environment variables or .env file.
    Never put real credentials here — use .env (gitignored) or Railway Variables.
    See .env.example for the full list of supported variables.
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # ── Database ──────────────────────────────────────────────────────────────
    database_url: str

    # ── LLM backend ───────────────────────────────────────────────────────────
    # Set ONE of these depending on your chosen backend:
    #   LLM_BACKEND=groq        → set GROQ_API_KEY
    #   LLM_BACKEND=gemini      → set GEMINI_API_KEY
    #   LLM_BACKEND=ollama      → no key needed
    #   LLM_BACKEND=huggingface → set HUGGINGFACE_TOKEN
    #   LLM_BACKEND=anthropic   → set ANTHROPIC_API_KEY (paid)
    llm_backend: str = "groq"
    llm_model: str = "llama-3.3-70b-versatile"

    # API keys — only the one matching LLM_BACKEND is required
    groq_api_key: str = ""
    gemini_api_key: str = ""
    huggingface_token: str = ""
    anthropic_api_key: str = ""
    ollama_host: str = "http://localhost:11434"

    # ── Patent sources ────────────────────────────────────────────────────────
    # USPTO Open Data Portal (replaced PatentsView March 2026)
    # Register free at https://data.uspto.gov/apis/getting-started
    # Endpoint: https://api.uspto.gov/api/v1/patent/applications/search
    # Auth: X-API-KEY header
    uspto_api_key: str = ""

    # EPO OPS — register free at https://developers.epo.org
    epo_client_id: str = ""
    epo_client_secret: str = ""

    # Lens.org — free tier at https://lens.org/lens/user/subscriptions
    lens_api_key: str = ""

    # Google BigQuery (optional)
    bigquery_project_id: str = ""
    google_application_credentials: str = ""

    # ── PostgreSQL (docker-compose / direct tooling) ──────────────────────────
    postgres_password: str = "changeme"

    # ── Notifications ─────────────────────────────────────────────────────────
    slack_webhook_url: str = ""

    # Resend (recommended for Railway — HTTP API, no port restrictions)
    # Sign up free at https://resend.com, set RESEND_API_KEY + RESEND_FROM
    resend_api_key: str = ""
    resend_from: str = ""          # e.g. "Patent Intel <noreply@yourdomain.com>"

    # SMTP fallback (works locally; Railway blocks outbound SMTP ports)
    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    digest_email_to: str = ""

    # ── Search ────────────────────────────────────────────────────────────────
    search_queries: str = (
        # ── Neurotech: stimulation & implants ────────────────────────────────
        "neural stimulation brain electrode implant,"
        "deep brain stimulation Parkinson tremor,"
        "spinal cord stimulation neuromodulation chronic pain,"
        "brain computer interface BCI cortical neural decoding,"
        "neuroprosthetics motor cortex limb rehabilitation,"
        "transcranial magnetic stimulation TMS tDCS,"
        "vagus nerve stimulation epilepsy depression,"
        "cochlear implant auditory brainstem prosthesis,"
        "retinal prosthesis visual implant epiretinal,"
        "microelectrode array neural recording electrophysiology,"
        "closed loop neural feedback adaptive stimulation,"
        # ── Neurotech: sensing & biotech overlap ─────────────────────────────
        "optogenetics neural circuit photostimulation,"
        "EEG ECoG brain signal electrode seizure,"
        "neurotransmitter biosensor brain chemistry,"
        # ── Cardiac & vascular devices ────────────────────────────────────────
        "cardiac pacemaker implantable defibrillator ICD,"
        "coronary stent drug eluting angioplasty,"
        "heart valve prosthetic transcatheter TAVR,"
        "cardiac ablation electrophysiology catheter,"
        "left ventricular assist device LVAD heart failure,"
        # ── Implantable & diagnostic devices ─────────────────────────────────
        "implantable biosensor continuous monitoring glucose,"
        "wearable biosensor physiological health monitoring,"
        "drug delivery implantable controlled release polymer,"
        "orthopedic implant joint replacement bone fixation,"
        "surgical robot minimally invasive laparoscopic,"
        "intraocular lens ophthalmology cataract vitreous,"
        "hearing aid cochlear auditory signal processing,"
        # ── AI & digital health ───────────────────────────────────────────────
        "machine learning medical imaging diagnosis radiology,"
        "AI pathology cancer detection deep learning,"
        "digital biomarker remote patient monitoring wearable,"
        "federated learning healthcare clinical data privacy"
    )
    backfill_from: str = "2020-01-01"

    # ── Scheduler ────────────────────────────────────────────────────────────
    schedule_cron: str = "0 2 * * *"

    # ── Limits ────────────────────────────────────────────────────────────────
    per_page: int = 50
    analysis_min_new: int = 5

    # ── Validators ────────────────────────────────────────────────────────────
    @field_validator("database_url")
    @classmethod
    def validate_db(cls, v: str) -> str:
        # Railway (and some other platforms) provide "postgres://" but
        # SQLAlchemy 2.0 requires the "postgresql://" scheme.
        if v.startswith("postgres://"):
            v = v.replace("postgres://", "postgresql://", 1)
        if not v.startswith("postgresql"):
            raise ValueError("DATABASE_URL must be a PostgreSQL connection string")
        return v

    # ── Derived properties ────────────────────────────────────────────────────
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
            "ollama": True,
            "huggingface": bool(self.huggingface_token),
            "anthropic": bool(self.anthropic_api_key),
        }
        return checks.get(backend, False)


settings = Settings()  # type: ignore[call-arg]

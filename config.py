from __future__ import annotations

from pydantic import field_validator, model_validator
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
    # Empty means "let the backend choose its own current default", which is
    # the safe setting: providers retire models (Groq retired
    # llama-3.3-70b-versatile on 2026-08-16) and a hardcoded name becomes a
    # 404 on every call. analysis._RETIRED_MODELS also remaps known-dead names.
    llm_model: str = ""

    # Tokens-per-minute budget for the configured backend, used to pace batch
    # jobs so they fit the limit instead of firing into 429s and retrying.
    # Groq's free tier is 8,000 TPM per model. Raise it if you upgrade.
    llm_tokens_per_minute: int = 8000

    # Optional model override for the bulk entity-affect pass. EMPTY means
    # "use the same model as everything else" — the safe default, because
    # silently downgrading extraction quality is not something a config file
    # should decide on its own. Set it only if you want affect on a separate
    # model: Groq rate limits are PER MODEL, so e.g. AFFECT_MODEL=
    # openai/gpt-oss-20b gives the affect pass its own 8k TPM budget instead
    # of sharing one with the digests.
    affect_model: str = ""

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

    # Lens.org — REMOVED from the active pipeline 2026-08: the free API tier is
    # noncommercial-only. Field retained so old .env files still parse. Re-enable
    # only under a signed Lens Commercial Use Agreement.
    lens_api_key: str = ""

    # openFDA — optional API key lifts rate limits (free at open.fda.gov/apis/authentication)
    openfda_api_key: str = ""

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

    # ── Thesis sources ────────────────────────────────────────────────────────
    # Add your email as OPENALEX_EMAIL to join OpenAlex polite pool (100k req/day)
    openalex_email: str = ""

    # EThOS API key (optional — basic search works without it)
    ethos_api_key: str = ""

    # Thesis search queries (comma-separated)
    thesis_queries: str = (
        "neural interface hardware implant,"
        "brain computer interface BCI decoding,"
        "deep brain stimulation closed loop,"
        "neuroprosthetics motor cortex rehabilitation,"
        "cochlear retinal prosthesis auditory visual,"
        "implantable medical device biocompatible,"
        "microelectrode array neural recording,"
        "medical signal processing deep learning EEG,"
        "neuromorphic computing chip hardware,"
        "biosensor wearable continuous monitoring,"
        "surgical robot minimally invasive,"
        "medical image segmentation diagnosis AI,"
        "spinal cord stimulation pain management,"
        "cardiac pacemaker defibrillator implant,"
        "federated learning clinical healthcare privacy"
    )

    # Earliest year to include theses from
    thesis_since_year: int = 2018

    # Optional extra keywords for hardware/software relevance filter (comma-separated)
    thesis_extra_keywords: str = ""

    # ── Current signals (grants, trials, FDA actions) ─────────────────────────
    # Compact search terms — these APIs want short phrases, not the long
    # patent-style query strings below.
    signal_queries: str = (
        "neurostimulation,"
        "brain-computer interface,"
        "deep brain stimulation,"
        "neuromodulation,"
        "vagus nerve stimulation,"
        "transcranial magnetic stimulation,"
        "EEG monitoring,"
        "neuroprosthesis,"
        "cochlear implant,"
        "intracortical electrode,"
        "seizure detection,"
        "sleep wearable"
    )
    signal_since: str = "2025-01-01"
    signal_schedule_cron: str = "0 4 * * *"   # signal ingest — default: 04:00 UTC daily

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
    schedule_cron: str = "0 2 * * *"          # patent ingest — default: 02:00 UTC daily
    thesis_schedule_cron: str = "0 3 * * 0"   # thesis ingest — default: 03:00 UTC Sundays

    # ── Limits ────────────────────────────────────────────────────────────────
    per_page: int = 50
    analysis_min_new: int = 5

    # ── Validators ────────────────────────────────────────────────────────────
    @model_validator(mode="before")
    @classmethod
    def _treat_empty_env_as_unset(cls, values):
        """
        Drop environment variables that arrived as empty strings, so the field
        default applies instead of being parsed.

        This exists because of how GitHub Actions handles secrets: referencing
        a secret that has NOT been set yields an empty string rather than
        omitting the variable. So a workflow line like

            SMTP_PORT: ${{ secrets.SMTP_PORT }}

        sets SMTP_PORT="" when the secret is absent, and pydantic then fails
        with "Input should be a valid integer, unable to parse string as an
        integer" — killing the run at `main.py init`, before any ingestion.

        Every non-string setting is exposed to this (smtp_port, per_page,
        analysis_min_new, thesis_since_year), so the guard is applied class-wide
        rather than per-field. Semantically "" and "unset" mean the same thing
        for configuration, and for the string settings the default is usually ""
        anyway, so no behaviour changes.
        """
        if isinstance(values, dict):
            return {
                k: v for k, v in values.items()
                if not (isinstance(v, str) and v.strip() == "")
            }
        return values

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
    def thesis_query_list(self) -> list[str]:
        return [q.strip() for q in self.thesis_queries.split(",") if q.strip()]

    @property
    def signal_query_list(self) -> list[str]:
        return [q.strip() for q in self.signal_queries.split(",") if q.strip()]

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

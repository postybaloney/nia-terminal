FROM python:3.11-slim

# System deps for lxml and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Non-root user for security
RUN useradd -m -u 1001 patent && chown -R patent:patent /app
USER patent

# Default: run the scheduler (blocking process)
CMD ["python", "main.py", "scheduler"]

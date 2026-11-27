FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir \
    aiohttp>=3.9.0 \
    websockets>=12.0 \
    psycopg2-binary>=2.9.0 \
    pydantic>=2.5.0 \
    python-dotenv>=1.0.0 \
    requests>=2.31.0

COPY . .

EXPOSE 8080

CMD ["python", "-u", "main.py"]

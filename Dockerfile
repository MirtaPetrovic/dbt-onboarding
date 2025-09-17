FROM python:3.12-slim

RUN pip install --upgrade pip && pip install dbt-bigquery

WORKDIR /app

COPY . .
COPY profiles.yml /app/profiles.yml

CMD ["dbt", "run", "--profiles-dir", "/app"]

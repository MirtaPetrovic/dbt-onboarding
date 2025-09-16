FROM python:3.12-slim

RUN pip install --upgrade pip && pip install dbt-bigquery

WORKDIR /app

COPY . .

CMD ["dbt", "run", "--profiles-dir", "/app"]

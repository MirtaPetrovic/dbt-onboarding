FROM python:3.12-slim

RUN pip install --upgrade pip && pip install dbt-bigquery

WORKDIR /app

COPY . .
COPY profiles.yml /app/profiles.yml
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
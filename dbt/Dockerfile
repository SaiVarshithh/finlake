FROM python:3.12-slim

ENV DBT_PROFILES_DIR=/opt/finlake/dbt \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /opt/finlake/dbt

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --create-home --uid 10001 dbt

COPY --chown=dbt:dbt . .
RUN chown dbt:dbt /opt/finlake/dbt

USER dbt

ENTRYPOINT ["dbt"]
CMD ["build"]

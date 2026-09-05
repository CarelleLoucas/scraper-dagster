FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster \
    PYTHONPATH=/app/packages \
    SCRAPY_SETTINGS_MODULE=wrc_pipeline.settings

WORKDIR /app
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --create-home app \
    && mkdir -p /opt/dagster /var/lib/dagster \
    && chown -R app:app /opt/dagster /var/lib/dagster

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app packages/ ./packages/
COPY packages/orchestration/dagster.yaml /opt/dagster/

USER app
EXPOSE 3000
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000", "-m", "orchestration.definitions"]

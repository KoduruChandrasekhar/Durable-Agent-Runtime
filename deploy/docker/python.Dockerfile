# Workers (Python SDK + demo agents), mock services and the mock LLM.
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY sdk/python/pyproject.toml sdk/python/pyproject.toml
COPY sdk/python/keel sdk/python/keel
RUN pip install --no-cache-dir "./sdk/python[mcp,otel,metrics]" fastapi "uvicorn[standard]" pyyaml "psycopg[binary]" matplotlib
COPY demos demos
COPY evals evals
COPY chaos chaos
COPY bench bench
COPY sim sim
COPY deploy/policy.yaml deploy/policy.yaml
RUN useradd -u 10001 -m keel && mkdir -p /data/mocks && chown -R keel /data /app
USER keel
ENV KEEL_MOCK_DATA=/data/mocks
CMD ["python", "-m", "demos.worker"]

FROM python:3.12-slim AS base

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

FROM base AS dependencies

COPY pyproject.toml uv.lock ./

COPY packages/data_access/pyproject.toml ./packages/data_access/pyproject.toml
COPY packages/analytics/pyproject.toml ./packages/analytics/pyproject.toml
COPY packages/data_sources/pyproject.toml ./packages/data_sources/pyproject.toml

COPY services/dagster/pyproject.toml ./services/dagster/pyproject.toml
RUN uv sync --frozen --package stock-dagster --no-install-project

FROM dependencies AS runtime

COPY packages/data_access/src ./packages/data_access/src
COPY packages/analytics/src ./packages/analytics/src
COPY packages/data_sources/src ./packages/data_sources/src
COPY services/dagster/src ./services/dagster/src
COPY services/dagster/dagster.yaml ./services/dagster/dagster.yaml

RUN uv sync --frozen --package stock-dagster

ENV PATH="/app/.venv/bin:$PATH"

ENV DAGSTER_HOME=/app/services/dagster

RUN mkdir -p $DAGSTER_HOME

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "stock_dagster"]
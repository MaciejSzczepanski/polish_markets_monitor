FROM python:3.12-slim AS base

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

FROM base AS dependencies

COPY pyproject.toml uv.lock ./
COPY packages/data_access/pyproject.toml ./packages/data_access/pyproject.toml
COPY packages/analytics/pyproject.toml ./packages/analytics/pyproject.toml
COPY services/api/pyproject.toml ./services/api/pyproject.toml

# Install dependencies (without projects themselves)
RUN uv sync --frozen --package api --no-install-project

FROM dependencies AS runtime

COPY packages/data_access/src ./packages/data_access/src
COPY packages/analytics/src ./packages/analytics/src
COPY services/api/src ./services/api/src

RUN uv sync --frozen --package api

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000



CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
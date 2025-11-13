FROM python:3.12-slim AS base

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv/ /uvx/ /bin/

FROM base AS dependencies

COPY pyproject.toml uv.lock ./
COPY packages/data_access/pyproject.toml ./packages/data_access/pyproject.toml
COPY packages/analytics/pyproject.toml ./packages/analytics/pyproject.toml
COPY services/api/pyproject.toml ./services/api/pyproject.toml
COPY services/dashboard/pyproject.toml ./services/dashboard/pyproject.toml

RUN uv sync --frozen --package dashboard --no-install-project

FROM dependencies AS runtime

COPY packages/data_access/src ./packages/data_access/src
COPY packages/analytics/src ./packages/analytics/src
COPY services/api/src ./services/api/src
COPY services/dashboard/src ./services/dashboard/src
COPY services/dashboard/.streamlit ./.streamlit


RUN uv sync --frozen --package dashboard

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8501

HEALTHCHECK CMD curl -fail http://localhost:8501

ENTRYPOINT ["streamlit"]
CMD ["run", "services/dashboard/src/dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
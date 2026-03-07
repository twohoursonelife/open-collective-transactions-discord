FROM python:3.13-alpine

COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen
COPY . .

CMD ["uv", "run", "main.py"]

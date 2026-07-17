FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency files first (better layer caching)
COPY pyproject.toml ./

# Install dependencies into a virtual environment
RUN uv sync --frozen --no-dev

# Copy the application code
COPY . .

EXPOSE 5000

CMD ["uv", "run", "python", "main.py"]
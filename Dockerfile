# Single image for all three services. Each compose service overrides `command`
# to point uvicorn at its own ASGI app, so dependencies are installed once and
# the image layer is shared. Build stage keeps wheels out of the final layer.

# ---- build stage: resolve wheels ----
FROM python:3.12-slim AS build
ENV PIP_NO_CACHE_DIR=1
WORKDIR /wheels
COPY requirements.txt .
RUN pip wheel --wheel-dir /wheels -r requirements.txt

# ---- runtime stage ----
FROM python:3.12-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY --from=build /wheels /wheels
COPY requirements.txt .
RUN pip install --no-index --find-links=/wheels -r requirements.txt && rm -rf /wheels

COPY transitflow ./transitflow

RUN useradd --create-home --uid 10001 appuser
USER appuser

# Overridden per service in docker-compose.yml.
CMD ["uvicorn", "transitflow.trip.app:app", "--host", "0.0.0.0", "--port", "8081"]

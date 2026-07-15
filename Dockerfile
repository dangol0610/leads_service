FROM python:3.13-slim AS builder

WORKDIR /app

COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY . .
RUN pip install --no-cache-dir .


FROM python:3.13-slim

RUN groupadd -r app && useradd -r -g app app

WORKDIR /app

COPY --from=builder /usr/local /usr/local
COPY --from=builder /app /app

USER app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

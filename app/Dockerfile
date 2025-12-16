# app/Dockerfile
FROM python:3.10-slim

# 1. Install System Dependencies (Crucial for ARM64/Ampere)
# These allow pip to compile packages like gevent/pandas if wheels are missing
RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV APP_HOME /usr/src/app

WORKDIR $APP_HOME

# 2. Install Python Deps
# COPY requirements from the context path defined in docker-compose
COPY app/requirements.txt .
# Ensure win32_setctime is REMOVED from this file before building!
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy Application Code
# CHANGED: Copy everything from root context to WORKDIR
# This preserves the structure: /usr/src/app/app/api.py
COPY . .

# Default command (will be overridden by compose)
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    libssl-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV APP_HOME /usr/src/app

WORKDIR $APP_HOME

COPY app/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command (will be overridden by compose)
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
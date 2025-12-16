# Use the same base Python image for consistency
FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV APP_HOME /usr/src/app 

# Set the working directory
WORKDIR $APP_HOME

# Copy only the requirements file first
COPY streamlit_app/requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire Streamlit application code
COPY streamlit_app $APP_HOME/streamlit_app

# Streamlit runs on port 8501
EXPOSE 8501

# The CMD is overridden in docker-compose.yml, but a default is good practice
CMD ["streamlit", "run", "streamlit_app/home.py", "--server.port", "8501", "--server.enableCORS", "true"]

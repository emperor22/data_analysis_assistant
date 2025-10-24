# Use the same base Python image for consistency
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV APP_HOME /app

# Set the working directory
WORKDIR $APP_HOME

# Copy only the requirements file first
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire Streamlit application code
COPY . $APP_HOME

# Streamlit runs on port 8501
EXPOSE 8501

# The CMD is overridden in docker-compose.yml, but a default is good practice
CMD ["streamlit", "run", "home.py", "--server.port", "8501", "--server.enableCORS", "true"]

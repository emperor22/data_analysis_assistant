# Use a lean official Python image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV APP_HOME /app

# Set the working directory
WORKDIR $APP_HOME

# Copy only the requirements file first to leverage Docker caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . $APP_HOME

# Define the default command to run
# This is typically overridden by docker-compose for each service
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
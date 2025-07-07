# Use official Python 3.13 slim image as base
FROM python:3.13-slim AS base

# Set working directory
WORKDIR /app

# Install and upgrade pip
RUN pip install --upgrade pip

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Use a non-root user for better security
RUN useradd --create-home appuser
USER appuser

# Expose application port
EXPOSE 8887

# Healthcheck for container orchestration
HEALTHCHECK --interval=30s --timeout=5s \
  CMD wget --spider http://localhost:8887/health || exit 1

# Start the service with Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8887"]

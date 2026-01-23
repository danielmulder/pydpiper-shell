# Use the official lightweight Python image
FROM python:3.11-slim

# Set environment variables to optimize Python performance
# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files
# PYTHONUNBUFFERED: Ensures logs are sent straight to the terminal
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install essential system tools and clear cache to keep the image slim
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Upgrade core build tools
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy the entire project into the container
# Note: Ensure your .dockerignore is properly configured
COPY . .

# Install the package in editable mode or as a standard install
# This automatically handles dependencies from pyproject.toml
RUN pip install --no-cache-dir .

# Define environment variables for path resolution
ENV PROJECT_ROOT=/app
ENV PYTHONPATH=/app/src

# Expose the PydPiper Studio port
EXPOSE 5000

# Launch the interactive shell or app module
CMD ["python", "-m", "pydpiper_shell.app"]
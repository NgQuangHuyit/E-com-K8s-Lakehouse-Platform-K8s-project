FROM apache/spark-py:v3.4.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /opt/spark/jobs

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy inference script
COPY ml_inference.py .

# Set permissions
RUN chmod +x ml_inference.py

# Switch back to spark user
USER 185

# Entry point
ENTRYPOINT ["python", "/opt/spark/jobs/ml_inference.py"]

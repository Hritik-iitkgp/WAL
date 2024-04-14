# FROM python:3.9-slim
# WORKDIR /app
# COPY . /app
# CMD ["python", "shard_manager.py"]

FROM python:3.9-slim

# Install MySQL client libraries
RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the necessary files to the container
COPY shm.py requirements.txt /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the entrypoint for the container
CMD ["python", "shm.py"]

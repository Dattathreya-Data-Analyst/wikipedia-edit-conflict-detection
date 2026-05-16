#!/bin/bash
echo "Stopping any existing containers..."
docker-compose down 2>/dev/null || docker compose down 2>/dev/null
echo "Starting Docker cluster..."
docker-compose up -d 2>/dev/null || docker compose up -d
echo "Waiting 20 seconds for services to initialize..."
sleep 20
echo "Installing Python dependencies..."
docker exec --user root wiki-spark-master pip3 install -r /app/requirements.txt -q
echo "Running pipeline..."
docker exec --user root -e PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip" -w /app wiki-spark-master python3 /app/main.py
echo "Cleaning up temporary files..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -name "*.crc" -name "_SUCCESS" -name ".DS_Store" -delete 2>/dev/null
echo "Pipeline complete. Results in output/ folder."

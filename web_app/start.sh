#!/bin/bash

echo "Starting Lead Finder Web Application..."
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt --quiet

# Create necessary directories
mkdir -p output
mkdir -p logs

# Start Flask app
echo ""
echo "============================================"
echo "Lead Finder Web App is starting..."
echo "Open your browser and go to:"
echo ""
echo "     http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop the server"
echo "============================================"
echo ""

python app.py

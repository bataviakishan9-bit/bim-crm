#!/bin/bash
# Run this once on the server after uploading files
# From inside: crm.biminfrasolutions.in/httpdocs/

cd $(dirname "$0")

# Create virtual environment
python3 -m venv venv

# Activate and install
source venv/bin/activate
pip install -r requirements.txt

echo "Setup complete! Restart the web app in Plesk panel."

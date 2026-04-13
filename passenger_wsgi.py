import sys
import os

# App directory
app_dir = os.path.dirname(os.path.abspath(__file__))

# Use virtualenv if present (created by setup_server.sh)
venv_path = os.path.join(app_dir, 'venv', 'lib')
if os.path.exists(venv_path):
    import glob
    site_pkgs = glob.glob(os.path.join(venv_path, 'python*', 'site-packages'))
    if site_pkgs:
        sys.path.insert(0, site_pkgs[0])

sys.path.insert(0, app_dir)

# Load .env
from dotenv import load_dotenv
load_dotenv(os.path.join(app_dir, ".env"))

from app import app as application

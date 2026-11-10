"""
PythonAnywhere WSGI configuration for the LTP Flask app.

Upload this file into /home/YOUR_USERNAME/mysite/ as wsgi.py.
The app source file must be in the same folder.
"""

import os
import sys
import importlib.util

PROJECT_DIR = os.path.expanduser("~/mysite")
SOURCE_FILE = os.path.join(
    PROJECT_DIR,
    "9_LTP_SHIFT_ENGINE_SR_FIXED_ANGEL_VOLUME_SYNC.py",
)

if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

if not os.path.isfile(SOURCE_FILE):
    raise FileNotFoundError("LTP source file not found: " + SOURCE_FILE)

spec = importlib.util.spec_from_file_location("ltp_app_module", SOURCE_FILE)
if spec is None or spec.loader is None:
    raise ImportError("Could not load the LTP application module.")

ltp_module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = ltp_module
spec.loader.exec_module(ltp_module)

application = ltp_module.app

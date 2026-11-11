"""Gunicorn entry point for Render. Keep one worker: app state is in-memory."""
from app import app, startup

# The original script starts itself only when run directly. Gunicorn imports it,
# so initialize the Angel One/WebSocket/recorder background services here.
startup()

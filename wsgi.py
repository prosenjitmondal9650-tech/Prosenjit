"""WSGI compatibility entry point; shares the single Render engine instance."""
from render_app import app, application

__all__ = ["app", "application"]

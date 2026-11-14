"""Compatibility entry point.

All deployment entry points must use the same live engine module.  The old
standalone copy of the engine created a second Flask app and independent
market-data state, so WebSocket logs could advance while /api/data served a
separate, frozen state dictionary.
"""
from render_app import app, application

__all__ = ["app", "application"]

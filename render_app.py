"""Render/Gunicorn entry point. Starts the engine once per web process."""
import threading
import app as engine

app = engine.app

if not getattr(engine, "_render_startup_started", False):
    engine._render_startup_started = True
    threading.Thread(
        target=engine.startup,
        name="render-engine-startup",
        daemon=True,
    ).start()

"""Render/Gunicorn entry point; exactly one process owns live market state."""
import importlib.util
import os
import sys
import threading

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(BASE_DIR, "9_LTP_SHIFT_ENGINE_SR_FIXED_ANGEL_VOLUME_SYNC.py")
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
if not os.path.isfile(SOURCE):
    raise FileNotFoundError("LTP source file not found: " + SOURCE)

# Reuse the module if another WSGI entry point has already imported it.
engine = sys.modules.get("ltp_engine")
if engine is None:
    spec = importlib.util.spec_from_file_location("ltp_engine", SOURCE)
    if spec is None or spec.loader is None:
        raise ImportError("Unable to load LTP engine")
    engine = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = engine
    spec.loader.exec_module(engine)

app = engine.app
_start_lock = threading.Lock()
with _start_lock:
    if not getattr(app, "_ltp_engine_started", False):
        engine.startup()
        app._ltp_engine_started = True

application = app

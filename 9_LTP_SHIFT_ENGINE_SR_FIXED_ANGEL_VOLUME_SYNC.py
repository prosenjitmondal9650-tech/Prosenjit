import os
import json
import struct
import ssl
import websocket
import time
import threading
import queue
import requests
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, request, render_template_string, Response, stream_with_context

from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pyotp


# ============================================================
# APP
# ============================================================

app = Flask(__name__)

IST = timezone(timedelta(hours=5, minutes=30))


# ============================================================
# ANGEL ONE ENVIRONMENT VARIABLES
# ============================================================
# IMPORTANT:
# Never hard-code your Angel One credentials here.
# Set these in Render / Android environment variables.
# ============================================================

API_KEY = os.getenv("ANGEL_API_KEY", "").strip()
CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE", "").strip()
MPIN = os.getenv("ANGEL_MPIN", "").strip()
TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "").strip()

PORT = int(os.getenv("PORT", "5000"))

# ============================================================
# SUPABASE CLOUD RECORDER (RENDER-SIDE, NO GITLAB RUNNER)
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_SNAPSHOT_TABLE = os.getenv(
    "SUPABASE_SNAPSHOT_TABLE", "nifty_recorder_snapshots"
).strip()
SUPABASE_SHIFT_TABLE = os.getenv(
    "SUPABASE_SHIFT_TABLE", "nifty_recorder_shift_history"
).strip()
SUPABASE_RUN_TABLE = os.getenv(
    "SUPABASE_RUN_TABLE", "nifty_recorder_runs"
).strip()
CLOUD_RECORDER_ENABLED = os.getenv(
    "CLOUD_RECORDER_ENABLED", "true"
).lower() in ("1", "true", "yes", "on")
CLOUD_RECORDER_INTERVAL = max(
    15, int(os.getenv("CLOUD_RECORDER_INTERVAL", "60"))
)
CLOUD_RECORDER_MANUAL = os.getenv(
    "CLOUD_RECORDER_MANUAL", "true"
).lower() in ("1", "true", "yes", "on")

cloud_recorder_thread = None
cloud_recorder_stop = threading.Event()
cloud_recorder_lock = threading.RLock()
cloud_recorder_running = False
cloud_recorder_run_id = ""
cloud_recorder_seen_events = set()

ENABLE_TRADING = (
    os.getenv("ENABLE_TRADING", "false").lower()
    in ("1", "true", "yes", "on")
)

NIFTY_LOT_SIZE = int(
    os.getenv("NIFTY_LOT_SIZE", "75")
)

MASTER_URL = (
    "https://margincalculator.angelone.in/"
    "OpenAPI_File/files/OpenAPIScripMaster.json"
)

MASTER_CACHE = os.getenv(
    "MASTER_CACHE",
    "OpenAPIScripMaster.json"
)

BASELINE_FILE = os.getenv(
    "BASELINE_FILE",
    "nifty_oi_today_baseline.json"
)

SHIFT_HISTORY_FILE = os.getenv(
    "SHIFT_HISTORY_FILE",
    "nifty_shift_history_today.json"
)

LAST_LTP_CACHE = os.getenv(
    "LAST_LTP_CACHE",
    "last_traded_price_cache.json"
)

NIFTY_TOKEN = "99926000"

NSE_EXCHANGE_TYPE = 1
NFO_EXCHANGE_TYPE = 2

WS_SPOT_MODE = 1  # Angel One LTP mode for NIFTY index
WS_OPTION_MODE = 3  # Angel One SNAP_QUOTE: raw LTP + day volume + OI + OHLC
# Raw option-chain values are NEVER sourced from Greeks REST or local polling.
RAW_OPTION_DATA_SOURCE = "ANGEL_ONE_SMART_STREAM_SNAP_QUOTE"

MASTER_CONNECT_TIMEOUT = 10
MASTER_READ_TIMEOUT = 60
WS_RECONNECT_SECONDS = 5

REST_POLL_SECONDS = 0
OPTION_REST_POLL_SECONDS = 0
GREEKS_REFRESH_SECONDS = 3  # Fast official Angel Greeks snapshot refresh; Greeks are REST snapshots, not WebSocket ticks.

STRIKES_AROUND_ATM = int(
    os.getenv(
        "STRIKES_AROUND_ATM",
        "10"
    )
)


# ============================================================
# GLOBAL STATE
# ============================================================

obj = None
ws = None
active_sws = None
subscribed_option_tokens = set()
subscribed_tokens_lock = threading.Lock()

auth_token = ""
feed_token = ""

ws_thread = None
greeks_thread = None
rest_thread = None

running = True

selected_expiry = None
selected_strike = None

master_options = []
contract_by_symbol = {}
contracts_by_token = {}

live_data = {}
greeks_data = {}

# Reuse the TLS connection for the official Greeks REST endpoint. Creating a
# fresh HTTPS connection on every Greeks refresh adds avoidable handshake delay.
greeks_http = requests.Session()
try:
    greeks_http.headers.update({"Connection": "keep-alive"})
except Exception:
    pass

# Only recalculate/resubscribe the option chain when ATM actually crosses to
# another strike. Doing this on every NIFTY tick wastes callback time.
last_subscription_atm = None

nifty_data = {
    "ltp": 0.0,
    "open": 0.0,
    "high": 0.0,
    "low": 0.0,
    "close": 0.0,
    "change": 0.0,
    "percent_change": 0.0,
    "time": "",
    "ltp_source": "NONE",
    "ltp_updated": "",
}

baseline = {
    "date": "",
    "expiry": "",
    "source": "",
    "values": {}
}

# Morning-to-current CALL/PUT shift history.  One event is stored only when
# the visible state/level actually changes, so repeated 1-second snapshots do
# not create duplicate history rows.
shift_history = {
    "date": "",
    "expiry": "",
    "call": [],
    "put": [],
}

state = {
    "logged_in": False,
    "websocket": "DISCONNECTED",
    "last_error": "",
    "last_update": "",
    "greeks_update": "",
    "server_start": "",
    "last_nifty_tick_at": 0.0,
    "last_option_tick_at": 0.0,
    "tick_count": 0,
    "greeks_count": 0,
    "raw_ws_messages": 0,
    "last_ws_message_at": 0.0,
    "last_ws_token": "",
    "first_ws_tick_at": 0.0,
    "live_tick_active": False,
    "ws_reconnects": 0,
    "last_ws_watchdog": 0.0,
}

data_lock = threading.RLock()
greeks_lock = threading.RLock()
login_lock = threading.RLock()
baseline_lock = threading.RLock()
shift_history_lock = threading.RLock()

# ============================================================
# TRUE LIVE TICK STREAM
# ============================================================
tick_clients = []
tick_clients_lock = threading.RLock()
tick_sequence = 0
# Coalesced derived-state refresh: market callbacks only mutate state/publish deltas.
derived_dirty_at = 0.0
derived_lock = threading.Lock()

def mark_derived_dirty():
    global derived_dirty_at
    with derived_lock:
        derived_dirty_at = time.time()

def publish_tick(delta=None):
    """Publish a compact live delta immediately; never rebuild the full chain here."""
    global tick_sequence
    tick_sequence += 1
    event = delta if isinstance(delta, dict) else {"type": "refresh", "seq": tick_sequence}
    event.setdefault("seq", tick_sequence)
    with tick_clients_lock:
        for q in list(tick_clients):
            try:
                q.put_nowait(event)
            except queue.Full:
                try:
                    q.get_nowait()
                except Exception:
                    pass
                try:
                    q.put_nowait(event)
                except Exception:
                    pass
            except Exception:
                try:
                    tick_clients.remove(q)
                except ValueError:
                    pass


# ============================================================
# SERVER-SENT EVENTS: ANGEL TICK -> BROWSER
# ============================================================
@app.get("/api/tick-stream")
def api_tick_stream():
    """Keep one persistent SSE connection per browser.

    Market ticks are pushed here immediately from publish_tick(); there is no
    browser polling involved. The browser's EventSource reconnects itself if
    the HTTP connection drops.
    """
    q = queue.Queue(maxsize=250)
    with tick_clients_lock:
        tick_clients.append(q)

    def generate():
        try:
            # Send a complete snapshot first so a newly opened page is usable
            # before the next market tick arrives.
            try:
                snapshot = make_payload()
            except Exception:
                snapshot = {}
            yield "event: snapshot\ndata: " + json.dumps({"type":"full_snapshot","payload":snapshot}, separators=(",",":"), default=str) + "\n\n"
            while running:
                try:
                    event = q.get(timeout=15)
                    yield "event: tick\ndata: " + json.dumps(event, separators=(",",":"), default=str) + "\n\n"
                except queue.Empty:
                    # SSE comment keeps proxies/browser connections alive.
                    yield ": heartbeat\n\n"
        except GeneratorExit:
            pass
        except Exception as e:
            print("SSE tick stream error:", repr(e), flush=True)
        finally:
            with tick_clients_lock:
                try:
                    tick_clients.remove(q)
                except ValueError:
                    pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ============================================================
# BASIC HELPERS
# ============================================================

def now_ist():
    return datetime.now(IST)


def now_string():
    return now_ist().strftime("%H:%M:%S")


def today_string():
    return now_ist().strftime("%Y-%m-%d")


def is_market_open():
    """
    NSE market session:
        Monday-Friday
        09:15 IST - 15:30 IST
    """

    now = now_ist()

    if now.weekday() >= 5:
        return False

    current_time = now.time()

    market_start = datetime.strptime(
        "09:15",
        "%H:%M"
    ).time()

    market_end = datetime.strptime(
        "15:30",
        "%H:%M"
    ).time()

    return (
        market_start
        <= current_time
        <= market_end
    )


def safe_float(value, default=0.0):

    try:

        if value is None:
            return default

        if isinstance(value, str):

            value = value.strip()

            if value == "":
                return default

        return float(value)

    except Exception:

        return default


def safe_int(value, default=0):

    try:
        return int(float(value))

    except Exception:

        return default


def websocket_price(value):

    """
    SmartWebSocketV2 prices are normally paise based.

    Example:
        2405500 -> 24055.00
    """

    return safe_float(value) / 100.0


def normalize_strike(value):

    value = safe_float(value)

    if value <= 0:
        return 0.0

    if value > 100000:
        value /= 100.0

    return round(
        value,
        2
    )


def expiry_display(expiry):
    """Return a safe human-readable expiry label for the UI."""
    value = format_expiry(expiry)
    if not value:
        return ""
    try:
        return datetime.strptime(value, "%d%b%Y").strftime("%d %b %Y").upper()
    except Exception:
        return value


def format_expiry(expiry):

    if not expiry:
        return ""

    text = str(expiry).upper().strip()

    # Angel One normally uses DDMMMYYYY, but accept ISO/date-like
    # values too so the Greeks response can be matched safely.
    for fmt_in in (
        "%d%b%Y",
        "%Y-%m-%d",
        "%d-%b-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
    ):
        try:
            dt = datetime.strptime(text, fmt_in)
            return dt.strftime("%d%b%Y").upper()
        except Exception:
            pass

    return text.replace(" ", "")


# ============================================================
# CREDENTIAL CHECK
# ============================================================

def validate_credentials():

    missing = []

    if not API_KEY:
        missing.append(
            "ANGEL_API_KEY"
        )

    if not CLIENT_CODE:
        missing.append(
            "ANGEL_CLIENT_CODE"
        )

    if not MPIN:
        missing.append(
            "ANGEL_MPIN"
        )

    if not TOTP_SECRET:
        missing.append(
            "ANGEL_TOTP_SECRET"
        )

    if missing:

        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
        )


# ============================================================
# ANGEL ONE LOGIN
# ============================================================

def angel_login():
    """Create a fresh Angel One session and atomically replace auth/feed tokens.

    Long-running Render processes can keep an expired JWT.  All callers share
    one lock so a WebSocket reconnect and a Greeks refresh cannot race and
    overwrite each other with different sessions.
    """
    global obj, auth_token, feed_token

    with login_lock:
        try:
            validate_credentials()

            print("ANGEL ONE LOGIN: creating fresh session", flush=True)

            new_obj = SmartConnect(api_key=API_KEY)
            totp = pyotp.TOTP(TOTP_SECRET).now()
            response = new_obj.generateSession(CLIENT_CODE, MPIN, totp)

            if not response or not response.get("status"):
                raise RuntimeError("Angel One login failed: " + str(response))

            data = response.get("data") or {}
            new_auth = data.get("jwtToken") or data.get("jwt") or ""
            if not new_auth:
                raise RuntimeError("Login successful but JWT token missing")

            new_feed = new_obj.getfeedToken() or ""
            if not new_feed:
                raise RuntimeError("Login successful but feed token missing")

            # Replace the session only after BOTH tokens are valid.
            obj = new_obj
            auth_token = str(new_auth)
            feed_token = str(new_feed)
            state["logged_in"] = True
            state["last_error"] = ""

            print("Angel One login successful; fresh JWT/feed token installed.", flush=True)
            return True

        except Exception as e:
            state["logged_in"] = False
            state["last_error"] = "Angel login: " + str(e)
            print("Angel login error:", repr(e), flush=True)
            return False


def relogin():

    global ws

    print(
        "Attempting Angel One re-login..."
    )

    try:

        if ws:

            try:
                ws.close_connection()

            except Exception:
                pass

    except Exception:
        pass

    time.sleep(2)

    return angel_login()


# ============================================================
# MASTER FILE
# ============================================================

def download_master():

    temp_file = (
        MASTER_CACHE
        + ".tmp"
    )

    print(
        "Downloading Angel One instrument master..."
    )

    response = requests.get(
        MASTER_URL,
        timeout=(
            MASTER_CONNECT_TIMEOUT,
            MASTER_READ_TIMEOUT
        ),
        stream=True
    )

    response.raise_for_status()

    with open(
        temp_file,
        "wb"
    ) as f:

        for chunk in response.iter_content(
            chunk_size=1024 * 1024
        ):

            if chunk:
                f.write(chunk)

    os.replace(
        temp_file,
        MASTER_CACHE
    )

    print(
        "Master downloaded."
    )


def load_master_file():

    if not os.path.exists(
        MASTER_CACHE
    ):

        download_master()

    try:

        with open(
            MASTER_CACHE,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)

    except Exception:

        print(
            "Master cache invalid. Re-downloading..."
        )

        download_master()

        with open(
            MASTER_CACHE,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)


def load_nifty_options():

    global master_options
    global contract_by_symbol
    global contracts_by_token

    raw = load_master_file()

    options = []

    for item in raw:

        if str(
            item.get(
                "exch_seg",
                ""
            )
        ).upper() != "NFO":

            continue

        name = str(
            item.get(
                "name",
                ""
            )
        ).upper()

        if name != "NIFTY":
            continue

        instrument_type = str(
            item.get(
                "instrumenttype",
                ""
            )
        ).upper()

        if instrument_type != "OPTIDX":
            continue

        symbol = str(
            item.get(
                "symbol",
                ""
            )
        ).upper().strip()

        if not symbol.endswith(
            ("CE", "PE")
        ):

            continue

        expiry = format_expiry(
            item.get("expiry")
        )

        if not expiry:
            continue

        token = str(
            item.get(
                "token",
                ""
            )
        ).strip()

        if not token:
            continue

        strike = normalize_strike(
            item.get("strike")
        )

        if strike <= 0:
            continue

        option_type = (
            "CE"
            if symbol.endswith("CE")
            else "PE"
        )

        options.append(
            {
                "symbol": symbol,
                "token": token,
                "expiry": expiry,
                "strike": strike,
                "option_type": option_type,
                "name": "NIFTY",
                "exchange": "NFO",
            }
        )

    master_options = options

    contract_by_symbol = {
        x["symbol"]: x
        for x in options
    }

    contracts_by_token = {
        x["token"]: x
        for x in options
    }

    print(
        "NIFTY option contracts loaded:",
        len(master_options)
    )

    return options


# ============================================================
# EXPIRY
# ============================================================

def get_expiries():

    values = set()

    for item in master_options:

        expiry = item.get(
            "expiry"
        )

        if not expiry:
            continue

        try:

            dt = datetime.strptime(
                expiry,
                "%d%b%Y"
            )

            if (
                dt.date()
                >= now_ist().date()
            ):

                values.add(
                    expiry
                )

        except Exception:
            pass

    return sorted(
        values,
        key=lambda x:
            datetime.strptime(
                x,
                "%d%b%Y"
            )
    )


def choose_expiry(
    expiry=None
):

    global selected_expiry

    expiries = get_expiries()

    if not expiries:

        raise RuntimeError(
            "No current NIFTY option expiry found."
        )

    if expiry:

        expiry = format_expiry(
            expiry
        )

        if expiry in expiries:

            selected_expiry = expiry

            return expiry

    selected_expiry = expiries[0]

    return selected_expiry


# ============================================================
# CONTRACT SELECTION
# ============================================================

def get_expiry_contracts():

    return [
        x
        for x in master_options
        if x["expiry"] == selected_expiry
    ]


def get_all_strikes():

    return sorted(
        set(
            x["strike"]
            for x in get_expiry_contracts()
        )
    )


def calculate_atm():

    spot = safe_float(
        nifty_data.get("ltp")
    )

    strikes = get_all_strikes()

    if not strikes:
        return 0.0

    if spot <= 0:
        return strikes[0]

    return min(
        strikes,
        key=lambda x:
            abs(x - spot)
    )


def get_visible_strikes():

    strikes = get_all_strikes()

    if not strikes:
        return []

    atm = calculate_atm()

    index = min(
        range(len(strikes)),
        key=lambda i:
            abs(
                strikes[i] - atm
            )
    )

    start = max(
        0,
        index - STRIKES_AROUND_ATM
    )

    end = min(
        len(strikes),
        index
        + STRIKES_AROUND_ATM
        + 1
    )

    return strikes[
        start:end
    ]


def get_subscription_tokens():

    # Keep the complete currently displayed CE/PE chain live.
    # 10 strikes around ATM = up to 21 strikes / 42 option tokens,
    # which stays within the normal token-list size used by this app.
    visible = set()

    strikes = set(get_visible_strikes())

    for item in get_expiry_contracts():
        if item["strike"] in strikes:
            visible.add(str(item["token"]))

    return sorted(visible, key=lambda x: int(x) if str(x).isdigit() else str(x))


# ============================================================
# LIVE DATA STORAGE
# ============================================================

def create_empty_data():

    return {
        "ltp": 0.0,
        "ltp_source": "NONE",
        "ltp_updated": "",
        "volume": 0,
        "oi": 0,
        "oi_change": 0,
        "net_change": 0.0,
        "percent_change": 0.0,
        "close": 0.0,
        "trade_qty": 0,
        "avg_price": 0.0,
        "buy_qty": 0,
        "sell_qty": 0,
        "open": 0.0,
        "high": 0.0,
        "low": 0.0,
        "time": "",
        "received": 0,
        # Exact Angel Smart Stream timing for raw-value synchronization.
        "exchange_timestamp": 0,
        "last_tick_received_ms": 0,
    }


def ensure_live_symbol(
    symbol
):

    if symbol not in live_data:

        live_data[symbol] = (
            create_empty_data()
        )

    return live_data[symbol]


# ============================================================
# LAST TRADED PRICE CACHE
# ============================================================

def save_ltp_cache():

    temp = (
        LAST_LTP_CACHE
        + ".tmp"
    )

    try:

        with data_lock:

            payload = {
                "updated_at":
                    now_ist().isoformat(),

                "nifty": {
                    "ltp":
                        safe_float(
                            nifty_data.get("ltp")
                        ),

                    "close":
                        safe_float(
                            nifty_data.get("close")
                        ),

                    "time":
                        nifty_data.get(
                            "time",
                            ""
                        ),
                },

                "options": {
                    symbol: {
                        "ltp":
                            safe_float(
                                item.get("ltp")
                            ),

                        "close":
                            safe_float(
                                item.get("close")
                            ),

                        "volume":
                            safe_int(
                                item.get("volume")
                            ),

                        "oi":
                            safe_int(
                                item.get("oi")
                            ),

                        "oi_change":
                            safe_int(
                                item.get("oi_change")
                            ),

                        "net_change":
                            safe_float(
                                item.get(
                                    "net_change"
                                )
                            ),

                        "percent_change":
                            safe_float(
                                item.get(
                                    "percent_change"
                                )
                            ),

                        "avg_price":
                            safe_float(
                                item.get(
                                    "avg_price"
                                )
                            ),

                        "open":
                            safe_float(
                                item.get("open")
                            ),

                        "high":
                            safe_float(
                                item.get("high")
                            ),

                        "low":
                            safe_float(
                                item.get("low")
                            ),

                        "time":
                            item.get(
                                "time",
                                ""
                            ),
                    }

                    for symbol, item
                    in live_data.items()

                    if (
                        safe_float(
                            item.get("ltp")
                        ) > 0
                        or
                        safe_float(
                            item.get("close")
                        ) > 0
                        or
                        safe_int(
                            item.get("volume")
                        ) > 0
                        or
                        safe_int(
                            item.get("oi")
                        ) > 0
                    )
                }
            }

        with open(
            temp,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                payload,
                f,
                indent=2
            )

        os.replace(
            temp,
            LAST_LTP_CACHE
        )

    except Exception as e:

        print(
            "LTP cache save error:",
            e
        )

        try:

            if os.path.exists(temp):
                os.remove(temp)

        except Exception:
            pass


def load_ltp_cache():

    if not os.path.exists(
        LAST_LTP_CACHE
    ):

        return

    try:

        with open(
            LAST_LTP_CACHE,
            "r",
            encoding="utf-8"
        ) as f:

            payload = json.load(f) or {}

        nifty = (
            payload.get("nifty")
            or {}
        )

        cached_nifty_ltp = safe_float(
            nifty.get("ltp")
        )

        cached_nifty_close = safe_float(
            nifty.get("close")
        )

        with data_lock:

            if (
                safe_float(
                    nifty_data.get("ltp")
                ) <= 0
                and cached_nifty_ltp > 0
            ):

                nifty_data["ltp"] = (
                    cached_nifty_ltp
                )

                nifty_data[
                    "ltp_source"
                ] = "LAST TRADED"

                nifty_data[
                    "ltp_updated"
                ] = nifty.get(
                    "time",
                    ""
                )

            if (
                safe_float(
                    nifty_data.get("close")
                ) <= 0
                and cached_nifty_close > 0
            ):

                nifty_data["close"] = (
                    cached_nifty_close
                )

            if (
                safe_float(
                    nifty_data.get("ltp")
                ) <= 0
                and cached_nifty_ltp <= 0
                and cached_nifty_close > 0
            ):

                nifty_data["ltp"] = (
                    cached_nifty_close
                )

                nifty_data[
                    "ltp_source"
                ] = "CLOSE CACHE"

                nifty_data[
                    "ltp_updated"
                ] = nifty.get(
                    "time",
                    ""
                )

            options = (
                payload.get("options")
                or {}
            )

            for symbol, cached in options.items():

                if symbol not in contract_by_symbol:
                    continue

                item = ensure_live_symbol(
                    symbol
                )

                cached_ltp = safe_float(
                    cached.get("ltp")
                )

                cached_close = safe_float(
                    cached.get("close")
                )

                if (
                    safe_float(
                        item.get("ltp")
                    ) <= 0
                    and cached_ltp > 0
                ):

                    item["ltp"] = (
                        cached_ltp
                    )

                    item[
                        "ltp_source"
                    ] = "LAST TRADED"

                    item[
                        "ltp_updated"
                    ] = cached.get(
                        "time",
                        ""
                    )

                    item["time"] = (
                        cached.get(
                            "time",
                            ""
                        )
                    )

                if (
                    safe_float(
                        item.get("close")
                    ) <= 0
                    and cached_close > 0
                ):

                    item["close"] = (
                        cached_close
                    )

                for key in (
                    "volume",
                    "oi",
                    "oi_change",
                    "net_change",
                    "percent_change",
                    "avg_price",
                    "open",
                    "high",
                    "low"
                ):

                    if key not in cached:
                        continue

                    if key in (
                        "volume",
                        "oi",
                        "oi_change"
                    ):

                        value = safe_int(
                            cached.get(key)
                        )

                    else:

                        value = safe_float(
                            cached.get(key)
                        )

                    item[key] = value

                if (
                    safe_float(
                        item.get("ltp")
                    ) <= 0
                    and cached_ltp <= 0
                    and cached_close > 0
                ):

                    item["ltp"] = (
                        cached_close
                    )

                    item[
                        "ltp_source"
                    ] = "CLOSE CACHE"

                    item[
                        "ltp_updated"
                    ] = cached.get(
                        "time",
                        ""
                    )

                    item["time"] = (
                        cached.get(
                            "time",
                            ""
                        )
                    )

        print(
            "Last traded price cache restored."
        )

    except Exception as e:

        print(
            "LTP cache load error:",
            e
        )


def set_nifty_ltp(
    ltp,
    source="LIVE"
):

    ltp = safe_float(
        ltp
    )

    if ltp <= 0:
        return False

    with data_lock:

        nifty_data["ltp"] = ltp
        nifty_data[
            "ltp_source"
        ] = source

        nifty_data[
            "ltp_updated"
        ] = now_string()

        nifty_data[
            "time"
        ] = now_string()

    return True


def set_option_ltp(
    item,
    ltp,
    source="LIVE"
):

    ltp = safe_float(
        ltp
    )

    if ltp <= 0:
        return False

    item["ltp"] = ltp
    item[
        "ltp_source"
    ] = source

    item[
        "ltp_updated"
    ] = now_string()

    return True


# ============================================================
# OI BASELINE
# ============================================================

def load_baseline():

    global baseline

    with baseline_lock:

        baseline = {
            "date": today_string(),
            "expiry": selected_expiry,
            "values": {}
        }

        if not os.path.exists(
            BASELINE_FILE
        ):
            return

        try:

            with open(
                BASELINE_FILE,
                "r",
                encoding="utf-8"
            ) as f:

                data = json.load(f)

            if (
                data.get("date")
                == today_string()
                and
                data.get("expiry")
                == selected_expiry
            ):

                baseline = data
                print("OI baseline loaded:", len(baseline.get("values", {})), "symbols", "date=", baseline.get("date"), "expiry=", baseline.get("expiry"), flush=True)

        except Exception as e:

            print(
                "Baseline load error:",
                e
            )


def save_baseline():

    temp = (
        BASELINE_FILE
        + ".tmp"
    )

    try:

        with baseline_lock:

            with open(
                temp,
                "w",
                encoding="utf-8"
            ) as f:

                json.dump(
                    baseline,
                    f,
                    indent=2
                )

            os.replace(
                temp,
                BASELINE_FILE
            )

    except Exception as e:

        print(
            "Baseline save error:",
            e
        )


def update_oi_baseline(
    symbol,
    oi
):

    oi = safe_int(
        oi
    )

    if oi <= 0:
        return 0

    changed = False

    with baseline_lock:

        values = baseline.setdefault(
            "values",
            {}
        )

        if symbol not in values:

            values[symbol] = oi
            changed = True
            print("OI baseline initialized:", symbol, "OI=", oi, flush=True)

        first_oi = safe_int(
            values.get(symbol)
        )

    if changed:
        save_baseline()

    return oi - first_oi


# ============================================================
# WEBSOCKET TICK
# ============================================================

def process_tick(message):

    global ws

    if not isinstance(
        message,
        dict
    ):
        return

    # SmartWebSocketV2 can deliver a wrapped data object and token bytes.
    if isinstance(message.get("data"), dict) and not message.get("token"):
        message = message["data"]

    token_value = message.get("token", "")
    if isinstance(token_value, bytes):
        token = token_value.decode("utf-8", errors="replace").strip("\x00 ")
    else:
        token = str(token_value).strip()

    if not token:
        return

    if not state.get("first_ws_tick_at"):
        state["first_ws_tick_at"] = time.time()
    state["last_ws_token"] = token

    # --------------------------------------------------------
    # NIFTY INDEX
    # --------------------------------------------------------

    if token == NIFTY_TOKEN:

        raw_ltp = message.get(
            "last_traded_price"
        )

        ltp = websocket_price(
            raw_ltp
        )

        with data_lock:

            # THIS IS AN ANGEL ONE WEBSOCKET TICK.
            # Every valid tick must update the NIFTY LTP immediately.
            if ltp > 0:
                set_nifty_ltp(
                    ltp,
                    "LIVE"
                )

            state[
                "last_update"
            ] = now_string()

        # Snapshot the values from this exact tick before releasing the lock.
        # Read the same Angel SNAP_QUOTE fields from the exact NIFTY tick.
        # These are used by the current 5-minute candle without any polling.
        nifty_volume_raw = message.get("volume_trade_for_the_day")
        if nifty_volume_raw is None:
            nifty_volume_raw = message.get("volume")
        nifty_volume = safe_int(nifty_volume_raw) if nifty_volume_raw is not None else 0

        with data_lock:
            nifty_ltp = safe_float(nifty_data.get("ltp"))
            nifty_source = nifty_data.get("ltp_source", "LIVE")
            nifty_time = nifty_data.get("time", "")

        state["last_nifty_tick_at"] = time.time()
        state["tick_count"] = safe_int(state.get("tick_count", 0)) + 1
        state["live_tick_active"] = True

        # Push this exact NIFTY tick immediately to connected browsers.
        # Push this exact Angel One Smart Stream tick immediately.
        # exchange_timestamp is used by the browser to place the tick into
        # the exact 5-minute candle bucket.
        try:
            tick_ms = safe_int(message.get("exchange_timestamp"))
            # Angel Smart Stream can expose exchange time in seconds or
            # milliseconds depending on the message/version. Normalize it
            # before the browser builds the 5-minute candle bucket.
            if 1_000_000_000 <= tick_ms < 10_000_000_000:
                tick_ms *= 1000
            elif tick_ms < 1_000_000_000 or tick_ms > 9_999_999_999_999:
                tick_ms = 0
        except Exception:
            tick_ms = 0
        publish_tick({
            "type": "nifty",
            "ltp": nifty_ltp,
            "ltp_source": nifty_source,
            "time": nifty_time,
            "tick_ms": tick_ms if tick_ms > 0 else int(time.time() * 1000),
            "volume": nifty_volume,
        })

        # Do NOT rebuild the entire payload on every NIFTY tick. That was the
        # main server-side latency source: make_payload() walks/ranks the full
        # option chain and was executed inside the Angel callback.
        # Only re-chain when ATM actually changes strike.
        global last_subscription_atm
        try:
            current_atm = calculate_atm()
            if current_atm > 0 and current_atm != last_subscription_atm:
                if active_sws is not None:
                    before = set(subscribed_option_tokens)
                    subscribe_live_tokens(active_sws)
                    after = set(subscribed_option_tokens)
                    last_subscription_atm = current_atm
                    if before != after:
                        ensure_visible_chain_symbols()
                        publish_tick({"type": "rechain", "payload": make_payload()})
        except Exception as e:
            state["last_error"] = "Subscription refresh: " + str(e)
        return

    # --------------------------------------------------------
    # OPTION
    # --------------------------------------------------------

    contract = contracts_by_token.get(
        token
    )

    if not contract:
        return

    symbol = contract[
        "symbol"
    ]

    item = ensure_live_symbol(
        symbol
    )

    # Angel One SNAP_QUOTE values are integer paise for prices.
    # Keep every quote field directly from the live WebSocket tick.
    ltp = websocket_price(message.get("last_traded_price"))

    volume_raw = message.get("volume_trade_for_the_day")
    if volume_raw is None:
        volume_raw = message.get("volume")
    volume_present = volume_raw is not None
    volume = safe_int(volume_raw) if volume_present else None

    oi_raw = message.get("open_interest")
    if oi_raw is None:
        oi_raw = message.get("oi")
    oi_present = oi_raw is not None
    oi = safe_int(oi_raw) if oi_present else None

    close = websocket_price(message.get("closed_price") or message.get("close"))
    open_price = websocket_price(message.get("open_price_of_the_day") or message.get("open"))
    high_price = websocket_price(message.get("high_price_of_the_day") or message.get("high"))
    low_price = websocket_price(message.get("low_price_of_the_day") or message.get("low"))
    avg_price = websocket_price(message.get("average_traded_price") or message.get("avg_price"))
    buy_qty = safe_int(message.get("total_buy_quantity") if message.get("total_buy_quantity") is not None else message.get("buy_qty"))
    sell_qty = safe_int(message.get("total_sell_quantity") if message.get("total_sell_quantity") is not None else message.get("sell_qty"))
    trade_qty = safe_int(message.get("last_traded_quantity") if message.get("last_traded_quantity") is not None else message.get("trade_qty"))

    # IMPORTANT: keep the exchange timestamp from the SAME Angel packet as
    # LTP/Volume/OI. This lets the browser compare raw values at the actual
    # market-tick time instead of comparing an old value with a newer screen.
    exchange_ts_raw = message.get("last_traded_timestamp")
    if exchange_ts_raw is None:
        exchange_ts_raw = message.get("exchange_timestamp")
    exchange_ts = safe_int(exchange_ts_raw)
    if 1_000_000_000 <= exchange_ts < 10_000_000_000:
        exchange_ts *= 1000
    elif exchange_ts < 1_000_000_000 or exchange_ts > 9_999_999_999_999:
        exchange_ts = 0
    received_ms = int(time.time() * 1000)

    # SNAP_QUOTE does not reliably expose a separate net_change field.
    # Angel One's close + live LTP gives the exact current change.
    supplied_net_change = message.get("net_change")
    if supplied_net_change is not None:
        net_change = websocket_price(supplied_net_change)
    elif close > 0 and ltp > 0:
        net_change = ltp - close
    else:
        net_change = 0.0

    with data_lock:

        if ltp > 0:

            set_option_ltp(
                item,
                ltp,
                "LIVE"
            )

        if volume_present and volume is not None and volume >= 0:
            item["volume"] = volume

        if oi_present and oi is not None and oi >= 0:
            item["oi"] = oi

            item[
                "oi_change"
            ] = update_oi_baseline(
                symbol,
                oi
            )

        if close > 0:
            item["close"] = close
        if open_price > 0:
            item["open"] = open_price
        if high_price > 0:
            item["high"] = high_price
        if low_price > 0:
            item["low"] = low_price
        if avg_price > 0:
            item["avg_price"] = avg_price
        item["buy_qty"] = buy_qty
        item["sell_qty"] = sell_qty
        item["trade_qty"] = trade_qty

        item["net_change"] = net_change

        if (
            close > 0
            and ltp > 0
        ):

            item[
                "percent_change"
            ] = (
                (ltp - close)
                / close
                * 100
            )

        # Keep raw Angel tick timing separate from local/server display time.
        item["time"] = now_string()
        item["received"] = received_ms / 1000.0
        item["exchange_timestamp"] = exchange_ts
        item["last_tick_received_ms"] = received_ms

        state["last_option_tick_at"] = received_ms / 1000.0
        state["tick_count"] = safe_int(state.get("tick_count", 0)) + 1

        # Publish the changed option immediately. Browser updates these live cells.
        # Derived/ranking state is refreshed by a separate coalescing thread so
        # this Angel callback never waits for a full-chain make_payload().
        mark_derived_dirty()
        delta = {
            "type": "option",
            "token": token,
            "symbol": symbol,
            "ltp": safe_float(item.get("ltp")),
            "close": safe_float(item.get("close")),
            "percent_change": safe_float(item.get("percent_change")),
            "net_change": safe_float(item.get("net_change")),
            "avg_price": safe_float(item.get("avg_price")),
            "open": safe_float(item.get("open")),
            "high": safe_float(item.get("high")),
            "low": safe_float(item.get("low")),
            "buy_qty": safe_int(item.get("buy_qty")),
            "sell_qty": safe_int(item.get("sell_qty")),
            "trade_qty": safe_int(item.get("trade_qty")),
            "time": item.get("time", ""),
            # Raw synchronization diagnostics: these fields belong to the
            # exact same Angel packet as LTP/Volume/OI.
            "exchange_timestamp": safe_int(item.get("exchange_timestamp")),
            "received_ms": safe_int(item.get("last_tick_received_ms")),
        }
        if volume_present:
            delta["volume"] = safe_int(item.get("volume"))
        if oi_present:
            delta["oi"] = safe_int(item.get("oi"))
            delta["oi_change"] = safe_int(item.get("oi_change"))
        # The option delta is sent immediately. Do not call make_payload() here:
        # that full-chain calculation can delay the next Angel callback.
        publish_tick(delta)

        state[
            "last_update"
        ] = now_string()



# ============================================================
# COALESCED DERIVED LIVE REFRESH
# ============================================================
def derived_live_refresh_loop():
    """Refresh derived/ranking/header state off the Angel WebSocket callback.

    Raw LTP/volume/OI ticks are published immediately. This loop only rebuilds
    the full derived payload at most ~4 times/sec, preventing a busy option
    chain from blocking the next Angel tick while still keeping calculations
    effectively live on screen.
    """
    last_sent = 0.0
    while running:
        try:
            time.sleep(0.25)
            with derived_lock:
                dirty = derived_dirty_at
            now = time.time()
            if dirty <= 0 or dirty <= last_sent:
                continue
            # Coalesce bursts of option ticks into one current snapshot.
            payload = make_payload()
            publish_tick({"type": "derived", "payload": payload})
            last_sent = now
        except Exception as e:
            state["last_error"] = "Derived live refresh: " + str(e)


# ============================================================
# FULL LIVE SNAPSHOT BROADCASTER
# ============================================================
def live_snapshot_loop():
    """Deprecated: no periodic market-data snapshots.

    All live browser updates are emitted only from Angel Smart Stream ticks.
    """
    return


# ============================================================
# BACKGROUND CACHE WRITER
# ============================================================
def cache_writer_loop():
    # Disk cache persistence only; this is NOT a market-data refresh.
    while running:
        try:
            time.sleep(5)
            save_ltp_cache()
        except Exception as e:
            print("Cache writer error:", e)


# ============================================================
# REST NIFTY FALLBACK
# ============================================================

def fetch_nifty_rest():

    global obj

    if (
        not obj
        or not state.get("logged_in")
    ):
        return False

    try:

        response = obj.ltpData(
            "NSE",
            "NIFTY",
            NIFTY_TOKEN
        )

        if not response:

            state[
                "last_error"
            ] = (
                "NIFTY REST: empty response"
            )

            return False

        if not response.get(
            "status"
        ):

            state[
                "last_error"
            ] = (
                "NIFTY REST: "
                + str(
                    response.get(
                        "message"
                    )
                    or response
                )
            )

            print(
                "NIFTY REST rejected:",
                response
            )

            return False

        data = (
            response.get("data")
            or {}
        )

        ltp = safe_float(
            data.get("ltp")
        )

        with data_lock:

            last_tick = safe_float(state.get("last_nifty_tick_at", 0.0))
            ws_fresh = last_tick > 0 and (time.time() - last_tick) <= 20
            if ltp > 0 and not ws_fresh:
                set_nifty_ltp(
                    ltp,
                    "REST-FALLBACK"
                )

            for key in (
                "open",
                "high",
                "low",
                "close"
            ):

                value = safe_float(
                    data.get(key)
                )

                if value > 0:
                    nifty_data[key] = value

            if (
                ltp > 0
                and
                safe_float(
                    nifty_data.get("close")
                ) > 0
            ):

                close = safe_float(
                    nifty_data.get("close")
                )

                nifty_data[
                    "change"
                ] = (
                    ltp - close
                )

                nifty_data[
                    "percent_change"
                ] = (
                    (ltp - close)
                    / close
                    * 100
                )

            nifty_data[
                "time"
            ] = now_string()

            state[
                "last_update"
            ] = now_string()

        if ltp > 0:

            state[
                "last_error"
            ] = ""

            save_ltp_cache()

            return True

        close_value = safe_float(
            data.get("close")
        )

        if close_value > 0:

            with data_lock:

                if (
                    safe_float(
                        nifty_data.get("ltp")
                    ) <= 0
                ):

                    nifty_data[
                        "ltp"
                    ] = close_value

                    nifty_data[
                        "ltp_source"
                    ] = "CLOSE"

                    nifty_data[
                        "ltp_updated"
                    ] = now_string()

                nifty_data[
                    "close"
                ] = close_value

                nifty_data[
                    "time"
                ] = now_string()

            save_ltp_cache()

            state[
                "last_error"
            ] = (
                "MARKET CLOSED • "
                "showing last closing price"
            )

            return True

        return (
            safe_float(
                nifty_data.get("ltp")
            ) > 0
        )

    except Exception as e:

        state[
            "last_error"
        ] = (
            "NIFTY REST error: "
            + str(e)
        )

        print(
            "NIFTY REST error:",
            e
        )

        return False


# ============================================================
# OPTION REST HELPERS
# ============================================================

def market_row_value(
    row,
    *names,
    default=None
):

    if not isinstance(
        row,
        dict
    ):
        return default

    for name in names:

        if (
            name in row
            and
            row.get(name) is not None
        ):

            return row.get(name)

    lowered = {
        str(k).strip().lower(): v
        for k, v in row.items()
    }

    for name in names:

        key = str(
            name
        ).strip().lower()

        if (
            key in lowered
            and
            lowered[key] is not None
        ):

            return lowered[key]

    return default


# ============================================================
# OPTION REST SEED
# ============================================================

def seed_options_rest():

    if not obj:
        return

    tokens = get_subscription_tokens()

    if not tokens:
        return

    print(
        "Seeding option data:",
        len(tokens)
    )

    batch_size = 45

    for start in range(
        0,
        len(tokens),
        batch_size
    ):

        batch = tokens[
            start:start + batch_size
        ]

        try:

            response = None

            try:

                response = obj.getMarketData(
                    "FULL",
                    {
                        "NFO": batch
                    }
                )

            except Exception as first_error:

                try:

                    response = obj.marketData(
                        {
                            "mode": "FULL",
                            "exchangeTokens": {
                                "NFO": batch
                            }
                        }
                    )

                except Exception:

                    print(
                        "Option market-data API error:",
                        first_error
                    )

                    continue

            if not response:
                continue

            if not response.get(
                "status"
            ):

                print(
                    "Option REST:",
                    response
                )

                continue

            data = (
                response.get("data")
                or {}
            )

            if isinstance(
                data,
                dict
            ):

                fetched = (
                    data.get("fetched")
                    or data.get("data")
                    or []
                )

            elif isinstance(
                data,
                list
            ):

                fetched = data

            else:

                fetched = []

            for row in fetched:

                token = str(
                    market_row_value(
                        row,
                        "token",
                        "symbolToken",
                        "symboltoken",
                        default=""
                    )
                ).strip()

                contract = (
                    contracts_by_token.get(
                        token
                    )
                )

                if not contract:
                    continue

                symbol = contract[
                    "symbol"
                ]

                item = ensure_live_symbol(
                    symbol
                )

                ltp = safe_float(
                    market_row_value(
                        row,
                        "ltp",
                        "lastTradedPrice",
                        default=0
                    )
                )

                oi = safe_int(
                    market_row_value(
                        row,
                        "opnInterest",
                        "openInterest",
                        "open_interest",
                        "oi",
                        default=0
                    )
                )

                volume = safe_int(
                    market_row_value(
                        row,
                        "tradeVolume",
                        "trade_volume",
                        "volume",
                        default=0
                    )
                )

                close = safe_float(
                    market_row_value(
                        row,
                        "close",
                        "closedPrice",
                        "closed_price",
                        default=0
                    )
                )

                with data_lock:

                    # REST is only a seed/recovery source. If this option
                    # received a WebSocket tick recently, NEVER overwrite its
                    # live LTP with the slower REST quote.
                    last_live = safe_float(item.get("received", 0.0))
                    live_is_fresh = last_live > 0 and (time.time() - last_live) <= 30 and item.get("ltp_source") == "LIVE"

                    if ltp > 0 and not live_is_fresh:

                        set_option_ltp(
                            item,
                            ltp,
                            "REST-FALLBACK"
                        )

                    # REST is only a one-time startup seed. A concurrent
                    # Smart Stream packet is newer and authoritative for raw
                    # option Volume/OI; never let a slower REST response replace it.
                    if not live_is_fresh:
                        item["oi"] = oi
                        if oi > 0:
                            item["oi_change"] = update_oi_baseline(symbol, oi)
                        item["volume"] = volume

                    item[
                        "net_change"
                    ] = safe_float(
                        market_row_value(
                            row,
                            "netChange",
                            "net_change",
                            default=0
                        )
                    )

                    item[
                        "avg_price"
                    ] = safe_float(
                        market_row_value(
                            row,
                            "avgPrice",
                            "averagePrice",
                            "avg_price",
                            default=0
                        )
                    )

                    item[
                        "open"
                    ] = safe_float(
                        market_row_value(
                            row,
                            "open",
                            default=0
                        )
                    )

                    item[
                        "high"
                    ] = safe_float(
                        market_row_value(
                            row,
                            "high",
                            default=0
                        )
                    )

                    item[
                        "low"
                    ] = safe_float(
                        market_row_value(
                            row,
                            "low",
                            default=0
                        )
                    )

                    if close > 0:
                        item[
                            "close"
                        ] = close

                    api_pct = safe_float(
                        market_row_value(
                            row,
                            "percentChange",
                            "percent_change",
                            default=0
                        )
                    )

                    if api_pct != 0:

                        item[
                            "percent_change"
                        ] = api_pct

                    elif (
                        close > 0
                        and
                        ltp > 0
                    ):

                        item[
                            "percent_change"
                        ] = (
                            (ltp - close)
                            / close
                            * 100
                        )

                    item[
                        "time"
                    ] = (
                        market_row_value(
                            row,
                            "exchTradeTime",
                            "exchFeedTime",
                            "exchangeTradeTime",
                            default=None
                        )
                        or
                        now_string()
                    )

                    item[
                        "received"
                    ] = time.time()

            save_ltp_cache()

        except Exception as e:

            print(
                "Option REST seed error:",
                e
            )


# ============================================================
# ANGEL ONE OPENING OI BASELINE
# ============================================================
def _angel_historical_oi(token, from_dt, to_dt):
    """Fetch Angel's official historical OI for one NFO token."""
    if not auth_token or not token:
        return 0
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/historical/v1/getOIData"
    headers = {
        "Authorization": "Bearer " + str(auth_token),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-PrivateKey": API_KEY,
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "106.51.24.1",
        "X-MACAddress": "00:00:00:00:00:00",
    }
    payload = {
        "exchange": "NFO",
        "symboltoken": str(token),
        "interval": "ONE_MINUTE",
        "fromdate": from_dt,
        "todate": to_dt,
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code >= 300:
            return 0
        body = r.json() or {}
        rows = body.get("data") or []
        if not isinstance(rows, list):
            return 0
        # The first available OI observation at/after 09:15 is the same
        # opening-day reference used for the intraday OI-change story.
        rows = sorted(rows, key=lambda x: str(x.get("time") or ""))
        for row in rows:
            oi = safe_int(row.get("oi"))
            if oi > 0:
                return oi
    except Exception as e:
        print("Historical OI baseline warning:", token, repr(e), flush=True)
    return 0


def seed_opening_oi_baseline_from_angel():
    """Seed today's OI baseline from Angel historical OI, not Render startup OI.

    Angel Smart Stream explicitly documents its OI-change-% packet field as a
    dummy/garbage value, so we must not derive the app's OI change from it.
    """
    if not state.get("logged_in") or not selected_expiry:
        return
    today = today_string()
    with baseline_lock:
        ready = (
            baseline.get("date") == today
            and baseline.get("expiry") == selected_expiry
            and baseline.get("source") == "ANGEL_HISTORICAL_09:15"
            and len(baseline.get("values") or {}) > 0
        )
    if ready:
        return

    now_s = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
    from_s = f"{today} 09:15"
    visible = set(get_visible_strikes())
    contracts = [
        c for c in get_expiry_contracts()
        if safe_float(c.get("strike")) in visible
    ]
    values = {}
    print("Seeding ANGEL opening OI baseline:", len(contracts), "visible contracts", flush=True)

    # Historical OI is token-scoped. Stay conservative with request rate.
    for idx, contract in enumerate(contracts):
        token = str(contract.get("token") or "")
        symbol = str(contract.get("symbol") or "")
        if token and symbol:
            oi0 = _angel_historical_oi(token, from_s, now_s)
            if oi0 > 0:
                values[symbol] = oi0
        if idx + 1 < len(contracts):
            time.sleep(0.36)

    if not values:
        print("ANGEL opening OI baseline unavailable; keeping existing baseline", flush=True)
        return

    with baseline_lock:
        baseline["date"] = today
        baseline["expiry"] = selected_expiry
        baseline["source"] = "ANGEL_HISTORICAL_09:15"
        baseline["values"] = values
    save_baseline()
    print("ANGEL opening OI baseline ready:", len(values), "contracts", flush=True)


# ============================================================
# ANGEL ONE LIVE GREEKS
# ============================================================

def _greeks_auth_error(response):
    """Return True when Angel says the current JWT/session is invalid."""
    if not isinstance(response, dict):
        return False
    message = str(response.get("message") or "").lower()
    code = str(response.get("errorCode") or response.get("errorcode") or "").upper()
    return code in {"AG8001", "AB1010", "AB1011"} or "invalid token" in message or "token expired" in message


def _greeks_headers(token):
    return {
        "Authorization": "Bearer " + str(token),
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": os.getenv("ANGEL_CLIENT_LOCAL_IP", "127.0.0.1"),
        "X-ClientPublicIP": os.getenv("ANGEL_CLIENT_PUBLIC_IP", "0.0.0.0"),
        "X-MACAddress": os.getenv("ANGEL_MAC_ADDRESS", "00:00:00:00:00:00"),
        "X-PrivateKey": API_KEY,
    }


def _request_greeks_rest(request_data):
    """Call the current official Angel One Option Greeks endpoint."""
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/marketData/v1/optionGreek"
    token = get_auth_token()
    if not token:
        raise RuntimeError("Angel One JWT token is not available")

    response = greeks_http.post(
        url,
        headers=_greeks_headers(token),
        json=request_data,
        timeout=(2, 6),
    )
    try:
        payload = response.json()
    except Exception:
        payload = {"status": False, "message": response.text[:300], "errorcode": str(response.status_code)}

    if _greeks_auth_error(payload):
        raise RuntimeError("AG8001: Invalid Token")
    if response.status_code >= 400:
        raise RuntimeError("Greeks HTTP %s: %s" % (response.status_code, payload))
    return payload


def angel_option_greeks_request(request_data, _retry=True):
    """Fetch Greeks with a fresh-session retry on AG8001/expired JWT.

    The previous code accepted an AG8001 response as if it were a normal API
    result, so the caller kept the old snapshot and printed "Live Greeks".
    This function never treats an authentication failure as success.
    """
    last_error = None

    # Use the current SDK object first when its optionGreek implementation is
    # available.  Crucially, inspect the returned JSON for AG8001 too; SDKs can
    # return the error dict without raising an exception.
    if obj and hasattr(obj, "optionGreek") and callable(getattr(obj, "optionGreek")):
        try:
            result = obj.optionGreek(request_data)
            if result:
                if _greeks_auth_error(result):
                    raise RuntimeError("AG8001: Invalid Token")
                return result
        except Exception as exc:
            last_error = exc

    try:
        return _request_greeks_rest(request_data)
    except Exception as exc:
        last_error = exc

    if _retry and ("AG8001" in str(last_error) or "Invalid Token" in str(last_error) or "expired" in str(last_error).lower()):
        print("GREEKS: JWT invalid/expired -> refreshing Angel One session", flush=True)
        if angel_login():
            try:
                # The newly-created SmartConnect is now the SDK object and
                # auth_token is the fresh JWT, so the retry is genuinely fresh.
                return angel_option_greeks_request(request_data, _retry=False)
            except Exception as retry_error:
                last_error = retry_error

    raise RuntimeError(last_error or "Angel One optionGreek request failed")


def fetch_option_greeks():
    global greeks_data

    if not obj or not selected_expiry:
        return False

    try:
        request_data = {
            "name": "NIFTY",
            "expirydate": format_expiry(selected_expiry),
        }
        response = angel_option_greeks_request(request_data)

        if not isinstance(response, dict) or not response.get("status"):
            message = response.get("message", "Unknown Greeks API error") if isinstance(response, dict) else str(response)
            state["last_error"] = "Greeks: " + str(message)
            print("GREEKS API FAILED:", message, flush=True)
            return False

        rows = response.get("data") or []
        if not isinstance(rows, list):
            rows = []

        valid_contracts = {}
        for contract in get_expiry_contracts():
            contract_type = str(
                contract.get("option_type") or contract.get("optionType") or
                contract.get("optiontype") or contract.get("instrument_type") or ""
            ).upper().strip()
            key = f"{normalize_strike(contract.get('strike')):.2f}_{contract_type}"
            valid_contracts[key] = contract

        new_data = {}
        for row in rows:
            if not isinstance(row, dict):
                continue

            row_expiry = row.get("expiryDate", row.get("expiry", row.get("expirydate")))
            if row_expiry and format_expiry(row_expiry) != format_expiry(selected_expiry):
                continue

            strike = normalize_strike(row.get("strikePrice"))
            option_type = str(row.get("optionType", row.get("optiontype", ""))).upper().strip()
            if strike <= 0 or option_type not in ("CE", "PE"):
                continue

            key = f"{strike:.2f}_{option_type}"
            if key not in valid_contracts:
                continue

            new_data[key] = {
                "iv": safe_float(row.get("impliedVolatility")),
                "delta": safe_float(row.get("delta")),
                "gamma": safe_float(row.get("gamma")),
                "theta": safe_float(row.get("theta")),
                "vega": safe_float(row.get("vega")),
                "trade_volume": safe_float(row.get("tradeVolume")),
                "updated": now_string(),
            }

        if not new_data:
            state["greeks_count"] = 0
            state["last_error"] = "Greeks API returned 0 matched NIFTY contracts for " + str(selected_expiry)
            print("GREEKS API: success response but 0 matched contracts", flush=True)
            return False

        with greeks_lock:
            greeks_data = dict(new_data)

        state["greeks_update"] = now_string()
        state["greeks_count"] = len(new_data)
        state["last_error"] = ""
        print("LIVE GREEKS OK:", len(new_data), "contracts; expiry=", selected_expiry, flush=True)
        # Greeks are REST snapshots, not Smart Stream ticks. Broadcast only
        # when the snapshot actually changes; never do this work in process_tick.
        try:
            publish_tick({"type": "greeks", "greeks_update": state["greeks_update"], "greeks_count": len(new_data), "payload": make_payload()})
        except Exception as broadcast_error:
            print("Greeks broadcast warning:", repr(broadcast_error), flush=True)
        return True

    except Exception as e:
        state["last_error"] = "Greeks API error: " + str(e)
        print("GREEKS API ERROR:", repr(e), flush=True)
        return False


# ============================================================
# GREEKS THREAD
# ============================================================

def greeks_loop():
    """Disabled periodic polling. Angel Greeks are REST snapshots, not ticks."""
    print("Periodic Greeks polling DISABLED — Angel Greeks are snapshot data.", flush=True)
    while running:
        time.sleep(60)


# ============================================================
# REST MARKET DATA THREAD
# ============================================================

def market_data_loop():
    """Disabled: market data is SmartWebSocketV2 tick-only.

    REST market-data polling is deliberately not used. Historical candles and
    one-time Greeks snapshots use their dedicated Angel REST APIs.
    """
    print("REST market-data polling DISABLED — SmartWebSocketV2 is the live source.", flush=True)
    while running:
        time.sleep(60)


# ============================================================
# WEBSOCKET
# ============================================================

def get_auth_token():

    global auth_token

    if auth_token:
        return auth_token

    if obj:

        value = getattr(
            obj,
            "access_token",
            None
        )

        if value:
            return value

        value = getattr(
            obj,
            "jwtToken",
            None
        )

        if value:
            return value

    return ""


def ensure_visible_chain_symbols():
    """Create empty rows for every currently visible CE/PE contract.
    Values are populated only by SmartWebSocketV2 ticks.
    """
    for contract in get_expiry_contracts():
        ensure_live_symbol(contract["symbol"])


def subscribe_live_tokens(wsapp, force=False):

    global subscribed_option_tokens

    new_tokens = set(get_subscription_tokens())

    with subscribed_tokens_lock:
        old_tokens = set(subscribed_option_tokens)

    if not force and new_tokens == old_tokens:
        return

    try:
        # Remove the previous option set first. This prevents old ATM strikes
        # from accumulating when NIFTY crosses strikes.
        if old_tokens:
            wsapp.unsubscribe(
                "NIFTYOPT01",
                WS_OPTION_MODE,
                [{
                    "exchangeType": NFO_EXCHANGE_TYPE,
                    "tokens": sorted(
                        old_tokens,
                        key=lambda x: int(x) if str(x).isdigit() else str(x)
                    )
                }]
            )
    except Exception as e:
        print("Live NFO unsubscribe warning:", e)

    if new_tokens:
        wsapp.subscribe(
            "NIFTYOPT01",
            WS_OPTION_MODE,
            [{
                "exchangeType": NFO_EXCHANGE_TYPE,
                "tokens": sorted(
                    new_tokens,
                    key=lambda x: int(x) if str(x).isdigit() else str(x)
                )
            }]
        )

    with subscribed_tokens_lock:
        subscribed_option_tokens = set(new_tokens)

    print("Live NFO subscription:", len(new_tokens), "tokens; mode:", WS_OPTION_MODE)


class DirectAngelOneWS:
    """Direct Angel One Smart Stream client; bypasses SmartWebSocketV2."""
    ROOT_URI = "wss://smartapisocket.angelone.in/smart-stream"

    def __init__(self, auth_token, api_key, client_code, feed_token):
        self.auth_token = auth_token
        self.api_key = api_key
        self.client_code = client_code
        self.feed_token = feed_token
        self.wsapp = None
        self.on_open = None
        self.on_data = None
        self.on_error = None
        self.on_close = None
        self._closed = False

    def subscribe(self, correlation_id, mode, token_list):
        if not self.wsapp:
            raise RuntimeError("Direct Angel WS is not connected")
        payload = {"correlationID": str(correlation_id), "action": 1,
                   "params": {"mode": int(mode), "tokenList": token_list}}
        self.wsapp.send(json.dumps(payload))
        print("DIRECT WS SUBSCRIBE:", mode, sum(len(x.get("tokens", [])) for x in token_list), flush=True)

    def unsubscribe(self, correlation_id, mode, token_list):
        if not self.wsapp:
            return
        payload = {"correlationID": str(correlation_id), "action": 0,
                   "params": {"mode": int(mode), "tokenList": token_list}}
        self.wsapp.send(json.dumps(payload))

    def close_connection(self):
        self._closed = True
        if self.wsapp:
            try:
                self.wsapp.close()
            except Exception:
                pass

    @staticmethod
    def _token(b):
        return b.split(b"\x00", 1)[0].decode("utf-8", errors="replace")

    @staticmethod
    def _q(b, a, z):
        return struct.unpack("<q", b[a:z])[0]

    @staticmethod
    def _d(b, a, z):
        return struct.unpack("<d", b[a:z])[0]

    def parse_packet(self, b):
        if not isinstance(b, (bytes, bytearray)) or len(b) < 51:
            return None
        mode = b[0]
        if mode not in (1, 2, 3, 4):
            return None
        d = {
            "subscription_mode": mode,
            "exchange_type": b[1],
            "token": self._token(bytes(b[2:27])),
            "sequence_number": self._q(b,27,35),
            "exchange_timestamp": self._q(b,35,43),
            "last_traded_price": self._q(b,43,51),
        }
        if mode in (2,3) and len(b) >= 123:
            d.update({
                "last_traded_quantity": self._q(b,51,59),
                "average_traded_price": self._q(b,59,67),
                "volume_trade_for_the_day": self._q(b,67,75),
                "total_buy_quantity": self._d(b,75,83),
                "total_sell_quantity": self._d(b,83,91),
                "open_price_of_the_day": self._q(b,91,99),
                "high_price_of_the_day": self._q(b,99,107),
                "low_price_of_the_day": self._q(b,107,115),
                "closed_price": self._q(b,115,123),
            })
        if mode == 3 and len(b) >= 147:
            d["last_traded_timestamp"] = self._q(b,123,131)
            d["open_interest"] = self._q(b,131,139)
            d["open_interest_change_percentage"] = self._q(b,139,147)
        return d

    def _on_open(self, wsapp):
        print("DIRECT ANGEL ONE WS CONNECTED", flush=True)
        if self.on_open:
            self.on_open(self)

    def _on_message(self, wsapp, message):
        state["raw_ws_messages"] = safe_int(state.get("raw_ws_messages", 0)) + 1
        state["last_ws_message_at"] = time.time()
        if isinstance(message, str):
            if message == "pong":
                # Transport heartbeat only; never treat pong as a market-data tick.
                return
            # Angel control/error messages are kept for diagnostics.
            if message.strip():
                state["last_error"] = "DIRECT WS TEXT: " + message[:300]
                print("DIRECT WS TEXT:", message[:500], flush=True)
            return
        try:
            parsed = self.parse_packet(message)
            if parsed:
                state["last_ws_token"] = parsed.get("token", "")
                # A binary Angel packet is real market-stream traffic. Keep a
                # separate timestamp so the watchdog can detect a frozen feed
                # even when websocket ping/pong is still healthy.
                state["last_market_packet_at"] = time.time()
                if self.on_data:
                    self.on_data(self, parsed)
        except Exception as e:
            state["last_error"] = "Direct WS parse: " + str(e)
            print("DIRECT WS PARSE ERROR:", repr(e), flush=True)

    def _on_error(self, wsapp, error):
        state["websocket"] = "ERROR"
        state["last_error"] = str(error)
        print("ANGEL ONE DIRECT WS ERROR:", repr(error), flush=True)
        if self.on_error:
            self.on_error(self, error)

    def _on_close(self, wsapp, code, msg):
        state["websocket"] = "DISCONNECTED"
        print("ANGEL ONE DIRECT WS CLOSED:", code, msg, flush=True)
        if self.on_close:
            self.on_close(self)

    def connect(self):
        self._closed = False
        headers = [
            "Authorization: " + str(self.auth_token),
            "x-api-key: " + str(self.api_key),
            "x-client-code: " + str(self.client_code),
            "x-feed-token: " + str(self.feed_token),
        ]
        self.wsapp = websocket.WebSocketApp(
            self.ROOT_URI,
            header=headers,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.wsapp.run_forever(
            sslopt={"cert_reqs": ssl.CERT_NONE},
            ping_interval=10,
            ping_timeout=5,
        )


def ensure_visible_chain_symbols():
    """Create empty rows for every currently visible CE/PE contract.
    Values are populated only by SmartWebSocketV2 ticks.
    """
    for contract in get_expiry_contracts():
        ensure_live_symbol(contract["symbol"])


def subscribe_live_tokens(wsapp, force=False):
    """Subscribe visible NIFTY option tokens through the SmartWebSocketV2 wrapper.

    Angel One's current SmartWebSocketV2 SDK stores unsubscribe requests in a
    structure that can interfere with its automatic resubscribe path. We keep
    the stream additive instead: old subscriptions are harmless, and only
    newly visible ATM strikes are added.
    """
    global subscribed_option_tokens
    new_tokens = set(get_subscription_tokens())
    with subscribed_tokens_lock:
        old_tokens = set(subscribed_option_tokens)
    if not force and new_tokens.issubset(old_tokens):
        return
    to_add = new_tokens if force else (new_tokens - old_tokens)
    if not to_add:
        return
    ordered = sorted(to_add, key=lambda x: int(x) if str(x).isdigit() else str(x))
    try:
        wsapp.subscribe(
            "NIFTYOPT01",
            WS_OPTION_MODE,
            [{"exchangeType": NFO_EXCHANGE_TYPE, "tokens": ordered}],
        )
        with subscribed_tokens_lock:
            subscribed_option_tokens.update(to_add)
        print("DIRECT WS NFO SUBSCRIPTION ADDED:", len(to_add), "tokens", flush=True)
    except Exception as e:
        state["last_error"] = "NFO subscription error: " + str(e)
        print("DIRECT WS NFO SUBSCRIBE ERROR:", repr(e), flush=True)
        raise


def subscribe_live_tokens(sws_client, force=False):
    """Subscribe visible NIFTY options through the official Angel wrapper.

    SmartWebSocketV2's callback receives the underlying WebSocketApp, not the
    SmartWebSocketV2 object. Therefore subscription calls MUST use the wrapper
    instance held here, never the callback argument.
    """
    global subscribed_option_tokens
    if sws_client is None:
        raise RuntimeError("SmartWebSocketV2 client is not ready")
    new_tokens = set(get_subscription_tokens())
    with subscribed_tokens_lock:
        old_tokens = set(subscribed_option_tokens)
    if not force and new_tokens.issubset(old_tokens):
        return
    to_add = new_tokens if force else (new_tokens - old_tokens)
    if not to_add:
        return
    ordered = sorted(to_add, key=lambda x: int(x) if str(x).isdigit() else str(x))
    sws_client.subscribe(
        "NIFTYOPT01", WS_OPTION_MODE,
        [{"exchangeType": NFO_EXCHANGE_TYPE, "tokens": ordered}],
    )
    with subscribed_tokens_lock:
        subscribed_option_tokens.update(to_add)
    print("ANGEL SMART WS NFO SUBSCRIPTION:", len(to_add), "tokens", flush=True)


def websocket_on_open(wsapp):
    """Official SmartWebSocketV2 on_open callback.

    `wsapp` is intentionally NOT used for subscribe(). Angel's official sample
    calls `sws.subscribe(...)` from the closure because wsapp is only the raw
    WebSocketApp.
    """
    global active_sws, subscribed_option_tokens, last_subscription_atm
    state["websocket"] = "CONNECTED"
    state["last_error"] = ""
    state["last_market_packet_at"] = time.time()
    with subscribed_tokens_lock:
        subscribed_option_tokens = set()
    last_subscription_atm = None
    try:
        client = active_sws
        if client is None:
            raise RuntimeError("SmartWebSocketV2 client missing in on_open")
        client.subscribe(
            "NIFTYIDX01", WS_SPOT_MODE,
            [{"exchangeType": NSE_EXCHANGE_TYPE, "tokens": [NIFTY_TOKEN]}],
        )
        subscribe_live_tokens(client, force=True)
        print("ANGEL SMART WS SUBSCRIPTIONS SENT: NIFTY + OPTIONS", flush=True)
    except Exception as e:
        state["websocket"] = "ERROR"
        state["last_error"] = "Angel Smart WS subscribe error: " + str(e)
        print("ANGEL SMART WS SUBSCRIBE ERROR:", repr(e), flush=True)


def websocket_on_data(wsapp, message, *callback_extra):
    """Accept both SmartApi SDK callback payloads and raw binary frames.

    SDK releases have differed in the callback argument shape.  Keep the
    callback tolerant, but count only actual market-data payloads (not pongs).
    """
    try:
        payload = message
        if isinstance(payload, str):
            if payload.strip().lower() in ("pong", "ping", ""):
                return
            try:
                payload = json.loads(payload)
            except Exception:
                state["last_error"] = "Angel WS text frame: " + payload[:250]
                print("ANGEL WS TEXT FRAME:", payload[:300], flush=True)
                return

        if isinstance(payload, (bytes, bytearray)):
            payload = DirectAngelOneWS.parse_packet(payload)
            if payload is None:
                state["last_error"] = "Angel WS binary frame could not be decoded"
                return

        if isinstance(payload, dict) and isinstance(payload.get("data"), dict) and not payload.get("token"):
            payload = payload["data"]

        if not isinstance(payload, dict):
            state["last_error"] = "Unsupported Angel WS payload: " + type(payload).__name__
            return

        state["raw_ws_messages"] = safe_int(state.get("raw_ws_messages", 0)) + 1
        state["last_ws_message_at"] = time.time()
        state["last_market_packet_at"] = time.time()
        process_tick(payload)
    except Exception as e:
        state["last_error"] = "WebSocket tick error: " + str(e)
        print("ANGEL ONE WS TICK ERROR:", repr(e), flush=True)


def websocket_on_error(wsapp=None, error=None, *extra):
    reason = error
    if reason is None and extra:
        reason = extra[-1]
    if reason is None:
        reason = "Unknown Smart Stream error"
    state["websocket"] = "ERROR"
    state["last_error"] = "Angel Smart Stream: " + str(reason)
    print("ANGEL ONE WS ERROR RAW:", repr(reason), flush=True)
    # Force the outer reconnect loop to regain control if the SDK leaves a
    # failed socket open after an error callback.
    current = active_sws
    if current is not None:
        try:
            current.close_connection()
        except Exception as close_error:
            print("Angel WS close-after-error warning:", repr(close_error), flush=True)


def websocket_on_close(wsapp=None, *close_details):
    global subscribed_option_tokens
    state["websocket"] = "DISCONNECTED"
    if close_details:
        state["last_error"] = "Angel Smart Stream closed: " + " ".join(str(x) for x in close_details)[:250]
    with subscribed_tokens_lock:
        subscribed_option_tokens = set()
    print("Angel One SmartWebSocketV2 closed; reconnect loop will retry.", flush=True)


def start_websocket():
    """Run Angel One SmartWebSocketV2 with an explicit outer reconnect loop."""
    global ws, feed_token, active_sws
    print("Starting OFFICIAL Angel One SmartWebSocketV2", flush=True)
    failure_count = 0
    while running:
        client = None
        try:
            if not state.get("logged_in"):
                if not angel_login():
                    time.sleep(WS_RECONNECT_SECONDS)
                    continue
            token = get_auth_token()
            if not token:
                raise RuntimeError("JWT token unavailable")
            if not feed_token and obj:
                feed_token = obj.getfeedToken() or ""
            if not feed_token:
                raise RuntimeError("Feed token unavailable")

            state["websocket"] = "CONNECTING"
            state["last_error"] = ""
            state["ws_connect_started_at"] = time.time()
            state["ws_reconnects"] = safe_int(state.get("ws_reconnects", 0)) + (1 if failure_count else 0)
            client = SmartWebSocketV2(
                token, API_KEY, CLIENT_CODE, feed_token,
                max_retry_attempt=2, retry_strategy=0,
                retry_delay=5, retry_duration=30,
            )
            active_sws = client
            ws = client
            client.on_open = websocket_on_open
            client.on_data = websocket_on_data
            client.on_error = websocket_on_error
            client.on_close = websocket_on_close
            print("Angel SmartWebSocketV2 connect() starting; SDK=smartapi-python", flush=True)
            client.connect()
            # connect() normally blocks until the socket closes. If it returns,
            # treat that as a disconnect rather than silently starting over.
            failure_count += 1
            state["websocket"] = "DISCONNECTED"
            state["last_error"] = "Angel Smart Stream ended; reconnecting"
            print("Angel SmartWebSocketV2 connect() returned; reconnecting", flush=True)
        except Exception as e:
            failure_count += 1
            state["websocket"] = "ERROR"
            state["last_error"] = "Angel Smart WS connection: " + str(e)
            print("ANGEL SMART WS CONNECTION ERROR:", repr(e), flush=True)
        finally:
            if client is not None:
                try:
                    client.close_connection()
                except Exception:
                    pass
            if active_sws is client:
                active_sws = None
            if ws is client:
                ws = None
            with subscribed_tokens_lock:
                subscribed_option_tokens.clear()

        if not running:
            break
        if failure_count >= 3:
            try:
                print("Angel WS reconnect threshold reached; refreshing login session", flush=True)
                relogin()
            except Exception as re:
                print("ANGEL SMART WS re-login error:", repr(re), flush=True)
            failure_count = 0
        time.sleep(WS_RECONNECT_SECONDS)

# ============================================================
# WEBSOCKET WATCHDOG
# ============================================================
def websocket_watchdog_loop():
    while running:
        try:
            if state.get("websocket") == "CONNECTED":
                # Do not let websocket ping/pong hide a frozen market feed.
                # During market hours, reconnect when no binary market packet
                # has arrived for 12 seconds. Outside market hours the stream
                # can legitimately be quiet.
                last_market = safe_float(state.get("last_market_packet_at", 0))
                age = time.time() - last_market if last_market > 0 else 999
                if is_market_open() and age > 15:
                    state["last_error"] = f"Angel WS watchdog: no market-data frame for {age:.1f}s; reconnecting"
                    print(f"ANGEL WS WATCHDOG: no market-data frame for {age:.1f}s; reconnecting", flush=True)
                    current = active_sws or ws
                    if current is not None:
                        current.close_connection()
            time.sleep(1)
        except Exception as e:
            print("DIRECT WS watchdog error:", repr(e), flush=True)
            time.sleep(1)


# ============================================================
# RANKING
# ============================================================

def percentage(
    value,
    maximum
):

    value = safe_float(
        value
    )

    maximum = safe_float(
        maximum
    )

    if maximum <= 0:
        return 0.0

    return round(
        value / maximum * 100,
        2
    )


def two_rank_values(
    values
):
    """
    Returns:
        highest unique positive value
        second-highest unique positive value

    Ties are treated as the same rank.
    """

    positive = sorted(
        {
            safe_float(v)
            for v in values
            if safe_float(v) > 0
        },
        reverse=True
    )

    highest = (
        positive[0]
        if positive
        else 0.0
    )

    second = (
        positive[1]
        if len(positive) > 1
        else 0.0
    )

    return (
        highest,
        second
    )


def rank_color_class(
    value,
    highest,
    second
):

    value = safe_float(
        value
    )

    if (
        value > 0
        and highest > 0
        and abs(
            value - highest
        ) < 1e-9
    ):

        return "rank-highest"

    if (
        value > 0
        and second > 0
        and abs(
            value - second
        ) < 1e-9
        and abs(
            second - highest
        ) >= 1e-9
    ):

        return "rank-second"

    return ""


def rank_status(
    value,
    maximum
):

    value = safe_float(
        value
    )

    maximum = safe_float(
        maximum
    )

    if maximum <= 0:

        return {
            "percent": 0,
            "rank": "",
            "shift": "",
            "class": ""
        }

    pct = (
        value
        / maximum
        * 100
    )

    return {
        "percent": round(
            pct,
            2
        ),
        "rank": "",
        "shift": "",
        "class": ""
    }


# ============================================================
# LTP CALCULATOR STYLE SUPPORT / RESISTANCE ENGINE
# ============================================================
# Source-derived rule: start from the pair of strikes around the
# imaginary line and move OTM. The highest Volume or highest OI,
# whichever is closest to that line, defines Support (PE) or
# Resistance (CE). If both maxima occur at the same strike, the
# level is both Volume + OI.
#
# The 75% second-highest rule is used only for WTT/WTB weakness:
# second-highest value / highest value >= 75% => yellow/weak candidate.
# When that candidate itself becomes the highest value, the level has
# shifted.
# ============================================================

coa_previous_support = 0.0
coa_previous_resistance = 0.0

# SHIFT-GATED STATE:
# WTB/WTT must NOT move merely because the live OI/Volume ranking flickers.
# A new level is accepted only when the previously identified weak candidate
# actually becomes the new highest level (the shift is completed). Until then
# the visible CALL/PUT state stays latched.
coa_pending_support = 0.0
coa_pending_resistance = 0.0
coa_latched_support_status = "STRONG"
coa_latched_resistance_status = "STRONG"

# Persistent last-shift story.  The floating boxes show the CURRENT state;
# these two stories keep the last actual level transition visible at the top.
coa_story_support = {"from": 0.0, "to": 0.0, "status": "STRONG"}
coa_story_resistance = {"from": 0.0, "to": 0.0, "status": "STRONG"}

def _side_value(row, side, field):
    if side == "ce":
        data = row.get("ce_data", {})
        if field == "volume":
            return safe_float(row.get("ce_volume", 0))
    else:
        data = row.get("pe_data", {})
        if field == "volume":
            return safe_float(row.get("pe_volume", 0))
    return safe_float(data.get(field, 0))


def _max_strikes(rows, side, field):
    pairs = [(row.get("strike", 0), _side_value(row, side, field)) for row in rows]
    positive = [(strike, value) for strike, value in pairs if value > 0]
    if not positive:
        return 0.0, []
    maximum = max(value for _, value in positive)
    strikes = [strike for strike, value in positive if abs(value - maximum) < 1e-9]
    return maximum, strikes


def _imaginary_pair(strikes, spot):
    strikes = sorted(set(strikes))
    if not strikes:
        return 0.0, 0.0
    below = [x for x in strikes if x <= spot]
    above = [x for x in strikes if x >= spot]
    if below and above:
        lo, hi = below[-1], above[0]
        if abs(lo - hi) < 1e-9:
            idx = strikes.index(lo)
            lo = strikes[idx - 1] if idx > 0 else lo
        return lo, hi
    if above:
        hi = above[0]
        idx = strikes.index(hi)
        lo = strikes[idx - 1] if idx > 0 else hi
        return lo, hi
    lo = below[-1]
    idx = strikes.index(lo)
    hi = strikes[idx + 1] if idx + 1 < len(strikes) else lo
    return lo, hi


def _closest_max_from_imaginary(rows, side, field, spot, direction):
    strikes = sorted(row["strike"] for row in rows)
    lo, hi = _imaginary_pair(strikes, spot)
    maximum, max_strikes = _max_strikes(rows, side, field)
    if maximum <= 0:
        return 0.0, maximum, lo, hi

    # Put/support: start at the upper member of the imaginary pair, then
    # move downward. Call/resistance: start at the lower member, then move upward.
    ordered = sorted(strikes, reverse=(direction == "support"))
    if direction == "support":
        ordered = [x for x in ordered if x <= hi]
        ordered = [hi] + [x for x in ordered if abs(x-hi) > 1e-9]
    else:
        ordered = [x for x in ordered if x >= lo]
        ordered = [lo] + [x for x in ordered if abs(x-lo) > 1e-9]

    for strike in ordered:
        if any(abs(strike - m) < 1e-9 for m in max_strikes):
            return strike, maximum, lo, hi
    return 0.0, maximum, lo, hi


def _second_rank_candidate(rows, side, field, current_level, direction):
    values = sorted(set(_side_value(r, side, field) for r in rows if _side_value(r, side, field) > 0), reverse=True)
    if len(values) < 2:
        return 0.0, 0.0
    highest, second = values[0], values[1]
    pct = second / highest * 100 if highest > 0 else 0.0
    if pct < 75.0:
        return 0.0, round(pct, 2)

    candidates = [r["strike"] for r in rows if abs(_side_value(r, side, field)-second) < 1e-9]
    if direction == "WTB":
        candidates = [x for x in candidates if x < current_level]
    elif direction == "WTT":
        candidates = [x for x in candidates if x > current_level]
    if not candidates:
        return 0.0, round(pct, 2)
    candidate = min(candidates, key=lambda x: abs(x-current_level))
    return candidate, round(pct, 2)


def _empty_shift_history():
    return {
        "date": today_string(),
        "expiry": selected_expiry or "",
        "call": [],
        "put": [],
    }


def load_shift_history():
    global shift_history
    fresh = _empty_shift_history()
    with shift_history_lock:
        shift_history = fresh
        if not os.path.exists(SHIFT_HISTORY_FILE):
            return
        try:
            with open(SHIFT_HISTORY_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f) or {}
            if (saved.get("date") == today_string() and
                    saved.get("expiry") == selected_expiry):
                shift_history = {
                    "date": saved.get("date", today_string()),
                    "expiry": saved.get("expiry", selected_expiry or ""),
                    "call": list(saved.get("call") or []),
                    "put": list(saved.get("put") or []),
                }
        except Exception as e:
            print("Shift history load error:", e)


def save_shift_history():
    temp = SHIFT_HISTORY_FILE + ".tmp"
    try:
        with shift_history_lock:
            with open(temp, "w", encoding="utf-8") as f:
                json.dump(shift_history, f, indent=2)
            os.replace(temp, SHIFT_HISTORY_FILE)
    except Exception as e:
        print("Shift history save error:", e)
        try:
            if os.path.exists(temp):
                os.remove(temp)
        except Exception:
            pass


def _normalize_story_source(value):
    """Normalize the factor that caused/represents a story state."""
    raw = str(value or "").strip().upper().replace("+", " + ")
    raw = raw.replace("VOL", "VOLUME")
    if raw in ("OI", "OPEN INTEREST"):
        return "OI"
    if raw in ("VOLUME",):
        return "VOLUME"
    if "OI" in raw and "VOLUME" in raw:
        return "OI + VOLUME"
    return raw


def record_shift_history(side, story, event_status=None):
    """Record genuine CALL/PUT state transitions, including OI vs VOLUME source."""
    if not isinstance(story, dict):
        return
    side_key = "call" if str(side).lower() == "call" else "put"
    status = str(event_status or story.get("status") or "STRONG").upper()
    # HISTORY/ENTRY RULE: only a completed SHIFT is an entry.
    # STRONG/WTB/WTT are live states and must never create history entries.
    if status != "SHIFT":
        return
    from_level = safe_float(story.get("from"))
    to_level = safe_float(story.get("to"))
    if from_level <= 0 or to_level <= 0:
        return
    if abs(to_level - from_level) < 1e-9:
        return

    # Stable = no movement.  WTB/WTT are the live weak directions.  SHIFT is
    # a completed move; its direction is kept separately for UI coloring.
    if status == "SHIFT":
        direction = "WTT" if to_level > from_level else ("WTB" if to_level < from_level else "STRONG")
    else:
        direction = status if status in ("WTB", "WTT") else "STRONG"

    source = _normalize_story_source(story.get("source") or story.get("field"))
    event = {
        "time": now_string(),
        "status": status,
        "direction": direction,
        "source": source,
        "from": from_level,
        "to": to_level,
        "changed": bool(from_level > 0 and to_level > 0 and abs(to_level - from_level) > 1e-9),
    }

    changed = False
    with shift_history_lock:
        if shift_history.get("date") != today_string() or shift_history.get("expiry") != (selected_expiry or ""):
            shift_history.clear()
            shift_history.update(_empty_shift_history())
        items = shift_history.setdefault(side_key, [])
        if not items:
            items.append(event)
            changed = True
        else:
            last = items[-1]
            same_state = (
                str(last.get("status", "")).upper() == status and
                _normalize_story_source(last.get("source") or last.get("field")) == source and
                abs(safe_float(last.get("from")) - from_level) < 1e-9 and
                abs(safe_float(last.get("to")) - to_level) < 1e-9
            )
            if not same_state:
                items.append(event)
                # Keep a practical full-day list while avoiding unbounded growth.
                if len(items) > 500:
                    del items[:-500]
                changed = True
    if changed:
        save_shift_history()


def get_shift_history():
    with shift_history_lock:
        return {
            "date": shift_history.get("date", today_string()),
            "expiry": shift_history.get("expiry", selected_expiry or ""),
            "call": [dict(x) for x in shift_history.get("call", [])],
            "put": [dict(x) for x in shift_history.get("put", [])],
        }


def calculate_ltp_levels(rows, spot):
    global coa_previous_support, coa_previous_resistance
    global coa_pending_support, coa_pending_resistance
    global coa_latched_support_status, coa_latched_resistance_status
    global coa_story_support, coa_story_resistance

    support_oi, max_pe_oi, imag_lo, imag_hi = _closest_max_from_imaginary(rows, "pe", "oi", spot, "support")
    support_vol, max_pe_vol, _, _ = _closest_max_from_imaginary(rows, "pe", "volume", spot, "support")
    resistance_oi, max_ce_oi, _, _ = _closest_max_from_imaginary(rows, "ce", "oi", spot, "resistance")
    resistance_vol, max_ce_vol, _, _ = _closest_max_from_imaginary(rows, "ce", "volume", spot, "resistance")

    # LTP Calculator rule: the imaginary-line PAIR is the starting point.
    # CALL/resistance starts from the LOWER member and scans upward into CALL
    # OTM strikes. PUT/support starts from the UPPER member and scans downward
    # into PUT OTM strikes. The FIRST encountered maximum (highest OI or
    # highest Volume) is the level. If both maxima are on the same strike, the
    # level is jointly OI + VOLUME. Do NOT simply choose the numerically
    # closest maximum: the scan order itself is the rule.
    def choose(oi_level, vol_level, side):
        if oi_level <= 0 and vol_level <= 0:
            return 0.0, ""

        # Both _closest_max_from_imaginary calls have already applied the exact
        # scan direction. Their returned strike is therefore the first valid
        # maximum from the imaginary pair. Compare those positions only.
        if oi_level <= 0:
            return vol_level, "VOLUME"
        if vol_level <= 0:
            return oi_level, "OI"

        if abs(oi_level - vol_level) < 1e-9:
            return oi_level, "OI + VOLUME"

        # Same strike spacing is ordered by the direction from the imaginary
        # pair. For resistance, smaller strike is encountered first; for
        # support, larger strike is encountered first.
        if side == "RESISTANCE":
            level = min(oi_level, vol_level)
        else:
            level = max(oi_level, vol_level)

        kinds = []
        if abs(oi_level - level) < 1e-9:
            kinds.append("OI")
        if abs(vol_level - level) < 1e-9:
            kinds.append("VOLUME")
        return level, "+".join(kinds)

    support, support_source = choose(support_oi, support_vol, "SUPPORT")
    resistance, resistance_source = choose(resistance_oi, resistance_vol, "RESISTANCE")

    # ------------------------------------------------------------
    # WTB / WTT calculation from the LTP Calculator percentage game.
    # A yellow/weak candidate exists only when the SECOND-HIGHEST value
    # in the relevant OI/Volume column is >75% of the highest value.
    # Direction is decided by where that second-highest strike sits:
    #   Resistance: lower strike = WTB, higher strike = WTT
    #   Support:    higher strike = WTT, lower strike = WTB
    # If both OI and Volume are weak in opposite directions, the factor
    # whose candidate is closer to the imaginary line gets priority.
    # ------------------------------------------------------------
    def factor_state(side, field, level):
        """Return the active yellow/weak candidate for one OI/Volume column.

        LTP Calculator percentage-game rule: the second-highest value becomes
        a yellow candidate only after it reaches 75% of the highest value.
        A candidate affects the current S/R only after it crosses that S/R:
          resistance: lower strike = WTB, higher strike = WTT
          support:    lower strike = WTB, higher strike = WTT
        """
        if level <= 0:
            return None
        option_side = "pe" if side == "support" else "ce"
        pairs = []
        for r in rows:
            v = _side_value(r, option_side, field)
            if v > 0:
                pairs.append((float(v), float(r["strike"])))
        if not pairs:
            return None

        highest = max(v for v, _ in pairs)
        # Use the second DISTINCT value, as the LTP Calculator percentage
        # game does; duplicate rows at the same value are not a new rank.
        distinct = sorted({v for v, _ in pairs}, reverse=True)
        if len(distinct) < 2 or highest <= 0:
            return None
        second = distinct[1]
        pct = second / highest * 100.0
        if pct < 75.0:
            return None

        candidates = sorted({strike for v, strike in pairs if abs(v-second) < 1e-9})
        if side == "resistance":
            below = [x for x in candidates if x < level]
            above = [x for x in candidates if x > level]
            # A yellow candidate below resistance means WTB; above means WTT.
            # Only a candidate that crosses the existing resistance is active.
            if below:
                candidate = max(below)
                direction = "WTB"
            elif above:
                candidate = min(above)
                direction = "WTT"
            else:
                return None
        else:
            below = [x for x in candidates if x < level]
            above = [x for x in candidates if x > level]
            # A yellow candidate below support means WTB; above means WTT.
            if above:
                candidate = min(above)
                direction = "WTT"
            elif below:
                candidate = max(below)
                direction = "WTB"
            else:
                return None

        boundary = imag_hi if side == "support" else imag_lo
        return {
            "status": direction,
            "strike": candidate,
            "percent": round(pct, 2),
            "field": field.upper(),
            "distance_to_line": abs(candidate - boundary),
        }

    def weak_info(side, level):
        if level <= 0:
            return {"status": "STRONG", "strike": 0, "percent": 0, "field": ""}
        candidates = []
        for field in ("oi", "volume"):
            item = factor_state(side, field, level)
            if item:
                candidates.append(item)
        if not candidates:
            return {"status": "STRONG", "strike": level, "percent": 0, "field": ""}

        # When OI and Volume disagree, the factor moving closer to the
        # imaginary line has priority. This is the published LTP Calculator
        # WTB/WTT rule for mixed weak factors.
        candidates.sort(key=lambda x: (x["distance_to_line"], -x["percent"]))
        return candidates[0]

    support_weak = weak_info("support", support)
    resistance_weak = weak_info("resistance", resistance)

    # Each floating box must show the SECOND-HIGHEST percentage for its own
    # column, exactly like the Percentage Game: CE OI, CE Volume, PE Volume,
    # PE OI. The percentage is calculated independently for each factor.
    def second_percent(side, field):
        option_side = "pe" if side == "support" else "ce"
        vals = sorted({
            _side_value(r, option_side, field)
            for r in rows
            if _side_value(r, option_side, field) > 0
        }, reverse=True)
        if len(vals) < 2 or vals[0] <= 0:
            return 0.0
        return round(vals[1] / vals[0] * 100.0, 2)

    ce_oi_second_pct = second_percent("resistance", "oi")
    ce_vol_second_pct = second_percent("resistance", "volume")
    pe_oi_second_pct = second_percent("support", "oi")
    pe_vol_second_pct = second_percent("support", "volume")

    # ============================================================
    # LTP CALCULATOR SHIFT ENGINE
    # ============================================================
    # IMPORTANT:
    #   1) WTB/WTT is the CURRENT live condition from the LTP Calculator
    #      OI + VOLUME percentage engine above. It must be visible immediately.
    #   2) SHIFT history is separately gated: a SHIFT is recorded only when
    #      the previously identified weak candidate actually becomes the
    #      selected level. A temporary ranking flicker is NOT a SHIFT.
    #   3) Only the valid weak states WTB and WTT are allowed.

    raw_support = support
    raw_resistance = resistance
    old_support = coa_previous_support
    old_resistance = coa_previous_resistance
    support_shift = False
    resistance_shift = False

    # First live level of the session establishes the morning reference.
    if coa_previous_support <= 0 and raw_support > 0:
        coa_previous_support = raw_support
    if coa_previous_resistance <= 0 and raw_resistance > 0:
        coa_previous_resistance = raw_resistance

    # A real SHIFT happens only when the new raw level equals the weak
    # candidate that was already identified on the previous calculation.
    # This prevents ordinary OI/Volume flicker from creating fake history.
    if (
        old_support > 0
        and raw_support > 0
        and abs(raw_support - old_support) > 1e-9
        and coa_pending_support > 0
        and abs(raw_support - coa_pending_support) < 1e-9
    ):
        support_shift = True
        coa_previous_support = raw_support

    if (
        old_resistance > 0
        and raw_resistance > 0
        and abs(raw_resistance - old_resistance) > 1e-9
        and coa_pending_resistance > 0
        and abs(raw_resistance - coa_pending_resistance) < 1e-9
    ):
        resistance_shift = True
        coa_previous_resistance = raw_resistance

    # Save the CURRENT weak candidates for the next tick. This is only a
    # pending target; it is not written as SHIFT until the target becomes the
    # actual selected level.
    coa_pending_support = (
        safe_float(support_weak.get("strike"))
        if str(support_weak.get("status", "")).upper() in ("WTB", "WTT")
        else 0.0
    )
    coa_pending_resistance = (
        safe_float(resistance_weak.get("strike"))
        if str(resistance_weak.get("status", "")).upper() in ("WTB", "WTT")
        else 0.0
    )

    # LIVE status comes directly from the LTP Calculator engine.
    # SHIFT is a history event, not a permanent live status.
    support_status = str(support_weak.get("status") or "STRONG").upper()
    resistance_status = str(resistance_weak.get("status") or "STRONG").upper()
    if support_status not in ("WTB", "WTT", "STRONG"):
        support_status = "STRONG"
    if resistance_status not in ("WTB", "WTT", "STRONG"):
        resistance_status = "STRONG"

    # Keep the accepted/current levels. A completed shift updates the level;
    # a merely weak candidate never changes the level.
    support = coa_previous_support or raw_support
    resistance = coa_previous_resistance or raw_resistance

    # Floating story:
    #   WTB/WTT  => current level -> current weak target
    #   SHIFT    => previous level -> newly accepted level
    #   STRONG   => current level -> same level
    if support > 0:
        if support_shift and old_support > 0:
            coa_story_support = {
                "from": old_support,
                "to": support,
                "status": "SHIFT",
                "source": support_source,
            }
        elif support_status in ("WTB", "WTT") and support_weak.get("strike", 0) > 0:
            coa_story_support = {
                "from": support,
                "to": safe_float(support_weak["strike"]),
                "status": support_status,
                "source": support_weak.get("field", ""),
            }
        else:
            coa_story_support = {
                "from": support,
                "to": support,
                "status": "STRONG",
                "source": support_source,
            }

    if resistance > 0:
        if resistance_shift and old_resistance > 0:
            coa_story_resistance = {
                "from": old_resistance,
                "to": resistance,
                "status": "SHIFT",
                "source": resistance_source,
            }
        elif resistance_status in ("WTB", "WTT") and resistance_weak.get("strike", 0) > 0:
            coa_story_resistance = {
                "from": resistance,
                "to": safe_float(resistance_weak["strike"]),
                "status": resistance_status,
                "source": resistance_weak.get("field", ""),
            }
        else:
            coa_story_resistance = {
                "from": resistance,
                "to": resistance,
                "status": "STRONG",
                "source": resistance_source,
            }

    # Persist the exact morning-to-current story.  CALL = resistance, PUT = support.
    if is_market_open():
        record_shift_history("call", coa_story_resistance)
        record_shift_history("put", coa_story_support)

    analysis = {
        "imaginary_line": {"lower": imag_lo, "upper": imag_hi},
        "support": {
            "level": support, "source": support_source, "status": support_status,
            "weak_strike": support_weak["strike"], "percent": support_weak["percent"],
            "field": support_weak["field"], "shift_from": old_support if support_shift else 0,
            "oi_second_percent": pe_oi_second_pct, "volume_second_percent": pe_vol_second_pct,
        },
        "resistance": {
            "level": resistance, "source": resistance_source, "status": resistance_status,
            "weak_strike": resistance_weak["strike"], "percent": resistance_weak["percent"],
            "field": resistance_weak["field"], "shift_from": old_resistance if resistance_shift else 0,
            "oi_second_percent": ce_oi_second_pct, "volume_second_percent": ce_vol_second_pct,
        },
        "story": {
            "call": dict(coa_story_resistance),
            "put": dict(coa_story_support),
        },
        "history": get_shift_history(),
        "coa": "",
    }

    # The live 9-scenario engine follows the CURRENT LTP Calculator
    # WTB/WTT/STRONG condition. SHIFT remains a history event only.
    ss = str(support_status or "STRONG").upper()
    rs = str(resistance_status or "STRONG").upper()
    coa_map = {
        ("STRONG", "STRONG"): (1, "COA 1.1", "STRONG / STRONG"),
        ("STRONG", "WTB"):    (2, "COA 1.2", "STRONG / WTB"),
        ("STRONG", "WTT"):    (3, "COA 1.3", "STRONG / WTT"),
        ("WTB", "STRONG"):    (4, "COA 1.4", "WTB / STRONG"),
        ("WTT", "STRONG"):    (5, "COA 1.5", "WTT / STRONG"),
        ("WTB", "WTB"):       (6, "COA 1.6", "WTB / WTB"),
        ("WTT", "WTT"):       (7, "COA 1.7", "WTT / WTT"),
        ("WTB", "WTT"):       (8, "COA 1.8", "WTB / WTT"),
        ("WTT", "WTB"):       (9, "COA 1.9", "WTT / WTB"),
    }
    scenario = coa_map.get((ss, rs), (0, "WAIT", f"{ss} / {rs}"))
    analysis["coa"] = scenario[1]
    analysis["scenario"] = {
        "number": scenario[0],
        "name": scenario[1],
        "state": scenario[2],
        "support_status": ss,
        "resistance_status": rs,
        "automatic": True,
    }

    # coa_previous_* already contains the last accepted/completed-shift level.
    # Never overwrite it with a transient live ranking change.
    return analysis


# ============================================================
# ROW BUILDER
# ============================================================

def build_rows():

    strikes = get_visible_strikes()

    rows = []

    ce_oi_values = []
    pe_oi_values = []

    ce_vol_values = []
    pe_vol_values = []

    ce_oi_change_values = []
    pe_oi_change_values = []

    expiry_contracts = (
        get_expiry_contracts()
    )

    contract_lookup = {
        (
            x["strike"],
            x["option_type"]
        ): x
        for x in expiry_contracts
    }

    for strike in strikes:

        ce = contract_lookup.get(
            (
                strike,
                "CE"
            )
        )

        pe = contract_lookup.get(
            (
                strike,
                "PE"
            )
        )

        ce_symbol = (
            ce["symbol"]
            if ce
            else ""
        )

        pe_symbol = (
            pe["symbol"]
            if pe
            else ""
        )

        ce_data = (
            live_data.get(
                ce_symbol,
                create_empty_data()
            )
        )

        pe_data = (
            live_data.get(
                pe_symbol,
                create_empty_data()
            )
        )

        with greeks_lock:

            ce_greek = (
                greeks_data.get(
                    f"{strike:.2f}_CE",
                    {}
                ).copy()
            )

            pe_greek = (
                greeks_data.get(
                    f"{strike:.2f}_PE",
                    {}
                ).copy()
            )

        # RAW DISPLAY RULE: Volume shown in the main chain must come only
        # from the Angel Smart Stream SNAP_QUOTE packet. Never substitute the
        # Greeks REST snapshot's trade_volume here; that is a different
        # snapshot path and can be older than the live tick.
        ce_volume = safe_int(ce_data.get("volume"))
        pe_volume = safe_int(pe_data.get("volume"))

        ce_oi = safe_int(
            ce_data.get("oi")
        )

        pe_oi = safe_int(
            pe_data.get("oi")
        )

        ce_chg = safe_int(
            ce_data.get(
                "oi_change"
            )
        )

        pe_chg = safe_int(
            pe_data.get(
                "oi_change"
            )
        )

        ce_oi_values.append(
            ce_oi
        )

        pe_oi_values.append(
            pe_oi
        )

        ce_vol_values.append(
            ce_volume
        )

        pe_vol_values.append(
            pe_volume
        )

        ce_oi_change_values.append(
            max(
                0,
                ce_chg
            )
        )

        pe_oi_change_values.append(
            max(
                0,
                pe_chg
            )
        )

        rows.append(
            {
                "strike": strike,
                "ce": ce,
                "pe": pe,
                "ce_data": ce_data,
                "pe_data": pe_data,
                "ce_greek": ce_greek,
                "pe_greek": pe_greek,
                "ce_volume": ce_volume,
                "pe_volume": pe_volume,
            }
        )

    # --------------------------------------------------------
    # HIGHEST / SECOND HIGHEST
    # --------------------------------------------------------

    max_ce_oi, second_ce_oi = (
        two_rank_values(
            ce_oi_values
        )
    )

    max_pe_oi, second_pe_oi = (
        two_rank_values(
            pe_oi_values
        )
    )

    max_ce_vol, second_ce_vol = (
        two_rank_values(
            ce_vol_values
        )
    )

    max_pe_vol, second_pe_vol = (
        two_rank_values(
            pe_vol_values
        )
    )

    max_ce_chg, second_ce_chg = (
        two_rank_values(
            ce_oi_change_values
        )
    )

    max_pe_chg, second_pe_chg = (
        two_rank_values(
            pe_oi_change_values
        )
    )

    # --------------------------------------------------------
    # LTP CALCULATOR STYLE SUPPORT / RESISTANCE
    # --------------------------------------------------------
    spot_for_levels = safe_float(nifty_data.get("ltp"))
    if spot_for_levels <= 0:
        spot_for_levels = calculate_atm()
    level_analysis = calculate_ltp_levels(rows, spot_for_levels)
    resistance = safe_float(level_analysis["resistance"]["level"])
    support = safe_float(level_analysis["support"]["level"])

    # --------------------------------------------------------
    # OUTPUT
    # --------------------------------------------------------

    output = []

    for row in rows:

        strike = row[
            "strike"
        ]

        ce = row["ce"]
        pe = row["pe"]

        ce_data = row[
            "ce_data"
        ]

        pe_data = row[
            "pe_data"
        ]

        ce_greek = row[
            "ce_greek"
        ]

        pe_greek = row[
            "pe_greek"
        ]

        ce_volume = row[
            "ce_volume"
        ]

        pe_volume = row[
            "pe_volume"
        ]

        ce_oi = safe_int(
            ce_data.get("oi")
        )

        pe_oi = safe_int(
            pe_data.get("oi")
        )

        ce_chg = safe_int(
            ce_data.get(
                "oi_change"
            )
        )

        pe_chg = safe_int(
            pe_data.get(
                "oi_change"
            )
        )

        ce_oi_rank = rank_status(
            ce_oi,
            max_ce_oi
        )

        ce_oi_rank[
            "class"
        ] = rank_color_class(
            ce_oi,
            max_ce_oi,
            second_ce_oi
        )

        ce_vol_rank = rank_status(
            ce_volume,
            max_ce_vol
        )

        ce_vol_rank[
            "class"
        ] = rank_color_class(
            ce_volume,
            max_ce_vol,
            second_ce_vol
        )

        ce_chg_rank = rank_status(
            max(
                0,
                ce_chg
            ),
            max_ce_chg
        )

        ce_chg_rank[
            "class"
        ] = rank_color_class(
            max(
                0,
                ce_chg
            ),
            max_ce_chg,
            second_ce_chg
        )

        pe_oi_rank = rank_status(
            pe_oi,
            max_pe_oi
        )

        pe_oi_rank[
            "class"
        ] = rank_color_class(
            pe_oi,
            max_pe_oi,
            second_pe_oi
        )

        pe_vol_rank = rank_status(
            pe_volume,
            max_pe_vol
        )

        pe_vol_rank[
            "class"
        ] = rank_color_class(
            pe_volume,
            max_pe_vol,
            second_pe_vol
        )

        pe_chg_rank = rank_status(
            max(
                0,
                pe_chg
            ),
            max_pe_chg
        )

        pe_chg_rank[
            "class"
        ] = rank_color_class(
            max(
                0,
                pe_chg
            ),
            max_pe_chg,
            second_pe_chg
        )

        output.append(
            {
                "strike": strike,

                "ce": {
                    "symbol":
                        (
                            ce["symbol"]
                            if ce
                            else ""
                        ),

                    "token":
                        (
                            ce["token"]
                            if ce
                            else ""
                        ),

                    "ltp":
                        (
                            safe_float(
                                ce_data.get("ltp")
                            )
                        ),

                    "close":
                        safe_float(
                            ce_data.get("close")
                        ),

                    "oi":
                        ce_oi,

                    "oi_change":
                        ce_chg,

                    "volume":
                        ce_volume,

                    "percent_change":
                        safe_float(
                            ce_data.get(
                                "percent_change"
                            )
                        ),

                    "iv":
                        safe_float(
                            ce_greek.get("iv")
                        ),

                    "delta":
                        safe_float(
                            ce_greek.get(
                                "delta"
                            )
                        ),

                    "gamma":
                        safe_float(
                            ce_greek.get(
                                "gamma"
                            )
                        ),

                    "theta":
                        safe_float(
                            ce_greek.get(
                                "theta"
                            )
                        ),

                    "vega":
                        safe_float(
                            ce_greek.get(
                                "vega"
                            )
                        ),

                    "oi_rank":
                        ce_oi_rank,

                    "volume_rank":
                        ce_vol_rank,

                    "oi_change_rank":
                        ce_chg_rank,
                },

                "pe": {
                    "symbol":
                        (
                            pe["symbol"]
                            if pe
                            else ""
                        ),

                    "token":
                        (
                            pe["token"]
                            if pe
                            else ""
                        ),

                    "ltp":
                        (
                            safe_float(
                                pe_data.get("ltp")
                            )
                        ),

                    "close":
                        safe_float(
                            pe_data.get("close")
                        ),

                    "oi":
                        pe_oi,

                    "oi_change":
                        pe_chg,

                    "volume":
                        pe_volume,

                    "percent_change":
                        safe_float(
                            pe_data.get(
                                "percent_change"
                            )
                        ),

                    "iv":
                        safe_float(
                            pe_greek.get("iv")
                        ),

                    "delta":
                        safe_float(
                            pe_greek.get(
                                "delta"
                            )
                        ),

                    "gamma":
                        safe_float(
                            pe_greek.get(
                                "gamma"
                            )
                        ),

                    "theta":
                        safe_float(
                            pe_greek.get(
                                "theta"
                            )
                        ),

                    "vega":
                        safe_float(
                            pe_greek.get(
                                "vega"
                            )
                        ),

                    "oi_rank":
                        pe_oi_rank,

                    "volume_rank":
                        pe_vol_rank,

                    "oi_change_rank":
                        pe_chg_rank,
                }
            }
        )

    return (
        output,
        resistance,
        support,
        level_analysis
    )


# ============================================================
# PAYLOAD
# ============================================================

def make_payload():

    rows, resistance, support, level_analysis = (
        build_rows()
    )

    atm = calculate_atm()

    with data_lock:

        nifty = dict(
            nifty_data
        )

        current_state = dict(
            state
        )

    if (
        not is_market_open()
        and safe_float(nifty.get("ltp")) <= 0
        and safe_float(nifty.get("close")) > 0
    ):
        nifty["ltp"] = safe_float(nifty.get("close"))
        nifty["ltp_source"] = "CLOSE"

    return {
        "broker":
            "Angel One",

        "live":
            True,

        "index":
            "NIFTY 50",

        "nifty_token":
            NIFTY_TOKEN,

        "websocket":
            current_state[
                "websocket"
            ],

        "logged_in":
            current_state[
                "logged_in"
            ],

        "last_update":
            current_state[
                "last_update"
            ],

        "greeks_update":
            current_state[
                "greeks_update"
            ],

        "greeks_count":
            safe_int(current_state.get("greeks_count", 0)),

        "error":
            current_state[
                "last_error"
            ],

        "data_ready":
            safe_float(
                nifty.get("ltp")
            ) > 0,

        "live_tick_active":
            (
                max(
                    safe_float(current_state.get("last_nifty_tick_at", 0)),
                    safe_float(current_state.get("last_option_tick_at", 0))
                ) > 0
                and (
                    time.time() - max(
                        safe_float(current_state.get("last_nifty_tick_at", 0)),
                        safe_float(current_state.get("last_option_tick_at", 0))
                    )
                ) <= 20
            ),

        "tick_count":
            safe_int(current_state.get("tick_count", 0)),

        # Raw market-data synchronization diagnostics.
        "raw_market_data": {
            "source": "ANGEL_ONE_SMART_STREAM_SNAP_QUOTE",
            "option_mode": WS_OPTION_MODE,
            "raw_values_only": True,
            "last_option_tick_at": safe_float(current_state.get("last_option_tick_at", 0)),
            "last_nifty_tick_at": safe_float(current_state.get("last_nifty_tick_at", 0)),
        },

        "market_open":
            is_market_open(),

        "option_data_count":
            sum(
                1
                for item
                in live_data.values()
                if (
                    safe_float(
                        item.get("ltp")
                    ) > 0
                    or
                    safe_int(
                        item.get("oi")
                    ) > 0
                    or
                    safe_int(
                        item.get("volume")
                    ) > 0
                )
            ),

        "option_oi_count":
            sum(
                1
                for item
                in live_data.values()
                if safe_int(
                    item.get("oi")
                ) > 0
            ),

        "option_volume_count":
            sum(
                1
                for item
                in live_data.values()
                if safe_int(
                    item.get("volume")
                ) > 0
            ),

        "nifty":
            nifty,

        "atm":
            atm,

        "expiry":
            selected_expiry,

        "oi_baseline_symbols":
            len(baseline.get("values", {})),

        "expiry_display":
            expiry_display(
                selected_expiry
            ),

        "selected_strike":
            selected_strike,

        "resistance":
            resistance,

        "support":
            support,

        "level_analysis":
            level_analysis,

        "rows":
            rows,
        "raw_ws_messages": safe_int(current_state.get("raw_ws_messages", 0)),
        "last_ws_message_at": safe_float(current_state.get("last_ws_message_at", 0)),
        "last_ws_token": current_state.get("last_ws_token", ""),
        "last_nifty_tick_at": safe_float(current_state.get("last_nifty_tick_at", 0)),
        "last_option_tick_at": safe_float(current_state.get("last_option_tick_at", 0)),
        "ws_reconnects": safe_int(current_state.get("ws_reconnects", 0)),
    }


# ============================================================
# API ROUTES
# ============================================================

@app.get("/api/expiries")
def api_expiries():
    """Expose the current/future NIFTY expiries from Angel One's instrument master."""
    try:
        expiries = get_expiries()
        return jsonify({
            "ok": True,
            "source": "ANGEL_ONE_INSTRUMENT_MASTER",
            "expiries": expiries,
            "selected": selected_expiry,
            "display": [expiry_display(x) for x in expiries],
        })
    except Exception as e:
        print("/api/expiries error:", repr(e), flush=True)
        return jsonify({"ok": False, "error": str(e), "expiries": [], "selected": selected_expiry}), 500


@app.post("/api/set_expiry")
def api_set_expiry():
    """Switch the whole option chain to an expiry that exists in Angel One's master."""
    global selected_expiry, baseline, greeks_data, shift_history, last_subscription_atm
    try:
        body = request.get_json(silent=True) or {}
        requested = format_expiry(body.get("expiry"))
        expiries = get_expiries()
        if not requested or requested not in expiries:
            return jsonify({
                "ok": False,
                "error": "Expiry is not available in Angel One instrument master.",
                "expiries": expiries,
                "selected": selected_expiry,
            }), 400

        if requested == selected_expiry:
            return jsonify({"ok": True, "selected": selected_expiry, "changed": False})

        selected_expiry = requested
        last_subscription_atm = None

        # Clear expiry-specific calculations so no values from the previous
        # contract series can leak into the newly selected option chain.
        with baseline_lock:
            baseline = {"date": "", "expiry": selected_expiry, "source": "", "values": {}}
        with greeks_lock:
            greeks_data = {}
        with shift_history_lock:
            shift_history = {"date": today_string(), "expiry": selected_expiry, "call": [], "put": []}

        ensure_visible_chain_symbols()
        if active_sws is not None:
            subscribe_live_tokens(active_sws)

        print("USER SELECTED ANGEL ONE EXPIRY:", selected_expiry, flush=True)
        return jsonify({"ok": True, "selected": selected_expiry, "display": expiry_display(selected_expiry), "changed": True})
    except Exception as e:
        state["last_error"] = "Expiry switch error: " + str(e)
        print("/api/set_expiry error:", repr(e), flush=True)
        return jsonify({"ok": False, "error": str(e), "selected": selected_expiry}), 500


@app.get("/api/data")
def api_data():

    try:

        payload = make_payload()

        payload[
            "server_ok"
        ] = True

        return jsonify(
            payload
        )

    except Exception as e:

        state[
            "last_error"
        ] = (
            "API /api/data error: "
            + str(e)
        )

        print(
            "/api/data error:",
            e
        )

        return jsonify(
            {
                "server_ok":
                    False,

                "error":
                    str(e),

                "websocket":
                    state.get(
                        "websocket",
                        "DISCONNECTED"
                    ),

                "logged_in":
                    state.get(
                        "logged_in",
                        False
                    ),

                "nifty":
                    dict(nifty_data),

                "rows":
                    [],
            }
        ), 500



# ============================================================
# LIVE ANGEL ONE CANDLE DATA (DIRECT OHLCV)
# ============================================================
CANDLE_DEFAULT_INTERVAL = os.getenv("CANDLE_DEFAULT_INTERVAL", "FIVE_MINUTE")
CANDLE_LOOKBACK_MINUTES = int(os.getenv("CANDLE_LOOKBACK_MINUTES", "450"))
CANDLE_INTERVALS = {
    "ONE_MINUTE": 1, "THREE_MINUTE": 3, "FIVE_MINUTE": 5,
    "TEN_MINUTE": 10, "FIFTEEN_MINUTE": 15, "THIRTY_MINUTE": 30,
    "ONE_HOUR": 60,
}

# Full-session Angel candle caches.  These are intentionally kept separate:
# one cache stores each successful historical request chunk, while the second
# stores the merged session shown to the browser.  A temporary Angel API
# response/rate-limit must never erase already-received morning candles.
_CANDLE_CHUNK_CACHE = {}
_CANDLE_CHUNK_CACHE_LOCK = threading.RLock()
_CANDLE_HISTORY_CACHE = {}
_CANDLE_HISTORY_LOCK = threading.RLock()
_CANDLE_CHUNK_CACHE_MAX_DAYS = 3

# Historical REST request guard.  One attempt per market-day/interval prevents
# browser refreshes and client retries from hammering Angel One.
_CANDLE_REQUEST_STATE = {}
_CANDLE_REQUEST_STATE_LOCK = threading.RLock()

def _candle_num(v, default=0.0):
    try: return float(v)
    except Exception: return default

def _candle_rows(raw):
    out=[]
    data = raw.get("data") if isinstance(raw, dict) else raw
    if not isinstance(data, list): return out
    for x in data:
        if not isinstance(x, (list, tuple)) or len(x) < 5: continue
        out.append({
            "time": str(x[0]),
            "open": _candle_num(x[1]), "high": _candle_num(x[2]),
            "low": _candle_num(x[3]), "close": _candle_num(x[4]),
            "volume": _candle_num(x[5]) if len(x) > 5 else 0.0,
        })
    return out

def get_live_candles(interval=CANDLE_DEFAULT_INTERVAL, minutes=None):
    """Return today's Angel One native historical candles without hammering REST.

    Important design rules:
      * Native Angel FIVE_MINUTE candles are the source of truth for the 5M chart.
      * A successful historical request is made at most once per session/interval.
      * A rate-limit/empty/error response is NEVER followed by ONE_MINUTE fallback
        requests.  That fallback was causing repeated Angel access-rate failures.
      * Successful data is kept in process memory and is reused on browser refresh.
      * The current forming candle is maintained from the Angel WebSocket tick.
      * On weekends / outside the NSE session, no historical request is made.
    """
    global obj
    interval = interval if interval in CANDLE_INTERVALS else CANDLE_DEFAULT_INTERVAL
    now = datetime.now(IST)
    today = now.strftime("%Y-%m-%d")
    session_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    session_end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    cache_key = (today, interval)

    # Before the NSE open, do not ask Angel for today's session candles.
    # Reuse anything already cached for today, otherwise return an empty result.
    if now < session_start or now.weekday() >= 5:
        with _CANDLE_HISTORY_LOCK:
            cached = list(_CANDLE_HISTORY_CACHE.get(cache_key, []))
        return cached, ("MARKET CLOSED - USING CACHED CANDLES" if cached else "MARKET CLOSED")

    # After the close, do not repeatedly hit the historical endpoint either.
    # If today's data is already cached, simply serve it.
    with _CANDLE_HISTORY_LOCK:
        cached = list(_CANDLE_HISTORY_CACHE.get(cache_key, []))

    # One REST historical load per interval per market day.  This prevents a
    # browser refresh / repeated /api/candles call from creating a rate-limit loop.
    request_state = _CANDLE_REQUEST_STATE.get(cache_key)
    if request_state and request_state.get("attempted"):
        if cached:
            return cached, request_state.get("error", "")
        return [], request_state.get("error", "ANGEL CANDLE REQUEST ALREADY ATTEMPTED")

    # Mark the request as attempted BEFORE calling Angel.  If Angel returns a
    # rate-limit error, later browser calls must not immediately retry it.
    with _CANDLE_REQUEST_STATE_LOCK:
        state = _CANDLE_REQUEST_STATE.get(cache_key)
        if state and state.get("attempted"):
            if cached:
                return cached, state.get("error", "")
            return [], state.get("error", "ANGEL CANDLE REQUEST ALREADY ATTEMPTED")
        _CANDLE_REQUEST_STATE[cache_key] = {
            "attempted": True,
            "at": time.time(),
            "error": "ANGEL CANDLE HISTORICAL REQUEST PENDING",
        }

    if obj is None and not angel_login():
        err = "ANGEL LOGIN FAILED"
        with _CANDLE_REQUEST_STATE_LOCK:
            _CANDLE_REQUEST_STATE[cache_key]["error"] = err
        return cached, err

    # FIVE_MINUTE is intentionally native.  For other intervals we also use
    # one native request; no chunk/fallback loop is used here.
    from_dt = session_start
    to_dt = min(now, session_end)
    params = {
        "exchange": "NSE",
        "symboltoken": NIFTY_TOKEN,
        "interval": interval,
        "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
        "todate": to_dt.strftime("%Y-%m-%d %H:%M"),
    }

    try:
        raw = obj.getCandleData(params)
        candles = _candle_rows(raw)
        print(
            f"ANGEL CANDLE {interval} SESSION: {len(candles)} "
            f"{params['fromdate']}->{params['todate']}",
            flush=True,
        )

        normalized = []
        for r in candles:
            try:
                raw_t = str(r.get("time", ""))
                dt = datetime.fromisoformat(raw_t.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=IST)
                dt = dt.astimezone(IST)
                if dt.date() != now.date() or dt < session_start:
                    continue
                if dt > to_dt + timedelta(minutes=1):
                    continue
                x = dict(r)
                x["time"] = dt.strftime("%Y-%m-%d %H:%M")
                normalized.append(x)
            except Exception:
                continue

        normalized.sort(key=lambda x: str(x.get("time", "")))
        dedup = {}
        for c in normalized:
            dedup[str(c.get("time", ""))[:16]] = c
        normalized = list(dedup.values())

        if normalized:
            with _CANDLE_HISTORY_LOCK:
                _CANDLE_HISTORY_CACHE[cache_key] = list(normalized)
            with _CANDLE_REQUEST_STATE_LOCK:
                _CANDLE_REQUEST_STATE[cache_key]["error"] = ""
            return normalized, ""

        # Empty is a terminal result for this session load.  Do NOT start a
        # ONE_MINUTE fallback: that is exactly what can multiply API calls.
        err = "ANGEL RETURNED 0 NATIVE CANDLE ROWS"
        with _CANDLE_REQUEST_STATE_LOCK:
            _CANDLE_REQUEST_STATE[cache_key]["error"] = err
        return cached, err

    except Exception as e:
        err = f"ANGEL CANDLE REQUEST ERROR: {e!r}"
        print(err, flush=True)
        with _CANDLE_REQUEST_STATE_LOCK:
            _CANDLE_REQUEST_STATE[cache_key]["error"] = err
        return cached, err


@app.get("/api/candles")
def api_candles():
    """Return today's Angel historical candles once per explicit chart load.

    This endpoint is NOT polled by a timer. The browser calls it when the
    candle chart is opened/interval is changed; live updates thereafter come
    from the Angel WebSocket tick stream.
    """
    try:
        interval = str(request.args.get("interval") or CANDLE_DEFAULT_INTERVAL).upper()
        if interval not in CANDLE_INTERVALS:
            interval = CANDLE_DEFAULT_INTERVAL
        candles, error = get_live_candles(interval=interval)
        if error and not candles:
            return jsonify({
                "ok": False, "candles": [], "error": error,
                "exchange": "NSE", "symbol": "NIFTY", "interval": interval
            }), 200

        last = candles[-1] if candles else {}
        now_local=datetime.now(IST)
        session_start_local=now_local.replace(hour=9, minute=15, second=0, microsecond=0)
        minutes_per = CANDLE_INTERVALS.get(interval, 5)
        expected_count=(max(0,int((now_local-session_start_local).total_seconds()//(minutes_per*60)))+1) if now_local>=session_start_local and now_local.weekday()<5 else 0
        return jsonify({
            "ok": True,
            "candles": candles,
            "last_candle": last,
            "count": len(candles),
            "expected_count": expected_count,
            "exchange": "NSE",
            "symbol": "NIFTY",
            "interval": interval,
            "coa": "WAIT",
            "scenario": {"number": "", "name": "WAIT"},
        })
    except Exception as e:
        print("/api/candles error:", repr(e), flush=True)
        return jsonify({
            "ok": False, "candles": [], "error": str(e),
            "exchange": "NSE", "symbol": "NIFTY"
        }), 500


# ============================================================
# TRADING
# ============================================================

@app.post("/api/place_order")
def api_place_order():

    if not ENABLE_TRADING:

        return jsonify(
            {
                "ok": False,
                "error":
                    "Trading is disabled. "
                    "Set ENABLE_TRADING=true "
                    "only when intentionally "
                    "using live order execution."
            }
        ), 403

    body = (
        request.get_json(
            silent=True
        )
        or {}
    )

    symbol = str(
        body.get(
            "symbol",
            ""
        )
    ).strip()

    token = str(
        body.get(
            "token",
            ""
        )
    ).strip()

    transaction_type = str(
        body.get(
            "transaction_type",
            ""
        )
    ).upper()

    exchange = str(
        body.get(
            "exchange",
            "NFO"
        )
    ).upper()

    quantity = safe_int(
        body.get(
            "quantity",
            NIFTY_LOT_SIZE
        )
    )

    if (
        not symbol
        or not token
    ):

        return jsonify(
            {
                "ok": False,
                "error":
                    "Symbol/token required."
            }
        ), 400

    if transaction_type not in (
        "BUY",
        "SELL"
    ):

        return jsonify(
            {
                "ok": False,
                "error":
                    "Transaction must be BUY or SELL."
            }
        ), 400

    if quantity <= 0:

        return jsonify(
            {
                "ok": False,
                "error":
                    "Invalid quantity."
            }
        ), 400

    if not obj:

        return jsonify(
            {
                "ok": False,
                "error":
                    "Angel One not connected."
            }
        ), 503

    try:

        order_params = {
            "variety":
                "NORMAL",

            "tradingsymbol":
                symbol,

            "symboltoken":
                token,

            "transactiontype":
                transaction_type,

            "exchange":
                exchange,

            "ordertype":
                "MARKET",

            "producttype":
                "INTRADAY",

            "duration":
                "DAY",

            "price":
                "0",

            "squareoff":
                "0",

            "stoploss":
                "0",

            "quantity":
                str(quantity),
        }

        response = obj.placeOrder(
            order_params
        )

        return jsonify(
            {
                "ok": True,
                "response":
                    response
            }
        )

    except Exception as e:

        return jsonify(
            {
                "ok": False,
                "error":
                    str(e)
            }
        ), 500


# ============================================================
# POSITIONS
# ============================================================

def angel_sdk_call_with_relogin(method_name, *args, **kwargs):
    """Call an authenticated Angel SDK method and refresh an expired session once.

    Render can keep a process alive longer than an Angel JWT.  SDK methods such
    as position() may return AG8001 in a JSON response instead of raising, so
    inspect both the response and exceptions before deciding the call succeeded.
    """
    global obj
    method = getattr(obj, method_name, None) if obj is not None else None
    if not callable(method):
        if not angel_login():
            raise RuntimeError(state.get("last_error") or "Angel One login failed")
        method = getattr(obj, method_name, None)
    if not callable(method):
        raise RuntimeError("Angel One SDK method unavailable: " + str(method_name))

    try:
        result = method(*args, **kwargs)
        if _greeks_auth_error(result):
            raise RuntimeError("AG8001: Invalid Token")
        return result
    except Exception as exc:
        message = str(exc).lower()
        auth_failed = any(code in message for code in ("ag8001", "ab1010", "ab1011", "invalid token", "token expired"))
        if not auth_failed:
            raise
        print("ANGEL SDK: expired JWT detected in", method_name, "-> refreshing session", flush=True)
        if not angel_login():
            raise RuntimeError(state.get("last_error") or "Angel One re-login failed") from exc
        method = getattr(obj, method_name, None)
        if not callable(method):
            raise RuntimeError("Angel One SDK method unavailable after re-login: " + str(method_name))
        result = method(*args, **kwargs)
        if _greeks_auth_error(result):
            raise RuntimeError("Angel One still reports AG8001 after fresh login")
        return result


@app.get("/api/positions")
def api_positions():

    if not obj:

        return jsonify(
            {
                "ok": False,
                "positions": [],
                "error":
                    "Angel One not connected."
            }
        )

    try:

        response = angel_sdk_call_with_relogin("position")

        if not response:

            return jsonify(
                {
                    "ok": True,
                    "positions": []
                }
            )

        if not response.get(
            "status"
        ):

            return jsonify(
                {
                    "ok": False,
                    "positions": [],
                    "error":
                        response.get(
                            "message",
                            "Position API error"
                        )
                }
            )

        rows = (
            response.get("data")
            or []
        )

        output = []

        for row in rows:

            net_qty = safe_int(
                row.get("netqty")
            )

            buy_avg = safe_float(
                row.get("buyavgprice")
            )

            sell_avg = safe_float(
                row.get("sellavgprice")
            )

            ltp = safe_float(
                row.get("ltp")
            )

            if net_qty > 0:

                pnl = (
                    ltp
                    - buy_avg
                ) * net_qty

            elif net_qty < 0:

                pnl = (
                    sell_avg
                    - ltp
                ) * abs(net_qty)

            else:

                pnl = safe_float(
                    row.get("pnl")
                )

            output.append(
                {
                    "symbol":
                        row.get(
                            "tradingsymbol",
                            ""
                        ),

                    "token":
                        row.get(
                            "symboltoken",
                            ""
                        ),

                    "exchange":
                        row.get(
                            "exchange",
                            ""
                        ),

                    "net_qty":
                        net_qty,

                    "buy_avg":
                        buy_avg,

                    "sell_avg":
                        sell_avg,

                    "ltp":
                        ltp,

                    "pnl":
                        round(
                            pnl,
                            2
                        ),

                    "product":
                        row.get(
                            "producttype",
                            ""
                        ),
                }
            )

        return jsonify(
            {
                "ok": True,
                "positions":
                    output
            }
        )

    except Exception as e:

        return jsonify(
            {
                "ok": False,
                "positions": [],
                "error":
                    str(e)
            }
        ), 500


# ============================================================
# SQUARE OFF
# ============================================================

@app.post("/api/square_off")
def api_square_off():

    if not ENABLE_TRADING:

        return jsonify(
            {
                "ok": False,
                "error":
                    "Trading is disabled."
            }
        ), 403

    body = (
        request.get_json(
            silent=True
        )
        or {}
    )

    symbol = str(
        body.get(
            "symbol",
            ""
        )
    ).strip()

    token = str(
        body.get(
            "token",
            ""
        )
    ).strip()

    exchange = str(
        body.get(
            "exchange",
            "NFO"
        )
    ).upper()

    quantity = safe_int(
        body.get(
            "quantity"
        )
    )

    if (
        not symbol
        or not token
        or quantity <= 0
    ):

        return jsonify(
            {
                "ok": False,
                "error":
                    "Invalid square-off request."
            }
        ), 400

    if not obj:

        return jsonify(
            {
                "ok": False,
                "error":
                    "Angel One not connected."
            }
        ), 503

    try:

        positions = angel_sdk_call_with_relogin("position")

        rows = (
            positions.get("data")
            or []
            if positions
            else []
        )

        net_qty = 0

        for row in rows:

            if (
                str(
                    row.get(
                        "symboltoken",
                        ""
                    )
                )
                == token
            ):

                net_qty = safe_int(
                    row.get("netqty")
                )

                break

        if net_qty == 0:

            return jsonify(
                {
                    "ok": False,
                    "error":
                        "No open position found."
                }
            ), 400

        transaction_type = (
            "SELL"
            if net_qty > 0
            else "BUY"
        )

        quantity = min(
            quantity,
            abs(net_qty)
        )

        order_params = {
            "variety":
                "NORMAL",

            "tradingsymbol":
                symbol,

            "symboltoken":
                token,

            "transactiontype":
                transaction_type,

            "exchange":
                exchange,

            "ordertype":
                "MARKET",

            "producttype":
                "INTRADAY",

            "duration":
                "DAY",

            "price":
                "0",

            "squareoff":
                "0",

            "stoploss":
                "0",

            "quantity":
                str(quantity),
        }

        response = obj.placeOrder(
            order_params
        )

        return jsonify(
            {
                "ok": True,
                "response":
                    response
            }
        )

    except Exception as e:

        return jsonify(
            {
                "ok": False,
                "error":
                    str(e)
            }
        ), 500


# ============================================================
# HTML UI
# ============================================================

HTML = r"""
<!DOCTYPE html>

<html lang="en">

<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width,initial-scale=1.0"
>

<title>
NIFTY 50 LIVE - ANGEL ONE
</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #0b0f14;
    color: #f2f5f8;
    font-family:
        Arial,
        Helvetica,
        sans-serif;
}

.topbar {

    position: sticky;
    top: 0;
    z-index: 20;

    display: flex;
    align-items: center;
    gap: 10px;

    padding: 10px;

    background: #111820;

    border-bottom:
        1px solid #26313c;
}

.logo {

    font-weight: 900;
    font-size: 18px;
}

.live {

    color: #00e676;
    font-weight: 800;
}

.expiry {

    margin-left: auto;

    background: #151e27;

    color: white;

    border:
        1px solid #364351;

    border-radius: 7px;

    padding: 7px;
}

.refresh {

    background: #ff9800;

    color: #111;

    border: 0;

    border-radius: 7px;

    padding:
        8px 12px;

    font-weight: 800;
}

.info {

    display: flex;

    align-items: center;

    gap: 10px;

    padding: 9px;

    overflow-x: auto;

    background: #0e141b;

    border-bottom:
        1px solid #202a34;
}

.badge {

    padding:
        6px 10px;

    border-radius: 6px;

    background: #b71c1c;

    color: white;

    font-weight: 800;

    white-space: nowrap;
}

.spot {

    padding:
        6px 10px;

    border:
        1px solid #394552;

    border-radius: 6px;

    background: #151d26;

    white-space: nowrap;
}

.status {

    font-size: 12px;

    opacity: .8;
}

.error {

    color: #ff5252;

    font-size: 12px;
}

.wrap {

    width: 100%;

    overflow-x: auto;
}

table {

    width: 100%;

    min-width: 1500px;

    border-collapse:
        collapse;

    font-size: 11px;
}

th {

    position: sticky;

    top: 0;

    z-index: 10;

    background: #17212b;

    border:
        1px solid #27333f;

    padding:
        7px 4px;

    white-space: nowrap;
}

td {

    border:
        1px solid #202a34;

    padding: 4px;

    text-align: center;

    white-space: nowrap;
}

/* =========================================================
   HEADER COLORS
   ========================================================= */

.call-head {

    background:
        #087b42 !important;

    color: white !important;

    font-weight: 900;
}

.put-head {

    background:
        #a91f2c !important;

    color: white !important;

    font-weight: 900;
}

.strike-head {

    background:
        #ff8a00 !important;

    color: #111 !important;

    font-weight: 900;
}


/* =========================================================
   STRIKE
   ========================================================= */

.strike {

    background:
        #ff8a00 !important;

    color: #111 !important;

    font-size: 14px;

    font-weight: 900;
}

.atm {

    box-shadow:
        inset 0 0 0 2px #ffd740;

    font-weight: 900;
}

.resistance {

    box-shadow:
        inset 0 0 0 2px #ff5252;
}

.support {

    box-shadow:
        inset 0 0 0 2px #00e676;
}


/* =========================================================
   HIGHEST / SECOND-HIGHEST + COMPACT PERCENTAGE
   ========================================================= */

.rank-highest {
    background: #55d9ff !important;
    color: #06131a !important;
    font-weight: 900 !important;
}

.rank-second {
    background: #ffe14d !important;
    color: #171200 !important;
    font-weight: 900 !important;
}

/* Percentage itself stays text-only even when the parent is ranked. */
.rank-percent.rank-highest,
.rank-percent.rank-second {
    background: transparent !important;
}

/* High-contrast rank text: the box and its number are deliberately
   different enough to remain readable on mobile.  This also overrides
   the normal +/- OI-change colors when a value is ranked. */
.rank-highest.metric-number,
.rank-highest.rank-percent,
.rank-highest.metric-number.green,
.rank-highest.metric-number.red,
.rank-highest.metric-number.muted {
    color: #06131a !important;
    text-shadow: 0 1px 0 rgba(255,255,255,.35);
}

.rank-second.metric-number,
.rank-second.rank-percent,
.rank-second.metric-number.green,
.rank-second.metric-number.red,
.rank-second.metric-number.muted {
    color: #3b2500 !important;
    text-shadow: 0 1px 0 rgba(255,255,255,.45);
}

.rank-percent {
    display: inline;
    padding: 0;
    margin: 0;
    min-width: 0;
    border: 0;
    border-radius: 0;
    background: transparent !important;
    font-size: 18px !important;
    line-height: 1.05;
    font-weight: 800;
    white-space: nowrap;
    letter-spacing: 0;
}

/* Old percent-box class is intentionally boxless. */
.box {
    display: inline;
    padding: 0;
    margin: 0;
    min-width: 0;
    border: 0;
    border-radius: 0;
    background: transparent !important;
    font-size: 8px;
    line-height: 1;
    font-weight: 700;
}

/* =========================================================
   NUMBER COLORS
   ========================================================= */

.num {
    font-variant-numeric: tabular-nums;
}

.green {
    color: #00e676;
}

.red {
    color: #ff5252;
}

.yellow {
    color: #ffd740;
}

.orange {
    color: #ff9800;
}

.muted {
    color: #8793a0;
}


/* =========================================================
   SECTIONS
   ========================================================= */

.section {

    padding:
        10px;

    border-top:
        1px solid #202a34;
}

.section-title {

    font-size:
        14px;

    font-weight:
        900;

    margin-bottom:
        7px;
}

.positions {

    width:
        100%;

    overflow-x:
        auto;
}


/* =========================================================
   BUTTONS
   ========================================================= */

button {

    cursor:
        pointer;
}

.buy {

    background:
        #00c853;

    color:
        white;

    border: 0;

    border-radius:
        4px;

    padding:
        4px 7px;

    font-weight:
        800;
}

.sell {

    background:
        #d50000;

    color:
        white;

    border: 0;

    border-radius:
        4px;

    padding:
        4px 7px;

    font-weight:
        800;
}

.greeks-live {

    color:
        #00e676;

    font-size:
        10px;

    font-weight:
        800;
}


/* ==========================================================
   BIG / CLEAR MOBILE DASHBOARD OVERRIDES
   ========================================================== */
body { font-size: 18px !important; }
.topbar, .header, .control-bar { min-height: 58px !important; }
button, select, input { font-size: 18px !important; min-height: 44px !important; }
table { font-size: 18px !important; min-width: 1250px !important; }
th { font-size: 19px !important; padding: 12px 10px !important; white-space: nowrap !important; }
td { font-size: 18px !important; padding: 11px 9px !important; min-width: 78px !important; height: 48px !important; }
.strike, .strike-cell { font-size: 21px !important; font-weight: 900 !important; }
.ltp, .ltp-cell { font-size: 20px !important; font-weight: 900 !important; }
.atm, .atm-cell { font-size: 21px !important; font-weight: 900 !important; }
.greeks-live { font-size: 14px !important; }
#spot, #atm, #support, #resistance, #ws, #greeks, #diagnostic, #status { font-size: 18px !important; font-weight: 800 !important; }
.card, .box, .story-box, .panel { border-radius: 10px !important; }


/* Live Candle + Pattern panel */
.candle-panel{margin:8px 8px 10px;padding:8px;background:#0e151d;border:1px solid #2a3948;border-radius:12px;color:#fff;box-sizing:border-box;overflow:hidden}
.expiry-bottom-label{font-size:8px!important;font-weight:900;opacity:.7!important;margin:0!important}.candle-expiry{background:#17222e;color:#fff;border:1px solid #40566a;border-radius:7px;padding:5px 7px;font-weight:800;font-size:10px;max-width:118px}
.candle-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:6px}.candle-head b{font-size:14px}.candle-head span{font-size:9px;opacity:.7;margin-left:7px}.candle-head select{background:#17222e;color:#fff;border:1px solid #40566a;border-radius:7px;padding:5px 7px;font-weight:700}.candle-controls{display:flex;align-items:center;gap:7px}.candle-switch{position:relative;width:48px;height:26px;display:inline-block;flex:0 0 auto}.candle-switch input{opacity:0;width:0;height:0}.candle-slider{position:absolute;inset:0;cursor:pointer;background:#394653;border-radius:999px;transition:.18s;border:1px solid #536575}.candle-slider:before{content:"";position:absolute;width:20px;height:20px;left:2px;top:2px;background:#fff;border-radius:50%;transition:.18s;box-shadow:0 1px 3px rgba(0,0,0,.35)}.candle-switch input:checked + .candle-slider{background:#16a765}.candle-switch input:checked + .candle-slider:before{transform:translateX(22px)}.candle-switch-label{font-size:10px!important;font-weight:800;opacity:.95!important;margin:0!important;min-width:24px;text-align:center}
.candle-summary{display:grid;grid-template-columns:1.5fr .8fr .7fr 1.2fr;gap:5px;margin-bottom:7px}.candle-summary>div{background:#151f29;border-radius:7px;padding:5px 6px;min-width:0}.candle-summary span{display:block;font-size:8px;opacity:.65}.candle-summary strong{display:block;font-size:11px;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.candle-summary .bull{color:#39d98a}.candle-summary .bear{color:#ff5b68}.candle-summary .neutral{color:#ffd166}
#candleCanvas{display:block!important;width:100%!important;height:250px!important;min-height:250px;background:#0a1016!important;border-radius:8px;overflow:hidden;position:relative}.candle-meta{font-size:9px;opacity:.72;padding:5px 2px 0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
@media(max-width:700px){.candle-panel{margin:6px 5px 8px}.candle-summary{grid-template-columns:1.45fr .75fr .65fr 1.15fr}.candle-summary strong{font-size:10px}.candle-expiry{max-width:104px;padding:5px 5px;font-size:9px}.expiry-bottom-label{font-size:7px!important}#candleCanvas{height:250px;min-height:250px}}

</style>

<style>
/* Tick-by-tick visual confirmation for live option columns. */
.tick-flash{animation:optionTickFlash .32s ease-out;}
@keyframes optionTickFlash{0%{filter:brightness(1.9)}100%{filter:brightness(1)}}

/* Compact LTP Calculator-style COA indicator: no large scenario banner/grid. */
.coa-badge{display:flex;align-items:center;justify-content:center;gap:7px;min-width:310px;height:52px;padding:5px 14px;border:2px solid #ff9800;border-radius:7px;background:#171d24;color:#fff;box-sizing:border-box;white-space:nowrap;box-shadow:0 2px 6px rgba(0,0,0,.28);}
.coa-badge .coa-mini{font-size:11px;font-weight:1000;letter-spacing:.5px;opacity:.82;}
.coa-badge #coaNumber{font-size:24px;font-weight:1000;line-height:1;color:#ff9800;}
.coa-badge #coaState{font-size:10px;font-weight:1000;line-height:1.08;text-transform:uppercase;opacity:.98;max-width:230px;overflow:hidden;text-overflow:ellipsis;}
.coa-badge.bull{border-color:#00e676;background:#092318;}
.coa-badge.bull #coaNumber{color:#00e676;}
.coa-badge.bear{border-color:#ff3030;background:#241013;}
.coa-badge.bear #coaNumber{color:#ff5050;}
.coa-badge.wait{border-color:#65717e;background:#171d24;}
.coa-badge.wait #coaNumber{color:#b8c2cc;}
@media(max-width:700px){.coa-badge{min-width:285px;height:48px;padding:4px 10px;gap:7px}.coa-badge #coaNumber{font-size:22px}.coa-badge #coaState{font-size:10px;max-width:210px}.coa-badge .coa-mini{font-size:10px;}}
/* FINAL SCENARIO ENGINE COLOR FIX: class changes must override the fixed base theme. */
.coa-badge.bull{background:#092318 !important;border-color:#00e676 !important;color:#eafff3 !important;}
.coa-badge.bull #coaNumber{color:#00e676 !important;}
.coa-badge.bear{background:#241013 !important;border-color:#ff3030 !important;color:#fff0f0 !important;}
.coa-badge.bear #coaNumber{color:#ff5050 !important;}
.coa-badge.neutral{background:#171d24 !important;border-color:#ff9800 !important;color:#ffffff !important;}
.coa-badge.neutral #coaNumber{color:#ff9800 !important;}
.coa-badge.wait{background:#171d24 !important;border-color:#65717e !important;color:#d7e0e8 !important;}
.coa-badge.wait #coaNumber{color:#b8c2cc !important;}
body.light-mode .coa-badge.bull{background:#e8fff2 !important;border-color:#00a85a !important;color:#075b31 !important;}
body.light-mode .coa-badge.bull #coaNumber{color:#008a45 !important;}
body.light-mode .coa-badge.bear{background:#fff0f1 !important;border-color:#d00000 !important;color:#8f0000 !important;}
body.light-mode .coa-badge.bear #coaNumber{color:#d00000 !important;}
body.light-mode .coa-badge.neutral{background:#fff7e8 !important;border-color:#ff9800 !important;color:#6b4300 !important;}
body.light-mode .coa-badge.neutral #coaNumber{color:#e47700 !important;}
body.light-mode .coa-badge.wait{background:#eef2f5 !important;border-color:#7b8792 !important;color:#46515c !important;}
body.light-mode .coa-badge.wait #coaNumber{color:#5b6772 !important;}


.level-story-row{display:flex;gap:8px;padding:6px 8px 4px;background:#0f141a;box-sizing:border-box;}
.level-story-box{flex:1;min-width:0;border:3px solid #ff9800;background:#121212;border-radius:8px;padding:6px 7px;text-align:center;box-sizing:border-box;overflow:hidden;white-space:nowrap;color:#fff;cursor:pointer;appearance:none;-webkit-appearance:none;box-shadow:0 2px 7px rgba(0,0,0,.35);}
.level-story-box:active{transform:scale(.99)}
.level-story-box.stable{border-color:#ff9800;}
.level-story-box.wtb{border-color:#ff3030;background:#241013;}
.level-story-box.wtt{border-color:#00e676;background:#092318;}
.level-story-box.shift-up{border-color:#00e676;background:#092318;}
.level-story-box.shift-down{border-color:#ff3030;background:#241013;}
.level-story-box .story-title{display:block;font-size:13px;font-weight:1000;line-height:1;margin-bottom:3px;}
.level-story-box .story-text{display:block;font-size:14px;font-weight:900;line-height:1.1;color:#fff;overflow:hidden;text-overflow:ellipsis;}
.level-story-box .story-hint{display:block;font-size:8px;font-weight:800;opacity:.72;margin-top:3px;}
.shift-history-modal{display:none;position:fixed;inset:0;background:rgba(0,0,0,.72);z-index:10000;align-items:center;justify-content:center;padding:14px;box-sizing:border-box;}
.shift-history-modal.open{display:flex;}
.shift-history-card{width:min(680px,96vw);max-height:82vh;overflow:hidden;background:#10161d;border:2px solid #ff9800;border-radius:12px;box-shadow:0 10px 35px rgba(0,0,0,.65);color:#fff;}
.shift-history-head{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;border-bottom:1px solid #293542;font-size:16px;}
.shift-close{border:0;background:#222c36;color:#fff;border-radius:7px;font-size:25px;width:40px;height:40px;line-height:1;cursor:pointer;}
.shift-history-meta{padding:7px 12px;font-size:11px;opacity:.8;border-bottom:1px solid #293542;}
.shift-history-list{padding:8px 10px;overflow:auto;max-height:65vh;}
.shift-event{display:grid;grid-template-columns:64px 1fr auto;gap:8px;align-items:center;padding:9px 8px;margin-bottom:6px;border:2px solid #ff9800;border-radius:7px;background:#171d24;}
.shift-event.wtb{border-color:#ff3030;}
.shift-event.wtt,.shift-event.shift-up{border-color:#00e676;}
.shift-event.shift-down{border-color:#ff3030;}
.shift-event .ev-time{font-weight:900;font-size:12px;}
.shift-event .ev-main{font-weight:900;font-size:13px;}
.shift-event .ev-sub{font-size:10px;opacity:.78;margin-top:2px;}
.shift-event .ev-dir{font-size:11px;font-weight:1000;text-align:right;}
.shift-empty{padding:25px 10px;text-align:center;color:#ff9800;font-weight:900;}
@media(max-width:700px){.level-story-row{gap:5px;padding:5px}.level-story-box{padding:6px 4px}.level-story-box .story-title{font-size:11px}.level-story-box .story-text{font-size:12px}.level-story-box .story-hint{font-size:7px}.shift-history-card{max-height:88vh}.shift-event{grid-template-columns:54px 1fr auto;gap:5px;padding:8px 5px}}

/* V24 FIX: anchor the floating chambers to the option-chain wrapper itself.
   Without this, position:absolute uses the page/body as its containing block
   and the chambers jump to the top. */
.wrap {
  position: relative !important;
}

/* V20 OVERLAY FIX: chambers float over the existing option-chain without consuming a row/gap */
.option-chain-wrapper, .chain-container, .table-wrap, .option-chain-container { position: relative !important; }
.floating-chamber, .floating-chamber-left, .floating-chamber-right, .call-chamber, .put-chamber, .price-marker {
  position: absolute !important;
  margin: 0 !important;
  z-index: 50 !important;
  pointer-events: none;
  box-sizing: border-box;
}
.floating-chambers, .chamber-row, .floating-row {
  position: absolute !important;
  left: 0 !important;
  top: 0 !important;
  right: auto !important;
  height: 0 !important;
  min-height: 0 !important;
  margin: 0 !important;
  padding: 0 !important;
  border: 0 !important;
  background: transparent !important;
  pointer-events: none;
  z-index: 50 !important;
}

/* ==========================================================
   FINAL LIVE-PRICE MARKER
   The marker row has ONE colspan=19 cell. The five visible boxes
   are positioned over the REAL option-chain cells, so the center
   live price can never jump into the PE column.
   ========================================================== */
.live-marker-row > .marker-stage {
  height: 28px !important;
  min-height: 28px !important;
  max-height: 28px !important;
  padding: 0 !important;
  border: 0 !important;
  background: transparent !important;
  position: relative !important;
  overflow: visible !important;
}
.marker-track {
  position: absolute !important;
  left: 0 !important;
  top: 0 !important;
  width: 100% !important;
  height: 28px !important;
  pointer-events: none !important;
  z-index: 120 !important;
}
.marker-track::before {
  content: '' !important;
  position: absolute !important;
  left: 0 !important;
  right: 0 !important;
  top: 50% !important;
  height: 2px !important;
  transform: translateY(-50%) !important;
  background: rgba(255,255,255,.88) !important;
  z-index: 0 !important;
}
.marker-box-abs {
  position: absolute !important;
  top: 0 !important;
  height: 28px !important;
  box-sizing: border-box !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
  gap: 3px !important;
  padding: 0 2px !important;
  overflow: visible !important;
  white-space: nowrap !important;
  background: rgb(18,18,18) !important;
  color: #fff !important;
  border: 2px solid #00ff66 !important;
  border-radius: 0 !important;
  z-index: 2 !important;
}
.marker-box-abs:first-child { border-radius: 5px 0 0 5px !important; }
.marker-box-abs:last-child { border-radius: 0 5px 5px 0 !important; }
.marker-box-abs .marker-inner {
  width: 100% !important;
  min-width: 0 !important;
  height: 28px !important;
  line-height: 28px !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
  gap: 3px !important;
  overflow: visible !important;
  position: relative !important;
  z-index: 3 !important;
}
.marker-box-abs .marker-label,
.marker-box-abs .marker-value {
  font: inherit !important;
  font-weight: 900 !important;
  font-size: 14px !important;
  line-height: 1 !important;
  white-space: nowrap !important;
}
.marker-price-abs {
  background: rgb(18,18,18) !important;
  color: #fff !important;
  border-color: #00ff66 !important;
  padding: 0 2px !important;
}
.marker-price-abs .marker-label,
.marker-price-abs .marker-value {
  color: #fff !important;
  font-size: 14px !important;
  line-height: 1 !important;
  letter-spacing: -0.2px !important;
  font-weight: 900 !important;
}
#floatingChambers { display: block !important; visibility: visible !important; opacity: 1 !important; }
#blood, .topbar .live { display: none !important; }


/* FULL-DAY CANDLE VIEWPORT: swipe/drag left-right on mobile */
#candleCanvas{overflow-x:auto!important;overflow-y:hidden!important;-webkit-overflow-scrolling:touch;scrollbar-width:thin;overscroll-behavior-x:contain;touch-action:pan-x;}
#candleCanvas::-webkit-scrollbar{height:7px;}
#candleCanvas::-webkit-scrollbar-thumb{background:#52697d;border-radius:8px;}
#candleCanvas::-webkit-scrollbar-track{background:#16212b;border-radius:8px;}
#candleCanvas svg{display:block;}

/* SETTINGS UI */
.candle-settings-bar{position:relative;display:flex;align-items:center;gap:7px;margin:5px 5px 7px;padding:5px 7px;background:#17222e;border:1px solid #33495c;border-radius:8px;min-height:34px;}
.settings-btn{width:31px;height:29px;border:1px solid #52697d;border-radius:7px;background:#243443;color:#fff;font-size:16px;cursor:pointer;font-weight:900;}
.settings-title{font-size:10px;font-weight:900;letter-spacing:.5px;opacity:.85;}
.candle-settings-bar #coaBadge,
.candle-settings-bar #ws,
.candle-settings-bar #greeks{
  flex:0 0 auto;
  white-space:nowrap;
  font-size:9px !important;
  font-weight:900 !important;
  line-height:1.1;
}
.candle-settings-bar #coaBadge{padding:4px 6px !important;}
.candle-settings-bar #ws,
.candle-settings-bar #greeks{opacity:.9;}

.candle-settings-panel{position:absolute;left:5px;right:5px;top:39px;z-index:300;background:#111a23;border:1px solid #40566a;border-radius:9px;padding:8px;box-shadow:0 8px 25px rgba(0,0,0,.45);}
.candle-settings-panel[hidden]{display:none!important;}
.setting-row{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:7px 3px;border-bottom:1px solid rgba(120,145,165,.16);font-size:10px;font-weight:900;}
.setting-row:last-child{border-bottom:0;}
.setting-row select,.theme-btn{background:#1d2a36;color:#fff;border:1px solid #496174;border-radius:6px;padding:5px 7px;font-weight:800;font-size:10px;}
.theme-btn{cursor:pointer;}
body.light-mode{background:#f4f6f8!important;color:#17202a!important;}
body.light-mode .topbar{background:#fff!important;border-color:#d4dce3!important;}
body.light-mode .info,body.light-mode .candle-panel,body.light-mode .level-story-row,body.light-mode .candle-settings-bar{background:#fff!important;color:#17202a!important;border-color:#cbd5df!important;}
body.light-mode .candle-settings-panel{background:#fff!important;color:#17202a!important;border-color:#cbd5df!important;}
body.light-mode .setting-row select,body.light-mode .theme-btn,body.light-mode .settings-btn{background:#eef2f5!important;color:#17202a!important;border-color:#b8c4cf!important;}
body.light-mode #candleCanvas{background:#fff!important;} body.light-mode #candleCanvas svg{background:#fff!important;}
body.light-mode .candle-meta{color:#5b6772!important;}

/* =========================================================
   LIGHT / WHITE MODE — OPTION CHAIN FULL VISIBILITY FIX
   IMPORTANT: every normal table descendant gets a dark text color.
   Sign colors are then applied with higher specificity.
   ========================================================= */
body.light-mode .wrap,
body.light-mode .wrap table,
body.light-mode .wrap tbody,
body.light-mode .wrap tr {
  background:#ffffff !important;
  color:#17202a !important;
}

/* Force every normal option-chain cell and its contents visible. */
body.light-mode .wrap td,
body.light-mode .wrap td * {
  color:#17202a !important;
}

body.light-mode .wrap td {
  background:#ffffff !important;
  border-color:#c7d0d9 !important;
  font-weight:700;
}

/* Normal numeric values */
body.light-mode .wrap td .num,
body.light-mode .wrap td .metric-number,
body.light-mode .wrap td .rank-percent {
  color:#17202a !important;
  opacity:1 !important;
  text-shadow:none !important;
}

/* PLUS = GREEN */
body.light-mode .wrap td .green,
body.light-mode .wrap td .metric-number.green,
body.light-mode .wrap td[data-field="ce_oi_change"] .green,
body.light-mode .wrap td[data-field="pe_oi_change"] .green {
  color:#008a45 !important;
  opacity:1 !important;
  font-weight:900 !important;
}

/* MINUS = RED */
body.light-mode .wrap td .red,
body.light-mode .wrap td .metric-number.red,
body.light-mode .wrap td[data-field="ce_oi_change"] .red,
body.light-mode .wrap td[data-field="pe_oi_change"] .red {
  color:#d00000 !important;
  opacity:1 !important;
  font-weight:900 !important;
}

/* ZERO / unavailable */
body.light-mode .wrap td .muted {
  color:#5f6b76 !important;
  opacity:1 !important;
}

/* Highest / second-highest cells */
body.light-mode .wrap td.rank-highest {
  background:#55d9ff !important;
  color:#06131a !important;
}
body.light-mode .wrap td.rank-highest * {
  color:#06131a !important;
}
body.light-mode .wrap td.rank-second {
  background:#ffe14d !important;
  color:#171200 !important;
}
body.light-mode .wrap td.rank-second * {
  color:#171200 !important;
}

/* FINAL MOBILE READABILITY FIX: ranked numbers themselves must stay
   high-contrast even if the cell is an OI-change cell (green/red). */
body.light-mode .wrap td.rank-highest .metric-number,
body.light-mode .wrap td.rank-highest .rank-percent {
  color:#06131a !important;
  text-shadow:0 1px 0 rgba(255,255,255,.45) !important;
}
body.light-mode .wrap td.rank-second .metric-number,
body.light-mode .wrap td.rank-second .rank-percent {
  color:#3b2500 !important;
  text-shadow:0 1px 0 rgba(255,255,255,.55) !important;
}

/* If an OI-change cell itself is ranked, its +/- number must still win. */
body.light-mode .wrap td[data-field="ce_oi_change"] .green,
body.light-mode .wrap td[data-field="pe_oi_change"] .green {
  color:#008a45 !important;
}
body.light-mode .wrap td[data-field="ce_oi_change"] .red,
body.light-mode .wrap td[data-field="pe_oi_change"] .red {
  color:#d00000 !important;
}

/* Strike column */
body.light-mode .wrap td.strike,
body.light-mode .wrap td.strike * {
  background:#ff8a00 !important;
  color:#111111 !important;
  font-weight:900 !important;
}
body.light-mode .wrap th.strike-head {
  background:#ff8a00 !important;
  color:#111111 !important;
}

/* CALL / PUT headers intentionally keep white lettering. */
body.light-mode .wrap th.call-head {
  background:#087b42 !important;
  color:#ffffff !important;
}
body.light-mode .wrap th.put-head {
  background:#a91f2c !important;
  color:#ffffff !important;
}
body.light-mode .wrap th.strike-head {
  background:#ff8a00 !important;
  color:#111111 !important;
}

/* Header cells other than the colored group headers */
body.light-mode .wrap th:not(.call-head):not(.put-head):not(.strike-head) {
  background:#eef2f5 !important;
  color:#17202a !important;
}

/* FINAL RANK BOX TEXT FIX: every text item inside the BLUE
   HIGHEST and YELLOW SECOND-HIGHEST boxes is BLACK. */
.rank-highest,
.rank-highest *,
.rank-second,
.rank-second * {
    color: #000000 !important;
    text-shadow: none !important;
}

/* Keep the two rank box backgrounds unchanged. */
.rank-highest {
    background: #55d9ff !important;
}
.rank-second {
    background: #ffe14d !important;
}

</style></head>


<body>


<!-- =======================================================
     TOP BAR
     ======================================================= -->

<!-- =======================================================
     INFO BAR
     ======================================================= -->

<!-- =======================================================
     LIVE ANGEL ONE 5-MINUTE OHLC + COA PANEL
     ======================================================= -->
<div id="candlePanel" class="candle-panel">
  <div class="candle-settings-bar">
    <!-- COA / WS / Angel Greeks moved here from the deleted INFO bar. -->
    <div id="coaBadge" class="coa-badge neutral" aria-live="polite" title="Current Chart of Accuracy scenario">
      <span class="coa-mini">COA</span>
      <strong id="coaNumber">--</strong>
      <span id="coaState">WAIT</span>
    </div>
    <div id="ws" class="status">WS: --</div>
    <div id="greeks" class="greeks-live">Greeks: --</div>
    <button id="candleSettingsBtn" class="settings-btn" type="button" onclick="toggleCandleSettings()" aria-label="Candle settings" title="Settings">⚙️</button>
    <span class="settings-title">SETTINGS</span>
    <div id="candleSettingsPanel" class="candle-settings-panel" hidden>
      <div class="setting-row">
        <span>CANDLE</span>
        <label class="candle-switch" title="Turn candle chart ON/OFF">
          <input id="candleToggle" type="checkbox" checked onchange="toggleCandleChart(this.checked)">
          <span class="candle-slider"></span>
        </label>
        <span id="candleToggleLabel" class="candle-switch-label">ON</span>
      </div>
      <div class="setting-row">
        <span>MODE</span>
        <button id="themeToggle" class="theme-btn" type="button" onclick="toggleTheme()">DARK / WHITE</button>
      </div>
      <div class="setting-row expiry-setting-row">
        <span>EXPIRY</span>
        <select id="expiry" class="candle-expiry" onchange="changeExpiry()" aria-label="Expiry date"></select>
      </div>
      <div class="setting-row">
        <span>INTERVAL</span>
        <select id="candleInterval" onchange="loadLiveCandles()">
          <option value="ONE_MINUTE">1 MIN</option>
          <option value="THREE_MINUTE">3 MIN</option>
          <option value="FIVE_MINUTE" selected>5 MIN</option>
          <option value="TEN_MINUTE">10 MIN</option>
          <option value="FIFTEEN_MINUTE">15 MIN</option>
          <option value="THIRTY_MINUTE">30 MIN</option>
          <option value="ONE_HOUR">1 HOUR</option>
        </select>
      </div>
    </div>
  </div>
  <div id="candleBody">
  <div id="candleCanvas" role="img" aria-label="NIFTY 50 live five minute candlestick chart"></div>
  <div id="candleMeta" class="candle-meta">Direct Angel One SmartAPI OHLCV • No local pattern calculation</div>
  </div>
</div>

<div class="level-story-row">
  <button id="callStory" class="level-story-box stable" type="button" onclick="openShiftHistory('call')" aria-label="Open CALL shift history">
    <span class="story-title">CALL</span><span class="story-text">--</span><span class="story-hint">TOUCH • SHIFT LIST</span>
  </button>
  <button id="putStory" class="level-story-box stable" type="button" onclick="openShiftHistory('put')" aria-label="Open PUT shift history">
    <span class="story-title">PUT</span><span class="story-text">--</span><span class="story-hint">TOUCH • SHIFT LIST</span>
  </button>
</div>

<div id="shiftHistoryModal" class="shift-history-modal" aria-hidden="true">
  <div class="shift-history-card" role="dialog" aria-modal="true" aria-labelledby="shiftHistoryTitle">
    <div class="shift-history-head">
      <strong id="shiftHistoryTitle">SHIFT HISTORY</strong>
      <button type="button" class="shift-close" onclick="closeShiftHistory()">×</button>
    </div>
    <div id="shiftHistoryMeta" class="shift-history-meta"></div>
    <div id="shiftHistoryList" class="shift-history-list"></div>
  </div>
</div>

<div
    id="error"
    class="error"
    style="padding:6px"
></div>


<!-- =======================================================
     OPTION CHAIN
     ======================================================= -->

<div class="wrap">

<!-- ANGEL ONE STYLE ATM-MID FLOATING CHAMBERS: no extra row/gap -->
<div id="floatingChambers" class="floating-chambers" aria-hidden="true">
  <div id="fc-ce-oi" class="fc-box fc-ce-oi"><span class="fc-label">WTB</span><span class="fc-value">--</span></div>
  <div id="fc-ce-vol" class="fc-box fc-ce-vol"><span class="fc-label">WTT</span><span class="fc-value">--</span></div>
  <div id="fc-price" class="fc-price"><span class="fc-dot"></span><span id="fc-clock" class="fc-price-value">--:--:--</span></div>
  <div id="fc-pe-vol" class="fc-box fc-pe-vol"><span class="fc-label">WTT</span><span class="fc-value">--</span></div>
  <div id="fc-pe-oi" class="fc-box fc-pe-oi"><span class="fc-label">WTB</span><span class="fc-value">--</span></div>
</div>

<table>

<thead>

<tr>

<th
    colspan="9"
    class="call-head"
>
    CALL / CE
</th>

<th
    rowspan="2"
    class="strike-head"
>
    STRIKE
</th>

<th
    colspan="9"
    class="put-head"
>
    PUT / PE
</th>

</tr>


<tr>

<th>VEGA</th>
<th>THETA</th>
<th>GAMMA</th>
<th>DELTA</th>
<th>IV</th>
<th>LTP</th>
<th>OI CHG</th>
<th>OI</th>
<th>VOL</th>

<th>VOL</th>
<th>OI</th>
<th>OI CHG</th>
<th>LTP</th>
<th>IV</th>
<th>DELTA</th>
<th>GAMMA</th>
<th>THETA</th>
<th>VEGA</th>

</tr>

</thead>

<tbody
    id="chain"
>
</tbody>

</table>

</div>


<!-- =======================================================
     POSITIONS
     ======================================================= -->

<div class="section">

<div class="section-title">
POSITIONS / LIVE P&L
</div>

<div class="positions">

<table>

<thead>

<tr>

<th>SYMBOL</th>
<th>NET QTY</th>
<th>BUY AVG</th>
<th>SELL AVG</th>
<th>LTP</th>
<th>P&L</th>
<th>ACTION</th>

</tr>

</thead>

<tbody
    id="positions"
>
</tbody>

</table>

</div>

</div>


<script>

let currentData = null;

// ==========================================================
// CENTER LIVE CLOCK (IST)
// ==========================================================
function updateCenterClock(){
  const el=document.getElementById('fc-clock');
  if(!el) return;
  try {
    const now=new Date();
    const parts=new Intl.DateTimeFormat('en-GB',{
      timeZone:'Asia/Kolkata', hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false
    }).formatToParts(now);
    const get=k=>(parts.find(x=>x.type===k)||{}).value||'00';
    el.textContent=`${get('hour')}:${get('minute')}:${get('second')}`;
  } catch(e) {
    const now=new Date();
    el.textContent=[now.getHours(),now.getMinutes(),now.getSeconds()].map(v=>String(v).padStart(2,'0')).join(':');
  }
}
updateCenterClock();
setInterval(updateCenterClock,1000);


/* ==========================================================
   FORMAT
   ========================================================== */

function fmt(
    value,
    digits = 2
) {

    const n =
        Number(value || 0);

    return n.toLocaleString(
        'en-IN',
        {
            minimumFractionDigits:
                digits,

            maximumFractionDigits:
                digits
        }
    );
}


function integer(
    value
) {

    return Number(
        value || 0
    ).toLocaleString(
        'en-IN'
    );
}

function signedInteger(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n)) return '0';
    if (n > 0) return '+' + Math.round(n).toLocaleString('en-IN');
    if (n < 0) return '-' + Math.abs(Math.round(n)).toLocaleString('en-IN');
    return '0';
}


/* ==========================================================
   RANK BOX
   ========================================================== */

function rankBox(
    obj
) {

    if (!obj) {
        return "";
    }

    const pct = Number(obj.percent);
    const safePct = Number.isFinite(pct) ? Math.abs(pct) : 0;
    const ptxt = safePct.toFixed(2) + "%";

    return `
        <span class="rank-percent ${obj.class || ""}">${ptxt}</span>
    `;
}


/* ==========================================================
   NUMBER COLOR
   ========================================================== */

function numberClass(
    value
) {

    if (
        Number(value) > 0
    ) {

        return "green";
    }

    if (
        Number(value) < 0
    ) {

        return "red";
    }

    return "muted";
}


/* ==========================================================
   NUMBER CELL
   ========================================================== */

function cellNumber(
    value,
    digits = 2
) {

    return `
        <span class="num">
            ${fmt(
                value,
                digits
            )}
        </span>
    `;
}


/* ==========================================================
   LTP
   ========================================================== */

function ltpCell(
    value,
    close = 0
) {

    // During the live session show only the live LTP. After the market
    // closes, fall back to the last saved closing/last-traded value.
    const liveValue = Number(value);
    const closeValue = Number(close);
    const marketClosed = currentData && currentData.market_open === false;
    const n = marketClosed && (!Number.isFinite(liveValue) || liveValue <= 0)
        && Number.isFinite(closeValue) && closeValue > 0
        ? closeValue : liveValue;

    if (
        !Number.isFinite(n)
        || n <= 0
    ) {

        return `
            <span class="num muted">
                --
            </span>
        `;
    }

    return `
        <span class="num">
            ${fmt(n, 2)}
        </span>
    `;
}


/* ==========================================================
   ROW
   ========================================================== */

function makeRow(
    row
) {

    const ce =
        row.ce || {};

    const pe =
        row.pe || {};

    const strike =
        Number(
            row.strike || 0
        );

    let strikeClass =
        "strike";

    if (
        currentData
        &&
        Number(
            currentData.atm
        ) === strike
    ) {

        strikeClass +=
            " atm";
    }



    const ceOiClass =
        ce.oi_rank?.class || "";

    const ceVolClass =
        ce.volume_rank?.class || "";

    const ceChgClass =
        ce.oi_change_rank?.class || "";

    const peVolClass =
        pe.volume_rank?.class || "";

    const peOiClass =
        pe.oi_rank?.class || "";

    const peChgClass =
        pe.oi_change_rank?.class || "";


    const token = ce.token || pe.token || "";

    return `
    <tr data-token="${token}" data-ce-token="${ce.token || ""}" data-pe-token="${pe.token || ""}">

        <!-- CALL -->

        <!-- CALL GREEKS: mirror order of the PUT side -->
        <td data-field="ce_vega">${cellNumber(ce.vega, 3)}</td>
        <td data-field="ce_theta">${cellNumber(ce.theta, 3)}</td>
        <td data-field="ce_gamma">${cellNumber(ce.gamma, 6)}</td>
        <td data-field="ce_delta">${cellNumber(ce.delta, 4)}</td>
        <td data-field="ce_iv">${cellNumber(ce.iv)}</td>

        <td data-field="ce_ltp">
            <span class="${numberClass(ce.percent_change)}">${ltpCell(ce.ltp, ce.close)}</span>
        </td>

        <td data-field="ce_oi_change" data-live-value="${ce.oi_change}" class="${ceChgClass}">

            <span class="${numberClass(ce.oi_change)}">${signedInteger(ce.oi_change)}</span>

            <br>

            ${rankBox(
                ce.oi_change_rank
            )}

        </td>

        <td data-field="ce_oi" data-live-value="${ce.oi}" class="${ceOiClass}">

            ${integer(ce.oi)}

            <br>

            ${rankBox(
                ce.oi_rank
            )}

        </td>

        <td data-field="ce_volume" data-live-value="${ce.volume}" class="${ceVolClass}">

            ${integer(ce.volume)}

            <br>

            ${rankBox(
                ce.volume_rank
            )}

        </td>


        <!-- STRIKE -->

        <td class="${strikeClass}">

            ${fmt(
                strike,
                0
            )}

        </td>


        <!-- PUT -->

        <td data-field="pe_volume" data-live-value="${pe.volume}" class="${peVolClass}">

            ${integer(pe.volume)}

            <br>

            ${rankBox(
                pe.volume_rank
            )}

        </td>

        <td data-field="pe_oi" data-live-value="${pe.oi}" class="${peOiClass}">

            ${integer(pe.oi)}

            <br>

            ${rankBox(
                pe.oi_rank
            )}

        </td>

        <td data-field="pe_oi_change" data-live-value="${pe.oi_change}" class="${peChgClass}">

            <span class="${numberClass(pe.oi_change)}">${signedInteger(pe.oi_change)}</span>

            <br>

            ${rankBox(
                pe.oi_change_rank
            )}

        </td>

        <td data-field="pe_ltp">
            <span class="${numberClass(pe.percent_change)}">${ltpCell(pe.ltp, pe.close)}</span>
        </td>

        <td>
            ${cellNumber(
                pe.iv
            )}
        </td>

        <td>
            ${cellNumber(
                pe.delta,
                4
            )}
        </td>

        <td>
            ${cellNumber(
                pe.gamma,
                6
            )}
        </td>

        <td>
            ${cellNumber(
                pe.theta,
                3
            )}
        </td>

        <td>
            ${cellNumber(
                pe.vega,
                3
            )}
        </td>

    </tr>
    `;
}


/* ==========================================================
   CHAIN RENDER
   ========================================================== */

function renderChain(
    rows
) {

    const body =
        document.getElementById(
            "chain"
        );

    body.innerHTML =
        rows
        .map(
            makeRow
        )
        .join("");

    const livePrice = Number(currentData?.nifty?.ltp || 0);
    if (livePrice > 0 && typeof updateLivePriceMarker === "function") {
        updateLivePriceMarker(livePrice);
        requestAnimationFrame(() => positionATMMidChambers(livePrice));
        setTimeout(() => positionATMMidChambers(livePrice, 1), 120);
    }
}


/* ==========================================================
   EXPIRIES
   ========================================================== */

async function loadExpiries() {

    try {

        const response =
            await fetch(
                "/api/expiries"
            );

        const data =
            await response.json();

        const select =
            document.getElementById(
                "expiry"
            );

        select.innerHTML = "";

        for (
            const [index, expiry]
            of (data.expiries || []).entries()
        ) {

            const option =
                document.createElement(
                    "option"
                );

            option.value =
                expiry;

            option.textContent =
                (data.display && data.display[index]) || expiry;

            if (
                expiry ===
                data.selected
            ) {

                option.selected =
                    true;
            }

            select.appendChild(
                option
            );
        }

    } catch (e) {

        console.error(e);
    }
}


/* ==========================================================
   CHANGE EXPIRY
   ========================================================== */

async function changeExpiry() {

    const expiry =
        document.getElementById(
            "expiry"
        ).value;

    try {

        const response = await fetch(
            "/api/set_expiry",
            {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ expiry: expiry })
            }
        );
        const result = await response.json();
        if (!response.ok || !result.ok) {
            throw new Error(result.error || "Expiry selection failed");
        }

        await loadExpiries();

        await updateData();

    } catch (e) {

        console.error(e);
    }
}


/* ==========================================================
   UPDATE DATA
   ========================================================== */

function setTextSafe(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value == null ? "" : String(value);
    return el;
}

async function updateData() {

    try {

        const response =
            await fetch(
                "/api/data",
                {
                    cache:
                        "no-store"
                }
            );

        const data =
            await response.json();

        if (
            !response.ok
            ||
            data.server_ok === false
        ) {

            const msg =
                data.error
                ||
                (
                    "HTTP "
                    + response.status
                );

            setTextSafe("error", msg);
            setTextSafe(
                "ws",
                "WS: " + (data.websocket || "ERROR") +
                (data.error ? " • " + String(data.error).slice(0, 120) : "")
            );

            return;
        }

        currentData =
            data;

        if (window.liveStreamHasTicks) {
            updateHeaderFromPayload(data);
            return;
        }

        /* ----------------------------------------------------
           DIAGNOSTIC
           ---------------------------------------------------- */

        const diagnostic =
            document.getElementById("diagnostic") ||
            document.getElementById("error");

        if (
            data.error
        ) {

            diagnostic.textContent =
                data.error;

        }
        else if (
            !data.logged_in
        ) {

            diagnostic.textContent =
                "Angel One login is not active. "
                +
                "Check environment variables.";

        }
        else if (
            !data.data_ready
        ) {

            diagnostic.textContent =
                "Connected to server, "
                +
                "but no NIFTY LTP received yet. "
                +
                "Angel One WebSocket is waiting for the first live tick...";

        }
        else if (
            data.market_open === false
            &&
            data.nifty?.ltp > 0
        ) {

            diagnostic.textContent =
                "MARKET CLOSED • "
                +
                "LAST TRADED / CLOSING PRICE";

        }
        else {

            diagnostic.textContent =
                "DATA OK • LTP: "
                +
                (
                    data.option_data_count
                    || 0
                )
                +
                " • OI: "
                +
                (
                    data.option_oi_count
                    || 0
                )
                +
                " • VOL: "
                +
                (
                    data.option_volume_count
                    || 0
                );
        }


        /* ----------------------------------------------------
           RESISTANCE
           ---------------------------------------------------- */

        setTextSafe("resistance", "Resistance: " + fmt(data.resistance, 0));


        /* ----------------------------------------------------
           SUPPORT
           ---------------------------------------------------- */

        setTextSafe("support", "Support: " + fmt(data.support, 0));


        /* ----------------------------------------------------
           WEBSOCKET
           ---------------------------------------------------- */

        setTextSafe("ws", "WS: " + (data.websocket || "--"));


        /* ----------------------------------------------------
           GREEKS
           ---------------------------------------------------- */

        setTextSafe("greeks", "Angel Greeks: " + (data.greeks_update || "--") + " • " + (data.greeks_count ?? 0) + " contracts");


        /* ----------------------------------------------------
           CHAIN
           ---------------------------------------------------- */

        renderChain(
            data.rows || []
        );

    } catch (e) {

        console.error(e);

        setTextSafe("error", "Browser/API connection error: " + (e.message || e));
    }
}


/* ==========================================================
   POSITIONS
   ========================================================== */

async function updatePositions() {

    try {

        const response =
            await fetch(
                "/api/positions",
                {
                    cache:
                        "no-store"
                }
            );

        const data =
            await response.json();

        const body =
            document.getElementById(
                "positions"
            );

        if (
            !data.ok
            ||
            !data.positions
        ) {

            body.innerHTML =
                "";

            return;
        }

        body.innerHTML =
            data.positions
            .map(
                p => {

                    const pnl =
                        Number(
                            p.pnl || 0
                        );

                    return `
                    <tr>

                        <td>
                            ${p.symbol}
                        </td>

                        <td>
                            ${integer(
                                p.net_qty
                            )}
                        </td>

                        <td>
                            ${fmt(
                                p.buy_avg
                            )}
                        </td>

                        <td>
                            ${fmt(
                                p.sell_avg
                            )}
                        </td>

                        <td>
                            ${fmt(
                                p.ltp
                            )}
                        </td>

                        <td class="${
                            pnl >= 0
                            ?
                            "green"
                            :
                            "red"
                        }">

                            ${fmt(
                                pnl
                            )}

                        </td>

                        <td>

                            ${
                                p.net_qty !== 0

                                ?

                                `<button
                                    class="sell"
                                    onclick="squareOff(
                                        '${p.symbol}',
                                        '${p.token}',
                                        '${p.exchange}',
                                        ${Math.abs(
                                            p.net_qty
                                        )}
                                    )"
                                >
                                    SQUARE OFF
                                </button>`

                                :

                                ""
                            }

                        </td>

                    </tr>
                    `;
                }
            )
            .join("");

    } catch (e) {

        console.error(e);
    }
}


/* ==========================================================
   SQUARE OFF
   ========================================================== */

async function squareOff(
    symbol,
    token,
    exchange,
    quantity
) {

    if (
        !confirm(
            "Confirm live square-off?"
        )
    ) {

        return;
    }

    try {

        const response =
            await fetch(
                "/api/square_off",
                {
                    method:
                        "POST",

                    headers: {
                        "Content-Type":
                            "application/json"
                    },

                    body:
                        JSON.stringify({
                            symbol,
                            token,
                            exchange,
                            quantity
                        })
                }
            );

        const data =
            await response.json();

        alert(
            data.ok
            ?
            "Order sent."
            :
            data.error
        );

    } catch (e) {

        alert(
            "Order error: "
            +
            e.message
        );
    }
}


/* ==========================================================
   START
   ========================================================== */

loadExpiries();

updateData();

updatePositions();


/* ==========================================================
   EVENT-DRIVEN LIVE STREAM — NO MARKET-DATA AUTO REFRESH
   ========================================================== */

// ==========================================================
// TRUE TICK-BY-TICK LIVE STREAM
// ==========================================================
let tickStream = null;
window.liveStreamHasTicks = false;

function applyLivePayload(data) {
    if (!data) return;
    currentData = data;
    updateHeaderFromPayload(data);
    // IMPORTANT: render the option chain FIRST, then create/position the
    // five floating marker boxes. On a hard refresh the marker row does not
    // exist yet, so positioning it before renderChain() made it disappear.
    const incomingRows = Array.isArray(data.rows) ? data.rows : [];
    if (incomingRows.length > 0) {
        renderChain(incomingRows);
    }
    updateFloatingChambers(data);
    const livePrice = Number(data.nifty?.ltp || 0);
    if (livePrice > 0) {
        requestAnimationFrame(() => positionATMMidChambers(livePrice));
    }
}

function updateHeaderFromPayload(data) {
    const diagnostic = document.getElementById("diagnostic") || document.getElementById("error");
    if (diagnostic && data.server_ok !== false) {
        diagnostic.title = "Angel One SmartWebSocketV2 live tick stream";
        const nowMs = Date.now() / 1000;
        const rawAge = data.last_ws_message_at ? Math.max(0, nowMs - Number(data.last_ws_message_at)) : -1;
        const niftyAge = data.last_nifty_tick_at ? Math.max(0, nowMs - Number(data.last_nifty_tick_at)) : -1;
        const optionAge = data.last_option_tick_at ? Math.max(0, nowMs - Number(data.last_option_tick_at)) : -1;
        if (data.websocket === "CONNECTED" && data.raw_ws_messages > 0 && niftyAge >= 0 && niftyAge <= 5) {
            diagnostic.textContent = "LIVE RAW TICK • ANGEL ONE • RAW: " + (data.raw_ws_messages || 0) + " • NIFTY: " + niftyAge.toFixed(1) + "s • OPT: " + (optionAge >= 0 ? optionAge.toFixed(1) + "s" : "--");
        } else if (data.websocket !== "CONNECTED") {
            diagnostic.textContent = "ANGEL ONE WS " + (data.websocket || "DISCONNECTED") + " • RAW: " + (data.raw_ws_messages || 0);
        } else {
            diagnostic.textContent = "WS CONNECTED • NO FRESH TICK • RAW: " + (data.raw_ws_messages || 0) + " • LAST TOKEN: " + (data.last_ws_token || "--");
        }
    }
    // Live session: show live LTP. Closed session: show the saved close
    // if a live LTP is unavailable.
    const liveNiftyLtp = Number(data.nifty?.ltp || 0);
    const savedNiftyClose = Number(data.nifty?.close || 0);
    const shownNifty = liveNiftyLtp > 0 ? liveNiftyLtp
        : (data.market_open === false && savedNiftyClose > 0 ? savedNiftyClose : 0);
    const spot = document.getElementById("spot");
    const spotSource = liveNiftyLtp > 0 ? (data.nifty?.ltp_source || "LIVE")
        : (shownNifty > 0 ? "LAST CLOSE" : "WAITING");
    if (spot) spot.textContent = "Spot: " + (shownNifty > 0 ? fmt(shownNifty) : "--") + " [" + spotSource + "]";
    const atm = document.getElementById("atm");
    if (atm) atm.textContent = "ATM: " + fmt(data.atm, 0);
    const wsEl = document.getElementById("ws");
    if (wsEl) wsEl.textContent = "WS: ANGEL ONE " + (data.websocket || "--");
    const gr = document.getElementById("greeks");
    if (gr) gr.textContent = "Angel Greeks: " + (data.greeks_update || "--") + " • " + (data.greeks_count ?? 0) + " contracts";
}

function metricPercent(side, field, value) {
    const n = Number(value || 0);
    let max = 0;
    document.querySelectorAll(`[data-field="${side === "CE" ? "ce" : "pe"}_${field}"]`).forEach(el => {
        const v = Number(el.dataset.liveValue || 0);
        if (Number.isFinite(v) && v > max) max = v;
    });
    if (max <= 0) return 0;
    return Math.abs(n / max * 100);
}

function metricCell(value, side, field, rankClass = "") {
    const pct = metricPercent(side, field, value);
    const ptxt = Number.isFinite(pct) ? pct.toFixed(2) + "%" : "0.00%";
    const n = Number(value || 0);
    const signClass = field === "oi_change"
        ? (n > 0 ? "green" : n < 0 ? "red" : "muted")
        : "";
    const displayValue = field === "oi_change" ? signedInteger(value) : integer(value);
    // Keep the highest/second-highest number itself visually distinct from
    // its colored ranking box.  The rank class is intentionally applied to
    // BOTH the raw number and percentage so the value never blends into the
    // cyan/yellow background.
    return `<span class="metric-number ${signClass} ${rankClass}">${displayValue}</span><br><span class="rank-percent ${rankClass}">${ptxt}</span>`;
}

function updateMetricCell(row, side, field, value) {
    const prefix = side === "CE" ? "ce" : "pe";
    const el = row.querySelector(`[data-field="${prefix}_${field}"]`);
    if (!el) return;
    el.dataset.liveValue = String(Number(value || 0));
    el.innerHTML = metricCell(value, side, field);
}

function refreshAllMetricPercentages() {
    document.querySelectorAll('tr[data-ce-token], tr[data-pe-token]').forEach(row => {
        ["volume", "oi", "oi_change"].forEach(field => {
            ["CE", "PE"].forEach(side => {
                const prefix = side === "CE" ? "ce" : "pe";
                const el = row.querySelector(`[data-field="${prefix}_${field}"]`);
                if (!el) return;
                const value = Number(el.dataset.liveValue || String(el.textContent).replace(/,/g, "").match(/-?\d+(?:\.\d+)?/)?.[0] || 0);
                el.dataset.liveValue = String(value);
                el.innerHTML = metricCell(value, side, field);
            });
        });
    });
}

function updateOptionCell(row, side, d) {
    if (!row) return;
    const prefix = side === "CE" ? "ce" : "pe";
    const set = (field, html, flash=true) => {
        const el = row.querySelector(`[data-field="${prefix}_${field}"]`);
        if (!el) return;
        el.innerHTML = html;
        if (flash) {
            el.classList.remove('tick-flash');
            void el.offsetWidth;
            el.classList.add('tick-flash');
        }
    };
    if (d.ltp !== undefined) {
        const n = Number(d.ltp);
        const close = Number(d.close || 0);
        const pct = Number(d.percent_change || 0);
        set("ltp", `<span class="${numberClass(pct)}">${ltpCell(n, close)}</span>`);
    }
    // RAW ANGEL VALUES: update the number immediately from the exact tick.
    // The rank/percentage decoration is refreshed separately by the derived
    // snapshot and must never replace or alter the raw number itself.
    if (d.volume !== undefined) updateMetricCell(row, side, "volume", d.volume);
    if (d.oi !== undefined) updateMetricCell(row, side, "oi", d.oi);
    if (d.oi_change !== undefined) updateMetricCell(row, side, "oi_change", d.oi_change);
}

function applyTickDelta(d) {
    if (!d) return;
    if (d.type === "full_snapshot" && d.payload) {
        // A snapshot with no rows must not blank an already populated chain.
        applyLivePayload(d.payload);
        return;
    }
    if (d.type === "rechain" && d.payload) {
        window.liveStreamHasTicks = true;
        applyLivePayload(d.payload);
        return;
    }
    if (d.type === "greeks" && d.payload) {
        applyLivePayload(d.payload);
        return;
    }
    if (d.type === "derived" && d.payload) {
        currentData = d.payload;
        updateFloatingChambers(currentData);
        updateHeaderFromPayload(currentData);
        // Rechain only when there are real rows. Never paint an empty array
        // over a working option chain during a transient websocket update.
        if (Array.isArray(currentData.rows) && currentData.rows.length > 0) {
            const existingRows = document.querySelectorAll('#chain tr[data-ce-token], #chain tr[data-pe-token]').length;
            if (existingRows === 0) renderChain(currentData.rows);
        }
        if (Number(currentData.nifty?.ltp || 0) > 0) updateLivePriceMarker(Number(currentData.nifty.ltp));
        return;
    }
    if (d.type === "nifty") {
        const n = Number(d.ltp || 0);
        window.liveStreamHasTicks = true;
        if (currentData) {
            currentData.nifty = currentData.nifty || {};
            currentData.nifty.ltp = n;
            currentData.nifty.ltp_source = "LIVE";
        }
        updateLivePriceMarker(n);
        // Use Angel One exchange time, not browser refresh time.
        updateLiveCandleFromTick(n, Number(d.tick_ms || Date.now()), Number(d.volume || 0));
        const spot = document.getElementById("spot");
        if (spot && n > 0) spot.textContent = "Spot: " + fmt(n) + " [LIVE]";
        return;
    }
    if (d.type !== "option") return;
    window.liveStreamHasTicks = true;
    const token = String(d.token || "");
    const esc = window.CSS && CSS.escape ? CSS.escape(token) : token.replace(/[^a-zA-Z0-9_-]/g, "\\$&");
    let row = document.querySelector(`tr[data-ce-token="${esc}"], tr[data-pe-token="${esc}"]`);
    // If the table was not yet painted when the first option tick arrived,
    // paint the last non-empty snapshot once, then apply the tick.
    if (!row && Array.isArray(currentData?.rows) && currentData.rows.length > 0) {
        renderChain(currentData.rows);
        row = document.querySelector(`tr[data-ce-token="${esc}"], tr[data-pe-token="${esc}"]`);
    }
    if (!row) return;
    const ceToken = row.getAttribute("data-ce-token");
    updateOptionCell(row, ceToken === token ? "CE" : "PE", d);
    updateFloatingChambers(currentData || {});
    const livePrice = Number(currentData?.nifty?.ltp || 0);
    if (livePrice > 0) requestAnimationFrame(() => positionATMMidChambers(livePrice));
}

function connectTickStream() {
    if (!window.EventSource) {
        const wsEl = document.getElementById("ws");
        if (wsEl) wsEl.textContent = "LIVE STREAM UNSUPPORTED";
        return;
    }
    if (tickStream) tickStream.close();
    tickStream = new EventSource(window.location.origin + "/api/tick-stream");
    tickStream.addEventListener("snapshot", ev => {
        // The server sends {type:"full_snapshot", payload:{...}}.
        // Route it through the same envelope handler used by live tick events;
        // passing the envelope directly to applyLivePayload() leaves the UI blank.
        try { applyTickDelta(JSON.parse(ev.data)); } catch(e) { console.error("SSE snapshot parse/apply error:", e); }
    });
    tickStream.addEventListener("tick", ev => {
        try { applyTickDelta(JSON.parse(ev.data)); } catch(e) { console.error(e); }
    });
    tickStream.onerror = () => {
        const wsEl = document.getElementById("ws");
        if (wsEl) wsEl.textContent = "SERVER STREAM: RECONNECTING...";
        // EventSource normally retries itself. Keep our own timer as a
        // backstop for browsers/proxies that leave a half-open stream.
        setTimeout(() => {
            if (!tickStream || tickStream.readyState !== EventSource.OPEN) connectTickStream();
        }, 1500);
    };
}

connectTickStream();

// No market-data REST polling here.
// Angel One Smart Stream -> server SSE -> browser is the live data path.
// EventSource automatically reconnects if the stream drops.

// Positions are not market-data streaming; refresh them manually when needed.
// No periodic market-data polling is used here.

</script>


<!-- ==========================================================
     ANGEL ONE STYLE ATM-MID FLOATING CHAMBERS
     Five overlay chambers sit BETWEEN the two strike rows around spot.
     They never create a table row or extra vertical gap.
     ========================================================== -->
<style>
.floating-chambers{
  position:absolute !important;
  left:0;
  top:0;
  width:100%;
  height:0;
  margin:0;
  padding:0;
  z-index:120;
  pointer-events:none;
  box-sizing:border-box;
}
.fc-box,.fc-price{
  position:absolute;
  top:0;
  height:20px;
  min-width:0;
  box-sizing:border-box;
  display:flex;
  align-items:center;
  justify-content:center;
  overflow:hidden;
  white-space:nowrap;
  border:1px solid #00d26a;
  border-radius:6px;
  background:#121212;
  box-shadow:0 2px 8px rgba(0,0,0,.5);
}
.fc-box{padding:0 3px;gap:2px;min-width:0;max-width:100%;overflow:hidden;}
.fc-label{font-size:11px;font-weight:900;line-height:1;opacity:.92;flex:0 0 auto;}
.fc-value{font-size:16px;font-weight:900;line-height:1;color:#ffd740;letter-spacing:0;flex:0 1 auto;min-width:0;}
.fc-price{padding:0 8px;gap:5px;background:#111;}
.fc-price-value{font-size:17px;font-weight:900;color:#ffd740;line-height:1;white-space:nowrap;flex:0 1 auto;min-width:0;}
#fc-clock{display:inline-block;min-width:78px;text-align:center;font-variant-numeric:tabular-nums;letter-spacing:.5px;color:#ffd740!important;}
.fc-dot{width:8px;height:8px;border-radius:50%;background:#ffd740;display:inline-block;flex:0 0 8px;}
.fc-price{border-color:#00d26a;}
.floating-chambers{overflow:visible !important; width:max-content !important;}
/* Angel One style: one continuous live-price guide line behind the five chambers. */
.floating-chambers::before{
  content:"";
  position:absolute;
  left:0; right:0;
  top:10px;
  height:1px;
  background:currentColor;
  opacity:.9;
  z-index:1;
  pointer-events:none;
}
.fc-box,.fc-price{
  z-index:2;
  background:#121212 !important;
  border-radius:0;
  margin:0 !important;
}
.fc-box:first-child{border-top-left-radius:5px;border-bottom-left-radius:5px;}
.fc-price{border-radius:5px;background:#111 !important;}
.fc-box:last-child{border-top-right-radius:5px;border-bottom-right-radius:5px;}
@media(max-width:700px){
  .fc-box,.fc-price{height:20px;border-radius:4px;}
  .fc-box{padding:0 3px;gap:2px;min-width:0;max-width:100%;overflow:hidden;}
  .fc-label{font-size:8px;}
  .fc-value,.fc-price-value{font-size:12px;}
  .fc-price{padding:0 5px;gap:4px;}
  .fc-dot{width:7px;height:7px;flex-basis:7px;}
}

/* FINAL WHITE MODE FIX: keep ATM floating chambers visible and readable */
body.light-mode #floatingChambers {
  display:block !important;
  visibility:visible !important;
  opacity:1 !important;
  z-index:120 !important;
}
body.light-mode #floatingChambers .fc-box,
body.light-mode #floatingChambers .fc-price {
  background:#ffffff !important;
  color:#17202a !important;
  border-color:#00a85a !important;
  box-shadow:0 2px 8px rgba(0,0,0,.18) !important;
}
body.light-mode #floatingChambers .fc-label,
body.light-mode #floatingChambers .fc-price-value {
  color:#17202a !important;
  opacity:1 !important;
  visibility:visible !important;
}
body.light-mode #floatingChambers .fc-value {
  color:#b36b00 !important;
  opacity:1 !important;
  visibility:visible !important;
}
body.light-mode .level-story-box,
body.light-mode .level-story-box.wtb,
body.light-mode .level-story-box.wtt,
body.light-mode .level-story-box.shift-up,
body.light-mode .level-story-box.shift-down {
  color:#17202a !important;
  background:#ffffff !important;
}
body.light-mode .level-story-box .story-title,
body.light-mode .level-story-box .story-text,
body.light-mode .level-story-box .story-hint {
  color:#17202a !important;
  opacity:1 !important;
}
body.light-mode .shift-history-modal {
  background:rgba(255,255,255,.78) !important;
}
body.light-mode .shift-history-card {
  background:#ffffff !important;
  color:#17202a !important;
  border-color:#ff9800 !important;
  box-shadow:0 10px 35px rgba(0,0,0,.22) !important;
}
body.light-mode .shift-history-head,
body.light-mode .shift-history-meta {
  color:#17202a !important;
  border-color:#d4dce3 !important;
}
body.light-mode .shift-close {
  background:#eef2f5 !important;
  color:#17202a !important;
}
body.light-mode .shift-event {
  background:#ffffff !important;
  color:#17202a !important;
  border-color:#ff9800 !important;
}
body.light-mode .shift-event * {
  color:#17202a !important;
}

/* FINAL FIX: the five ATM floating marker boxes are a separate marker layer.
   They must stay visible in WHITE mode and keep their live status + percentage. */
body.light-mode .marker-track::before {
  background:rgba(23,32,42,.32) !important;
}
body.light-mode .marker-box-abs,
body.light-mode .marker-price-abs {
  background:#ffffff !important;
  color:#17202a !important;
  border-color:#00a85a !important;
  box-shadow:0 1px 5px rgba(0,0,0,.14) !important;
  visibility:visible !important;
  opacity:1 !important;
}
body.light-mode .marker-box-abs .marker-inner,
body.light-mode .marker-box-abs .marker-label,
body.light-mode .marker-box-abs .marker-value,
body.light-mode .marker-price-abs .marker-label,
body.light-mode .marker-price-abs .marker-value {
  color:#17202a !important;
  visibility:visible !important;
  opacity:1 !important;
  text-shadow:none !important;
}
body.light-mode .marker-box-abs .marker-value {
  color:#17202a !important;
}

/* FINAL CANDLE VISIBILITY / STABILITY SAFETY */
#candleBody{display:block!important;visibility:visible!important;opacity:1!important;min-height:250px;}
#candleCanvas{display:block!important;visibility:visible!important;opacity:1!important;position:relative!important;isolation:isolate!important;}
#candleCanvas>svg{display:block!important;visibility:visible!important;opacity:1!important;}
#candleCanvas rect,#candleCanvas line,#candleCanvas text{vector-effect:non-scaling-stroke;}
body.light-mode #candleCanvas{background:#fff!important;}
body.light-mode #candleCanvas>svg{background:#fff!important;}
</style>
<script>
function updateFloatingChambers(data) {
  const root=document.getElementById('floatingChambers');
  if(!root || !data) return;
  root.setAttribute('aria-hidden','false');

  // Compact LTP Calculator-style live COA indicator.
  // Only the CURRENT scenario number is shown; the nine-scenario grid/banner is intentionally removed.
  const scenario=data.level_analysis?.scenario || {};
  const sn=Number(scenario.number||0);
  const coa=document.getElementById('coaBadge');
  const num=document.getElementById('coaNumber');
  const state=document.getElementById('coaState');
  const coaNames = {
    1:'STRONG / STRONG',
    2:'BEARISH',
    3:'BULLISH',
    4:'BEARISH',
    5:'BULLISH',
    6:'BLOOD BATH • HIGHLY BEARISH',
    7:'BULL RUN • HIGHLY BULLISH',
    8:'NOT TRADABLE',
    9:'NOT TRADABLE'
  };
  if(num) num.textContent=sn>=1&&sn<=9 ? `1.${sn}` : '--';
  if(state) state.textContent=sn>=1&&sn<=9 ? coaNames[sn] : 'WAIT';
  if(coa){
    coa.classList.remove('bull','bear','neutral','wait');
    if(!sn) coa.classList.add('wait');
    else if([3,5,7].includes(sn)) coa.classList.add('bull');
    else if([2,4,6,8,9].includes(sn)) coa.classList.add('bear');
    else coa.classList.add('neutral');
    const lightMode=document.body.classList.contains('light-mode');
    const scenarioColors = !sn ?
      {bg:lightMode?'#eef2f5':'#171d24',border:lightMode?'#7b8792':'#65717e',num:lightMode?'#5b6772':'#b8c2cc',fg:lightMode?'#46515c':'#d7e0e8'} :
      ([3,5,7].includes(sn) ?
        {bg:lightMode?'#e8fff2':'#092318',border:lightMode?'#00a85a':'#00e676',num:lightMode?'#008a45':'#00e676',fg:lightMode?'#075b31':'#eafff3'} :
       ([2,4,6,8,9].includes(sn) ?
        {bg:lightMode?'#fff0f1':'#241013',border:lightMode?'#d00000':'#ff3030',num:lightMode?'#d00000':'#ff5050',fg:lightMode?'#8f0000':'#fff0f0'} :
        {bg:lightMode?'#fff7e8':'#171d24',border:'#ff9800',num:lightMode?'#e47700':'#ff9800',fg:lightMode?'#6b4300':'#ffffff'}));
    coa.style.setProperty('background',scenarioColors.bg,'important');
    coa.style.setProperty('border-color',scenarioColors.border,'important');
    coa.style.setProperty('color',scenarioColors.fg,'important');
    if(num) num.style.setProperty('color',scenarioColors.num,'important');
    coa.title=sn>=1&&sn<=9 ? `Chart of Accuracy 1.0 • Scenario ${sn} • COA 1.${sn} • ${coaNames[sn]}` : 'Chart of Accuracy 1.0 • waiting for live data';
  }

  const a=data.level_analysis || {};
  const rs=a.resistance || {};
  const ss=a.support || {};
  const statusOnly=x=>{
    const st=String(x?.status||'STRONG').toUpperCase();
    return ['WTB','WTT','STRONG','SHIFT'].includes(st) ? st : 'STRONG';
  };
  const setStatus=id=>{
    const el=document.getElementById(id); if(!el) return;
    const l=el.querySelector('.fc-label');
    const v=el.querySelector('.fc-value');
    const text=statusOnly(id.includes('ce') ? rs : ss);
    if(l) l.textContent=text;
    if(v) v.textContent='';
  };

  setStatus('fc-ce-oi');
  setStatus('fc-ce-vol');
  setStatus('fc-pe-vol');
  setStatus('fc-pe-oi');

  const story=data.level_analysis?.story || {};
  const history=data.level_analysis?.history || {};
  const fmtLevel=n=>Number(n||0).toLocaleString('en-IN',{maximumFractionDigits:0});
  const storyText=(x)=>{
    const from=Number(x?.from||0), to=Number(x?.to||0);
    const st=String(x?.status||'STRONG').toUpperCase();
    const src=String(x?.source||x?.field||'').toUpperCase().replaceAll('VOL','VOLUME');
    const srcText=src ? ` • ${src}` : '';
    if(from<=0 && to<=0) return '--';
    if(st==='STRONG' && Math.abs(from-to)<1e-9) return `${fmtLevel(from)} • STABLE${srcText}`;
    return `${fmtLevel(from)} → ${fmtLevel(to)} • ${st}${srcText}`;
  };
  const applyStoryBox=(id, side, x)=>{
    const box=document.getElementById(id); if(!box) return;
    const st=String(x?.status||'STRONG').toUpperCase();
    box.classList.remove('stable','wtb','wtt','shift-up','shift-down');
    let cls='stable';
    if(st==='WTB') cls='wtb';
    else if(st==='WTT') cls='wtt';
    else if(st==='SHIFT') cls=Number(x?.to||0)>Number(x?.from||0)?'shift-up':'shift-down';
    box.classList.add(cls);
    const title=box.querySelector('.story-title');
    const text=box.querySelector('.story-text');
    if(title) title.textContent=side.toUpperCase();
    if(text) text.textContent=storyText(x);
    box.dataset.historyCount=String((history[side]||[]).length);
  };
  applyStoryBox('callStory','call',story.call||{});
  applyStoryBox('putStory','put',story.put||{});

  // The center box is a REAL-TIME CLOCK, not the NIFTY live price.
  // Never overwrite it from market-data payloads.
  const price=Number(data?.nifty?.ltp||0);
  if(price > 0) positionATMMidChambers(price);
}

function historyClass(ev){
  const st=String(ev?.status||'STRONG').toUpperCase();
  if(st==='WTB') return 'wtb';
  if(st==='WTT') return 'wtt';
  if(st==='SHIFT') return Number(ev?.to||0)>Number(ev?.from||0)?'shift-up':'shift-down';
  return 'stable';
}

function openShiftHistory(side){
  const modal=document.getElementById('shiftHistoryModal');
  if(!modal) return;
  const data=currentData?.level_analysis?.history || {};
  const events=Array.isArray(data[side]) ? data[side] : [];
  const title=document.getElementById('shiftHistoryTitle');
  const meta=document.getElementById('shiftHistoryMeta');
  const list=document.getElementById('shiftHistoryList');
  if(title) title.textContent=(side==='call'?'CALL':'PUT')+' • MORNING → CURRENT SHIFT HISTORY';
  if(meta) meta.textContent=`Date: ${data.date||'--'} • Expiry: ${data.expiry||'--'} • ${events.length} recorded state(s)`;
  if(list){
    if(!events.length){
      list.innerHTML='<div class="shift-empty">NO SHIFT / NO HISTORY YET</div>';
    }else{
      list.innerHTML=events.map(ev=>{
        const from=Number(ev.from||0), to=Number(ev.to||0);
        const status=String(ev.status||'STRONG').toUpperCase();
        const dir=String(ev.direction||status).toUpperCase();
        const movement=(from>0&&to>0)?`${from.toLocaleString('en-IN')} → ${to.toLocaleString('en-IN')}`:'--';
        const label=status==='STRONG'?'STABLE':(status==='SHIFT'?'SHIFT COMPLETED':status==='WTB'?'WEAK TO BOTTOM':'WEAK TO TOP');
        const rawSource=String(ev.source||ev.field||'').toUpperCase().replaceAll('VOL','VOLUME');
        const source=rawSource ? ` • ${rawSource}` : '';
        return `<div class="shift-event ${historyClass(ev)}"><div class="ev-time">${ev.time||'--'}</div><div><div class="ev-main">${label}${source}</div><div class="ev-sub">${movement}${ev.changed?' • LEVEL MOVED':' • NO LEVEL MOVE'}</div></div><div class="ev-dir">${dir}</div></div>`;
      }).join('');
    }
  }
  modal.classList.add('open');
  modal.setAttribute('aria-hidden','false');
}

function closeShiftHistory(){
  const modal=document.getElementById('shiftHistoryModal');
  if(!modal) return;
  modal.classList.remove('open');
  modal.setAttribute('aria-hidden','true');
}

window.addEventListener('keydown',e=>{ if(e.key==='Escape') closeShiftHistory(); });
window.addEventListener('click',e=>{ const m=document.getElementById('shiftHistoryModal'); if(m && e.target===m) closeShiftHistory(); });

function positionATMMidChambers(price, retry=0) {
  const body = document.getElementById('chain');
  const p = Number(price || 0);
  if (!body || !Number.isFinite(p) || p <= 0) return;

  // IMPORTANT: never delete the existing marker before we know the new
  // marker can be built. A transient empty/partial chain used to make the
  // five boxes disappear after refresh.
  const rows = Array.from(body.querySelectorAll('tr[data-ce-token], tr[data-pe-token]'))
    .map(row => ({
      row,
      strike: Number((row.querySelector('td.strike, .strike')?.textContent || '').replace(/,/g, '').trim())
    }))
    .filter(x => Number.isFinite(x.strike))
    .sort((a,b) => a.strike - b.strike);

  if (!rows.length) {
    if (retry < 8) requestAnimationFrame(() => positionATMMidChambers(p, retry + 1));
    return;
  }

  const insertBefore = rows.find(x => x.strike >= p)?.row || null;
  const referenceRow = insertBefore || rows[rows.length - 1].row;

  // Use semantic cells, not hard-coded child indexes. This survives hidden
  // columns / layout changes and prevents the marker from vanishing when the
  // table temporarily has a different column structure.
  const refs = {
    ceOi: referenceRow.querySelector('[data-field="ce_oi"]'),
    ceVol: referenceRow.querySelector('[data-field="ce_volume"]'),
    strike: referenceRow.querySelector('td.strike, .strike'),
    peVol: referenceRow.querySelector('[data-field="pe_volume"]'),
    peOi: referenceRow.querySelector('[data-field="pe_oi"]')
  };
  if (!refs.ceOi || !refs.ceVol || !refs.strike || !refs.peVol || !refs.peOi) {
    if (retry < 8) requestAnimationFrame(() => positionATMMidChambers(p, retry + 1));
    return;
  }

  const d = currentData || {};
  const pct = n => (Number.isFinite(Number(n)) ? Number(n).toFixed(2) : '0.00') + '%';
  const priceText = p.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2});
  const a = d.level_analysis || {};
  const rs = a.resistance || {};
  const ss = a.support || {};
  const stateOnly = x => {
    const st = String(x?.status || 'STRONG').toUpperCase();
    return ['WTB','WTT','STRONG','SHIFT'].includes(st) ? st : 'STRONG';
  };
  const liveSecondPercent = (side, field) => {
    const prefix = side === 'CE' ? 'ce' : 'pe';
    const vals = Array.from(document.querySelectorAll(`td[data-field="${prefix}_${field}"]`))
      .map(el => Number(el.dataset.liveValue || 0))
      .filter(v => Number.isFinite(v) && v > 0)
      .sort((a,b) => b-a);
    return vals.length > 1 && vals[0] > 0 ? (vals[1] / vals[0]) * 100 : 0;
  };

  // Build everything in memory first. The current marker stays visible until
  // the replacement is complete.
  const tr = document.createElement('tr');
  tr.className = 'live-marker-row';
  tr.setAttribute('aria-label', 'Live NIFTY price marker');
  const stageCell = document.createElement('td');
  stageCell.colSpan = 19;
  stageCell.className = 'marker-stage';
  const track = document.createElement('div');
  track.className = 'marker-track';
  stageCell.appendChild(track);
  tr.appendChild(stageCell);

  const stageRect = stageCell.getBoundingClientRect();
  // A freshly-created detached row has no useful geometry. Insert it
  // temporarily only after all semantic references are known.
  const existingMarker = body.querySelector('tr.live-marker-row');
  if (existingMarker) existingMarker.remove();
  if (insertBefore) body.insertBefore(tr, insertBefore); else body.appendChild(tr);

  const finalStageRect = stageCell.getBoundingClientRect();
  const place = (ref, label, value, extra='') => {
    const rr = ref.getBoundingClientRect();
    const box = document.createElement('div');
    box.className = 'marker-box-abs' + (extra === 'marker-price' ? ' marker-price-abs' : '');
    box.style.left = Math.max(0, rr.left - finalStageRect.left) + 'px';
    box.style.width = Math.max(1, rr.width) + 'px';
    box.innerHTML = `<div class="marker-inner"><span class="marker-label">${label}</span><span class="marker-value">${value}</span></div>`;
    track.appendChild(box);
  };

  place(refs.ceOi, stateOnly(rs), pct(liveSecondPercent('CE','oi')));
  place(refs.ceVol, stateOnly(rs), pct(liveSecondPercent('CE','volume')));
  place(refs.strike, '●', priceText, 'marker-price');
  place(refs.peVol, stateOnly(ss), pct(liveSecondPercent('PE','volume')));
  place(refs.peOi, stateOnly(ss), pct(liveSecondPercent('PE','oi')));

  // Force a second alignment after browser layout settles, especially after
  // a hard mobile refresh/orientation change.
  requestAnimationFrame(() => {
    const marker = body.querySelector('tr.live-marker-row');
    if (!marker && retry < 12) positionATMMidChambers(p, retry + 1);
  });
  if (retry === 0) {
    setTimeout(() => {
      const marker = body.querySelector('tr.live-marker-row');
      if (!marker) positionATMMidChambers(p, 1);
    }, 120);
  }
}

function updateLivePriceMarker(price) {
  const p = Number(price || 0);
  if (!Number.isFinite(p) || p <= 0) return;
  positionATMMidChambers(p);
}

function renderChainWithLiveMarker(rows) {
  renderChain(rows || []);
  updateFloatingChambers(currentData || {rows:rows || []});
  const p=Number(currentData?.nifty?.ltp||0);
  if(p>0) updateLivePriceMarker(p);
}

function realignFloatingChambers(){
  const p=Number(currentData?.nifty?.ltp||0);
  if(p>0) requestAnimationFrame(()=>positionATMMidChambers(p));
}
window.addEventListener('resize',realignFloatingChambers,{passive:true});
window.addEventListener('orientationchange',realignFloatingChambers,{passive:true});
window.addEventListener('scroll',realignFloatingChambers,{passive:true});
const _floatingWrap=document.querySelector('.wrap');
if(_floatingWrap) _floatingWrap.addEventListener('scroll',realignFloatingChambers,{passive:true});
window.addEventListener('load',()=>{
  const p=Number(currentData?.nifty?.ltp||0);
  if(p>0) requestAnimationFrame(()=>positionATMMidChambers(p));
});


// ============================================================
// LIVE ANGEL ONE CANDLE DATA + TICK-BY-TICK CURRENT CANDLE
let candleIntervalName='FIVE_MINUTE';
let currentCandleCache=[];
let liveCandleLastTick=0;
let liveCandleDrawQueued=false;
let selectedCandleKey='';
let selectedCandleHideTimer=0;
// Interactive chart controls: zoom, horizontal pan and touch pinch zoom.
let candleZoom=1;
let candleZoomMin=0.55;
let candleZoomMax=4.0;
// Separate horizontal/time zoom and vertical/price zoom, like Angel One.
let candlePriceZoom=1;
let candlePriceZoomMin=0.65;
let candlePriceZoomMax=3.5;
let candlePricePan=0;
let candleLastRenderZoom=1;
let candleGesture=null;

function candleNum(v){
  const n=Number(v);
  return Number.isFinite(n)?n:0;
}
function candleFmt(v){
  return candleNum(v).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2});
}
function candleMinutes(name){
  const map={ONE_MINUTE:1,THREE_MINUTE:3,FIVE_MINUTE:5,TEN_MINUTE:10,
             FIFTEEN_MINUTE:15,THIRTY_MINUTE:30,ONE_HOUR:60};
  return map[name]||5;
}
function candleBucket(ms, minutes){
  const size=minutes*60*1000;
  const d=new Date(ms);
  // NSE candle buckets are anchored to 09:15 IST, not arbitrary UTC/browser
  // boundaries. This prevents the live candle's displayed time from jumping
  // when the browser/API supplies different timestamp formats.
  const anchor=new Date(d);
  anchor.setHours(9,15,0,0);
  return anchor.getTime()+Math.floor((ms-anchor.getTime())/size)*size;
}
function candleTimeMs(c){
  const raw=String(c?.time||'');
  const t=Date.parse(raw);
  return Number.isFinite(t)?t:NaN;
}
function candlePattern(data){
  if(!data || !data.length) return {name:'WAIT', cls:'neutral'};
  const c=data[data.length-1];
  const o=candleNum(c.open), h=candleNum(c.high), l=candleNum(c.low), cl=candleNum(c.close);
  if(!(h>0 && l>0 && Math.max(o,cl)>0)) return {name:'WAIT', cls:'neutral'};
  const range=Math.max(h-l,0.000001);
  const body=Math.abs(cl-o);
  const upper=h-Math.max(o,cl);
  const lower=Math.min(o,cl)-l;
  if(body <= range*0.10) return {name:'DOJI', cls:'neutral'};
  if(lower >= body*2 && upper <= body*0.8)
    return {name:cl>=o?'HAMMER':'HAMMER-LIKE', cls:'bull'};
  if(upper >= body*2 && lower <= body*0.8)
    return {name:cl<o?'SHOOTING STAR':'INVERTED HAMMER', cls:cl<o?'bear':'neutral'};
  if(data.length>=2){
    const p=data[data.length-2];
    const po=candleNum(p.open), pc=candleNum(p.close);
    if(pc<po && cl>o && o<=pc && cl>=po) return {name:'BULLISH ENGULFING', cls:'bull'};
    if(pc>po && cl<o && o>=pc && cl<=po) return {name:'BEARISH ENGULFING', cls:'bear'};
  }
  return {name:cl>=o?'BULLISH':'BEARISH', cls:cl>=o?'bull':'bear'};
}
function setCandlePattern(data){
  const p=candlePattern(data);
  const el=document.getElementById('candlePattern');
  if(el){
    el.textContent=p.name;
    el.classList.remove('bull','bear','neutral');
    el.classList.add(p.cls);
  }
}
function drawCandles(candles){
  // Stable Angel-One-style 5-minute chart renderer.
  // IMPORTANT: never blank an already rendered chart because one REST/tick
  // update is temporarily empty or malformed.
  const root=document.getElementById('candleCanvas');
  if(!root) return;

  const raw=Array.isArray(candles)?candles:[];
  const normalize=(c)=>{
    if(!c || typeof c!=='object') return null;
    const time=c.time ?? c.timestamp ?? c.datetime ?? c.date;
    const open=candleNum(c.open ?? c.o ?? c.Open);
    const high=candleNum(c.high ?? c.h ?? c.High);
    const low=candleNum(c.low ?? c.l ?? c.Low);
    const close=candleNum(c.close ?? c.c ?? c.Close ?? c.ltp);
    const volume=candleNum(c.volume ?? c.v ?? c.Volume);
    if(!time || !(open>0 || high>0 || low>0 || close>0)) return null;
    const h=Math.max(high,open,close,low);
    const l=Math.min(...[low,open,close].filter(v=>v>0));
    if(!(h>0 && l>0)) return null;
    return {time:String(time),open:open>0?open:close,high:h,low:l,close:close>0?close:open,volume:Math.max(0,volume),live:!!c.live};
  };
  const normalized=raw.map(normalize).filter(Boolean).sort((a,b)=>candleTimeMs(a)-candleTimeMs(b));
  const dedup=new Map();
  normalized.forEach(c=>dedup.set(String(c.time).slice(0,16),c));
  const data=Array.from(dedup.values()).sort((a,b)=>candleTimeMs(a)-candleTimeMs(b));

  // Do not replace good data with an empty frame.
  if(!data.length){
    if(currentCandleCache && currentCandleCache.length){
      requestAnimationFrame(()=>drawCandles(currentCandleCache));
    }else{
      root.innerHTML='<div style="height:100%;display:flex;align-items:center;justify-content:center;color:#7f8c99;font:700 12px sans-serif">NIFTY 50 • 5 MIN • WAITING FOR ANGEL ONE CANDLES</div>';
    }
    return;
  }
  currentCandleCache=data;

  const measuredW=Math.floor(root.getBoundingClientRect().width||root.clientWidth||0);
  if(measuredW<120){
    requestAnimationFrame(()=>drawCandles(currentCandleCache));
    return;
  }

  const oldScrollLeft=root.scrollLeft||0;
  const oldMax=Math.max(0,root.scrollWidth-root.clientWidth);
  const wasNearRight=(oldMax-oldScrollLeft)<42;
  const viewportW=Math.max(320,measuredW);
  const h=Math.max(250, Math.floor(root.clientHeight||250));
  const pad={l:8,r:58,t:22,b:25};
  const volumeH=Math.round(h*.16);
  const gap=7;
  const priceH=Math.max(100,h-pad.t-pad.b-volumeH-gap);
  const zoom=Math.max(candleZoomMin,Math.min(candleZoomMax,Number(candleZoom)||1));
  candleZoom=zoom;
  const priceZoom=Math.max(candlePriceZoomMin,Math.min(candlePriceZoomMax,Number(candlePriceZoom)||1));
  candlePriceZoom=priceZoom;
  const step=Math.max(10,14*zoom);
  const plotW=Math.max(viewportW-pad.l-pad.r,data.length*step);
  const chartW=pad.l+plotW+pad.r;
  const priceTop=pad.t;
  const priceBottom=priceTop+priceH;
  const volTop=priceBottom+gap;
  const volBottom=h-pad.b;
  const hi0=Math.max(...data.map(c=>c.high),...data.map(c=>c.close));
  const lo0=Math.min(...data.map(c=>c.low).filter(v=>v>0),...data.map(c=>c.close).filter(v=>v>0));
  const span=Math.max(hi0-lo0,1);
  const baseCenter=(hi0+lo0)/2;
  const visibleSpan=Math.max(1,span/priceZoom);
  // Vertical price-axis pan keeps the complete price range movable.
  const panLimit=Math.max(0,span*1.5);
  candlePricePan=Math.max(-panLimit,Math.min(panLimit,Number(candlePricePan)||0));
  const center=baseCenter+candlePricePan;
  const hi=center+visibleSpan/2;
  const lo=Math.max(0.01,center-visibleSpan/2);
  const py=v=>priceTop+(hi-v)/(hi-lo)*priceH;
  const bodyW=Math.max(4,Math.min(12,step*.62));
  const esc=v=>String(v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  const timeLabel=t=>{const m=String(t||'').match(/(\d{2}):(\d{2})/);return m?m[1]+':'+m[2]:String(t||'').slice(11,16);};
  const light=document.body.classList.contains('light-mode');
  const bg=light?'#ffffff':'#0b1118';
  const fg=light?'#27323c':'#d7e0e8';
  const muted=light?'#687581':'#7f8c99';
  const grid=light?'rgba(71,85,105,.16)':'rgba(148,163,184,.16)';
  const vgrid=light?'rgba(71,85,105,.10)':'rgba(148,163,184,.10)';
  const upColor=light?'#0a9f69':'#26d99a';
  const downColor=light?'#e33f4f':'#ff5b67';

  root.style.display='block';
  root.style.width='100%';
  root.style.height=h+'px';
  root.style.minHeight=h+'px';
  root.style.background=bg;
  root.style.overflowX='auto';
  root.style.overflowY='hidden';
  root.style.position='relative';
  root.style.contain='layout paint';
  root.style.scrollBehavior='auto';

  let svg=`<svg xmlns="http://www.w3.org/2000/svg" width="${chartW}" height="${h}" viewBox="0 0 ${chartW} ${h}" style="display:block;min-width:${chartW}px;background:${bg}" aria-label="NIFTY 50 5 minute candles">`;

  // Clean chart header; no separate O/H/L/C boxes.
  svg+=`<text x="${pad.l}" y="14" fill="${fg}" font-size="9" font-family="sans-serif" font-weight="800">NIFTY 50 • 5 MIN</text>`;
  const latest=data[data.length-1];
  const latestUp=latest.close>=latest.open;
  const latestColor=latestUp?upColor:downColor;
  svg+=`<text x="${Math.max(pad.l+105,chartW-pad.r-72)}" y="14" fill="${latestColor}" font-size="9" font-family="sans-serif" font-weight="900" text-anchor="end">${esc(candleFmt(latest.close))}</text>`;

  for(let i=0;i<=5;i++){
    const y=priceTop+i*priceH/5;
    const v=hi-(hi-lo)*i/5;
    svg+=`<line x1="${pad.l}" y1="${y.toFixed(2)}" x2="${(chartW-pad.r).toFixed(2)}" y2="${y.toFixed(2)}" stroke="${grid}" stroke-width="1"/>`;
    svg+=`<text x="${chartW-pad.r+5}" y="${(y+3).toFixed(2)}" fill="${muted}" font-size="8" font-family="sans-serif">${esc(v.toLocaleString('en-IN',{maximumFractionDigits:2}))}</text>`;
  }

  // Dedicated TradingView-style bottom TIME SCALE.  Keep this band separate
  // from the volume pane so the time labels are always visible and touchable.
  const timeAxisTop=volBottom+1;
  svg+=`<rect x="0" y="${volBottom.toFixed(2)}" width="${chartW}" height="${(h-volBottom).toFixed(2)}" fill="${bg}"/>`;
  svg+=`<line x1="${pad.l}" y1="${volBottom.toFixed(2)}" x2="${chartW-pad.r}" y2="${volBottom.toFixed(2)}" stroke="${grid}" stroke-width="1"/>`;

  const labelEvery=Math.max(1,Math.ceil(data.length/10));
  data.forEach((c,i)=>{
    const x=pad.l+step*i+step/2;
    if(i%labelEvery===0){
      svg+=`<line x1="${x.toFixed(2)}" y1="${priceTop}" x2="${x.toFixed(2)}" y2="${volBottom}" stroke="${vgrid}" stroke-width="1"/>`;
      svg+=`<line x1="${x.toFixed(2)}" y1="${volBottom.toFixed(2)}" x2="${x.toFixed(2)}" y2="${(volBottom+5).toFixed(2)}" stroke="${muted}" stroke-width="1"/>`;
      svg+=`<text x="${Math.max(pad.l,x-15).toFixed(2)}" y="${(h-7).toFixed(2)}" fill="${fg}" font-size="9" font-family="sans-serif" font-weight="700">${esc(timeLabel(c.time))}</text>`;
    }
  });

  const maxVol=Math.max(...data.map(c=>c.volume),1);
  svg+=`<line x1="${pad.l}" y1="${(volTop-3).toFixed(2)}" x2="${chartW-pad.r}" y2="${(volTop-3).toFixed(2)}" stroke="${grid}"/>`;
  data.forEach((c,i)=>{
    if(c.volume<=0)return;
    const x=pad.l+step*i+step/2;
    const vh=Math.max(1,(c.volume/maxVol)*(volBottom-volTop-2));
    const y=volBottom-vh;
    const color=c.close>=c.open?upColor:downColor;
    svg+=`<rect x="${(x-bodyW/2).toFixed(2)}" y="${y.toFixed(2)}" width="${bodyW.toFixed(2)}" height="${vh.toFixed(2)}" fill="${color}" opacity=".28"/>`;
  });

  // Candles: draw wick first, then body so every body remains visible.
  data.forEach((c,i)=>{
    const x=pad.l+step*i+step/2;
    const color=c.close>=c.open?upColor:downColor;
    const yH=py(c.high), yL=py(c.low);
    const yO=py(c.open), yC=py(c.close);
    const top=Math.min(yO,yC);
    const bodyH=Math.max(2.5,Math.abs(yC-yO));
    svg+=`<line x1="${x.toFixed(2)}" y1="${yH.toFixed(2)}" x2="${x.toFixed(2)}" y2="${yL.toFixed(2)}" stroke="${color}" stroke-width="1.5" vector-effect="non-scaling-stroke"/>`;
    svg+=`<rect x="${(x-bodyW/2).toFixed(2)}" y="${top.toFixed(2)}" width="${bodyW.toFixed(2)}" height="${bodyH.toFixed(2)}" rx="1" fill="${color}" stroke="${color}" stroke-width=".5"/>`;
  });

  const live=candleNum(latest.close);
  if(live>0){
    const y=py(live);
    svg+=`<line x1="${pad.l}" y1="${y.toFixed(2)}" x2="${chartW-pad.r}" y2="${y.toFixed(2)}" stroke="${latestColor}" stroke-width="1" stroke-dasharray="4 3" opacity=".8"/>`;
    const tagW=52,tagH=17,tx=chartW-pad.r+2,ty=Math.max(priceTop,Math.min(priceBottom-tagH,y-tagH/2));
    svg+=`<rect x="${tx}" y="${ty.toFixed(2)}" width="${tagW}" height="${tagH}" rx="3" fill="${latestColor}"/>`;
    svg+=`<text x="${tx+tagW/2}" y="${(ty+11.5).toFixed(2)}" text-anchor="middle" fill="#fff" font-size="8.5" font-family="sans-serif" font-weight="900">${esc(candleFmt(live))}</text>`;
  }

  const selectedIdx=data.findIndex(c=>String(c.time)===String(selectedCandleKey));
  if(selectedIdx>=0){
    const sc=data[selectedIdx], sx=pad.l+step*selectedIdx+step/2;
    svg+=`<line x1="${sx.toFixed(2)}" y1="${priceTop}" x2="${sx.toFixed(2)}" y2="${volBottom}" stroke="${light?'rgba(71,85,105,.65)':'rgba(225,232,238,.55)'}" stroke-width="1" opacity=".55"/>`;
    const boxW=Math.min(190,chartW-10),boxH=55;
    const bx=Math.max(4,Math.min(chartW-boxW-4,sx-boxW/2));
    const by=Math.max(18,Math.min(priceBottom-boxH,py(sc.high)-boxH-5));
    svg+=`<rect x="${bx}" y="${by}" width="${boxW}" height="${boxH}" rx="5" fill="${light?'#ffffff':'#18232e'}" stroke="${light?'#aab6c1':'#657482'}"/>`;
    svg+=`<text x="${bx+7}" y="${by+13}" fill="${fg}" font-size="8" font-family="sans-serif" font-weight="800">${esc(String(sc.time).replace('T',' ').replace('Z',''))}</text>`;
    svg+=`<text x="${bx+7}" y="${by+28}" fill="${fg}" font-size="8" font-family="sans-serif">H ${esc(candleFmt(sc.high))}  L ${esc(candleFmt(sc.low))}</text>`;
    svg+=`<text x="${bx+7}" y="${by+43}" fill="${fg}" font-size="8" font-family="sans-serif">O ${esc(candleFmt(sc.open))}  C ${esc(candleFmt(sc.close))}  V ${esc(candleNum(sc.volume).toLocaleString('en-IN'))}</text>`;
  }
  svg+='</svg>';
  root.innerHTML=svg;

  // TradingView-style chart controls:
  // 1) Drag the MAIN chart left/right = pan through candles.
  // 2) Drag the BOTTOM TIME SCALE left/right = horizontal zoom (bar spacing).
  // 3) Drag the PRICE SCALE up/down = vertical price zoom (candle height).
  // 4) Two-finger pinch = horizontal/time zoom.
  // 5) Wheel over an axis = zoom that axis; wheel over chart = scroll.
  // The axis gestures deliberately do NOT pan the chart.
  const chartSvg=root.querySelector('svg');
  if(chartSvg){
    chartSvg.style.display='block';
    chartSvg.style.visibility='visible';
    chartSvg.style.opacity='1';
    chartSvg.style.touchAction='none';
    chartSvg.style.overflow='visible';
    chartSvg.style.cursor='grab';

    const rect0=()=>chartSvg.getBoundingClientRect();
    const axisInfo=(ev)=>{
      const r=rect0();
      const x=ev.clientX-r.left;
      const y=ev.clientY-r.top;
      const priceAxisX=chartW-pad.r;
      // Price scale is the rightmost ~42px. Time scale is the bottom 25px
      // where the HH:MM labels are rendered.
      const price= x >= priceAxisX-42 && y >= pad.t && y <= priceBottom+4;
      const time = y >= volBottom-2 && y <= h;
      return {x,y,price,time};
    };

    let gesture=null;
    const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));

    const begin=(ev)=>{
      if(ev.target.closest && ev.target.closest('button')) return;
      const ax=axisInfo(ev);
      if(ev.pointerType==='touch' && ev.isPrimary===false) return;

      if(ax.price){
        ev.preventDefault();
        gesture={type:'price',startY:ev.clientY,startZoom:candlePriceZoom};
        root.style.cursor='ns-resize';
      }else if(ax.time){
        ev.preventDefault();
        gesture={type:'time',startX:ev.clientX,startZoom:candleZoom,startScroll:root.scrollLeft};
        root.style.cursor='ew-resize';
      }else{
        gesture={type:'pan',startX:ev.clientX,startScroll:root.scrollLeft,moved:false};
        root.style.cursor='grabbing';
      }
      try{chartSvg.setPointerCapture(ev.pointerId);}catch(_e){}
    };

    const move=(ev)=>{
      if(!gesture)return;
      if(gesture.type==='price'){
        ev.preventDefault();
        const dy=ev.clientY-gesture.startY;
        // TradingView-style: pull price scale upward => zoom in (larger
        // candles vertically); downward => zoom out. Keep the price under
        // the finger fixed while changing the visible price range.
        const oldZoom=candlePriceZoom;
        const newZoom=clamp(gesture.startZoom*Math.exp(-dy*0.012),candlePriceZoomMin,candlePriceZoomMax);
        if(Math.abs(newZoom-oldZoom)>0.0001){
          const baseHi=Math.max(...currentCandleCache.map(c=>c.high),...currentCandleCache.map(c=>c.close));
          const baseLo=Math.min(...currentCandleCache.map(c=>c.low).filter(v=>v>0),...currentCandleCache.map(c=>c.close).filter(v=>v>0));
          const baseSpan=Math.max(baseHi-baseLo,1);
          const oldSpan=Math.max(1,baseSpan/oldZoom);
          const oldCenter=(baseHi+baseLo)/2+(Number(candlePricePan)||0);
          const r=rect0();
          const localY=clamp(ev.clientY-r.top,pad.t,priceBottom);
          const ratio=(localY-pad.t)/Math.max(1,priceH);
          const anchoredPrice=oldCenter+oldSpan/2-ratio*oldSpan;
          const newSpan=Math.max(1,baseSpan/newZoom);
          const newCenter=anchoredPrice-(0.5-ratio)*newSpan;
          candlePriceZoom=newZoom;
          candlePricePan=clamp(newCenter-(baseHi+baseLo)/2,-baseSpan*1.5,baseSpan*1.5);
          drawCandles(currentCandleCache);
        }
        return;
      }
      if(gesture.type==='time'){
        ev.preventDefault();
        const dx=ev.clientX-gesture.startX;
        // Pull the time scale left => bars become wider; pull right => bars
        // become narrower. Preserve the candle under the drag point.
        const oldZoom=gesture.startZoom;
        const newZoom=clamp(oldZoom*Math.exp(-dx*0.008),candleZoomMin,candleZoomMax);
        if(Math.abs(newZoom-candleZoom)>0.0001){
          const r=rect0();
          const localX=ev.clientX-r.left;
          const contentX=localX+gesture.startScroll;
          const oldStep=Math.max(10,14*oldZoom);
          const newStep=Math.max(10,14*newZoom);
          const anchorIndex=(contentX-pad.l)/oldStep;
          candleZoom=newZoom;
          const desired=pad.l+anchorIndex*newStep-localX;
          // drawCandles preserves scroll by default; apply the new anchored
          // scroll immediately after rendering.
          drawCandles(currentCandleCache);
          requestAnimationFrame(()=>{
            const max=Math.max(0,root.scrollWidth-root.clientWidth);
            root.scrollLeft=clamp(desired,0,max);
          });
        }
        return;
      }

      const dx=ev.clientX-gesture.startX;
      if(Math.abs(dx)>4)gesture.moved=true;
      if(gesture.moved){
        ev.preventDefault();
        root.scrollLeft=clamp(gesture.startScroll-dx,0,Math.max(0,root.scrollWidth-root.clientWidth));
      }
    };

    const end=(ev)=>{
      if(!gesture)return;
      const wasPan=gesture.type==='pan' && gesture.moved;
      gesture=null;
      root.style.cursor='grab';
      try{chartSvg.releasePointerCapture(ev.pointerId);}catch(_e){}
      if(wasPan){
        chartSvg.dataset.justDragged='1';
        setTimeout(()=>delete chartSvg.dataset.justDragged,120);
      }
    };

    chartSvg.addEventListener('pointerdown',begin,{passive:false});
    chartSvg.addEventListener('pointermove',move,{passive:false});
    chartSvg.addEventListener('pointerup',end,{passive:false});
    chartSvg.addEventListener('pointercancel',end,{passive:false});

    // Native two-finger pinch controls horizontal/time zoom. The midpoint is
    // preserved so the candles do not jump away from the user's fingers.
    let pinch=null;
    const distance=(a,b)=>Math.hypot(a.clientX-b.clientX,a.clientY-b.clientY);
    const midpoint=(a,b)=>(a.clientX+b.clientX)/2;
    chartSvg.addEventListener('touchstart',(ev)=>{
      if(ev.touches.length===2){
        ev.preventDefault();
        const r=rect0();
        const mx=midpoint(ev.touches[0],ev.touches[1])-r.left;
        pinch={distance:distance(ev.touches[0],ev.touches[1]),zoom:candleZoom,scroll:root.scrollLeft,localX:mx};
      }
    },{passive:false});
    chartSvg.addEventListener('touchmove',(ev)=>{
      if(ev.touches.length===2 && pinch){
        ev.preventDefault();
        const d=distance(ev.touches[0],ev.touches[1]);
        const newZoom=clamp(pinch.zoom*(d/Math.max(1,pinch.distance)),candleZoomMin,candleZoomMax);
        const oldStep=Math.max(10,14*pinch.zoom);
        const newStep=Math.max(10,14*newZoom);
        const anchorIndex=(pinch.localX+pinch.scroll-pad.l)/oldStep;
        candleZoom=newZoom;
        drawCandles(currentCandleCache);
        requestAnimationFrame(()=>{
          const desired=pad.l+anchorIndex*newStep-pinch.localX;
          root.scrollLeft=clamp(desired,0,Math.max(0,root.scrollWidth-root.clientWidth));
        });
      }
    },{passive:false});
    chartSvg.addEventListener('touchend',(ev)=>{if(ev.touches.length<2)pinch=null;},{passive:false});

    chartSvg.addEventListener('wheel',(ev)=>{
      const ax=axisInfo(ev);
      if(ax.price){
        ev.preventDefault();
        const oldZoom=candlePriceZoom;
        const newZoom=clamp(oldZoom*(ev.deltaY<0?1.08:.926),candlePriceZoomMin,candlePriceZoomMax);
        candlePriceZoom=newZoom;
        drawCandles(currentCandleCache);
      }else if(ax.time){
        ev.preventDefault();
        const r=rect0();
        const localX=ev.clientX-r.left;
        const oldZoom=candleZoom;
        const newZoom=clamp(oldZoom*(ev.deltaX<0||ev.deltaY<0?1.10:.91),candleZoomMin,candleZoomMax);
        const oldStep=Math.max(10,14*oldZoom),newStep=Math.max(10,14*newZoom);
        const anchorIndex=(localX+root.scrollLeft-pad.l)/oldStep;
        candleZoom=newZoom;
        drawCandles(currentCandleCache);
        requestAnimationFrame(()=>{
          const desired=pad.l+anchorIndex*newStep-localX;
          root.scrollLeft=clamp(desired,0,Math.max(0,root.scrollWidth-root.clientWidth));
        });
      }else if(ev.ctrlKey||ev.metaKey){
        ev.preventDefault();
        const r=rect0();
        const localX=ev.clientX-r.left;
        const oldZoom=candleZoom;
        const newZoom=clamp(oldZoom*(ev.deltaY<0?1.12:.893),candleZoomMin,candleZoomMax);
        const oldStep=Math.max(10,14*oldZoom),newStep=Math.max(10,14*newZoom);
        const anchorIndex=(localX+root.scrollLeft-pad.l)/oldStep;
        candleZoom=newZoom;
        drawCandles(currentCandleCache);
        requestAnimationFrame(()=>{
          const desired=pad.l+anchorIndex*newStep-localX;
          root.scrollLeft=clamp(desired,0,Math.max(0,root.scrollWidth-root.clientWidth));
        });
      }else{
        ev.preventDefault();
        const dx=Math.abs(ev.deltaX)>Math.abs(ev.deltaY)?ev.deltaX:ev.deltaY;
        root.scrollLeft=clamp(root.scrollLeft+dx,0,Math.max(0,root.scrollWidth-root.clientWidth));
      }
    },{passive:false});

    const pickCandle=(ev)=>{
      if(chartSvg.dataset.justDragged==='1')return;
      ev.preventDefault();
      const r=rect0();
      const localX=(ev.clientX??0)-r.left+root.scrollLeft;
      const idx=Math.max(0,Math.min(data.length-1,Math.floor((localX-pad.l)/step)));
      const picked=data[idx];
      if(!picked)return;
      const key=String(picked.time||idx);
      selectedCandleKey=(selectedCandleKey===key)?'':key;
      drawCandles(currentCandleCache);
    };
    chartSvg.addEventListener('click',pickCandle,{passive:false});
  }

}

function queueLiveCandleDraw(){
  if(liveCandleDrawQueued)return;
  liveCandleDrawQueued=true;
  requestAnimationFrame(()=>{
    liveCandleDrawQueued=false;
    if(document.getElementById('candleToggle')?.checked) drawCandles(currentCandleCache);
  });
}
function updateLiveCandleFromTick(price, tickMs, tickVolume){
  const p=Number(price||0);
  if(!Number.isFinite(p)||p<=0)return;
  let nowMs=Number.isFinite(Number(tickMs))&&Number(tickMs)>0?Number(tickMs):Date.now();
  // Never let an invalid/future exchange timestamp freeze the live candle.
  // If Angel sends seconds, normalize it here too; if it is wildly outside
  // the browser clock, use the local arrival time for bucket selection.
  if(nowMs < 1_000_000_000_000) nowMs*=1000;
  const browserNow=Date.now();
  if(!Number.isFinite(nowMs) || Math.abs(nowMs-browserNow)>10*60*1000) nowMs=browserNow;
  const mins=candleMinutes(candleIntervalName);
  const bucket=candleBucket(nowMs,mins);
  let last=currentCandleCache.length?currentCandleCache[currentCandleCache.length-1]:null;
  let lastMs=last?candleTimeMs(last):NaN;
  const lastBucket=Number.isFinite(lastMs)?candleBucket(lastMs,mins):-1;

  if(!last || lastBucket!==bucket){
    last={time:new Date(bucket).toLocaleString('sv-SE',{hour12:false}).slice(0,16),open:p,high:p,low:p,close:p,
          volume:Number(tickVolume||0),live:true};
    currentCandleCache=[...(currentCandleCache||[]),last];
  }else{
    last.high=Math.max(candleNum(last.high),p);
    last.low=Math.min(candleNum(last.low),p);
    last.close=p;
    if(Number(tickVolume)>=0) last.volume=Math.max(candleNum(last.volume),Number(tickVolume));
    last.live=true;
  }
  liveCandleLastTick=Date.now();
  const set=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v;};
  set('candleClose',candleFmt(last.close));
  set('candleHigh',candleFmt(last.high));
  set('candleLow',candleFmt(last.low));
  set('candleVolume',candleNum(last.volume).toLocaleString('en-IN'));
  queueLiveCandleDraw();
}
async function loadLiveCandles(){
  candleIntervalName=document.getElementById('candleInterval')?.value||'FIVE_MINUTE';
  try{
    const r=await fetch('/api/candles?interval='+encodeURIComponent(candleIntervalName),{cache:'no-store'});
    if(!r.ok) throw new Error('HTTP '+r.status);
    const d=await r.json();
    let incoming=Array.isArray(d.candles)?d.candles:[];
    // Do not retry the Angel historical endpoint from the browser. The server
    // deliberately makes at most one historical request per session/interval.
    // A retry loop here was multiplying access and triggering Angel rate limits.
    // Never shrink the chart on refresh.  Merge the new server response with
    // the candles already loaded in this page; Angel may temporarily return
    // a shorter/partial history because of a rate-limit or gateway response.
    if(!incoming.length && currentCandleCache.length) return;
    const oldByTime=new Map((currentCandleCache||[]).filter(c=>c&&c.time).map(c=>[String(c.time).slice(0,16),c]));
    incoming.forEach(c=>{ if(c&&c.time) oldByTime.set(String(c.time).slice(0,16),c); });
    incoming=Array.from(oldByTime.values()).sort((a,b)=>candleTimeMs(a)-candleTimeMs(b));
    const localLast=currentCandleCache?.length?currentCandleCache[currentCandleCache.length-1]:null;
    const serverLast=incoming.length?incoming[incoming.length-1]:null;

    // Merge the live websocket candle into Angel's historical session data.
    // Never throw away the live candle merely because REST has not closed it yet.
    if(localLast?.live){
      const lb=candleBucket(candleTimeMs(localLast),candleMinutes(candleIntervalName));
      const idx=incoming.findIndex(c=>candleBucket(candleTimeMs(c),candleMinutes(candleIntervalName))===lb);
      if(idx>=0){
        const c=incoming[idx];
        c.open=candleNum(c.open)||candleNum(localLast.open);
        c.high=Math.max(candleNum(c.high),candleNum(localLast.high));
        c.low=Math.min(candleNum(c.low),candleNum(localLast.low));
        c.close=candleNum(localLast.close)||candleNum(c.close);
        c.volume=Math.max(candleNum(c.volume),candleNum(localLast.volume));
        c.live=true;
      }else if(!serverLast || lb>=candleBucket(candleTimeMs(serverLast),candleMinutes(candleIntervalName))){
        incoming.push(localLast);
      }
    }

    incoming.sort((a,b)=>candleTimeMs(a)-candleTimeMs(b));
    // De-duplicate by candle timestamp.
    const map=new Map();
    incoming.forEach(c=>map.set(String(c.time),c));
    currentCandleCache=Array.from(map.values());
    drawCandles(currentCandleCache);

    const c=currentCandleCache[currentCandleCache.length-1]||d.last_candle||{};
    const set=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v===''||v==null?'--':v;};
    set('candleOpen',candleFmt(c.open)); set('candleHigh',candleFmt(c.high));
    set('candleLow',candleFmt(c.low)); set('candleClose',candleFmt(c.close));
    set('candleVolume',candleNum(c.volume).toLocaleString('en-IN'));
    set('candleCOA',d.coa||'WAIT');
    set('candleScenario',d.scenario?.name?((d.scenario.number||'')+' • '+d.scenario.name):'WAIT');
    const meta=document.getElementById('candleMeta');
    if(meta){
      const expected=Number(d.expected_count||0);
      meta.textContent=String(d.exchange||'NSE')+' • '+String(d.symbol||'NIFTY')+
        ' • '+String(candleIntervalName)+' • ANGEL ONE DIRECT • '+currentCandleCache.length+
        (expected?' / '+expected:'')+' CANDLES';
      if(c.time)meta.textContent+=' • '+c.time;
    }
  }catch(e){
    const m=document.getElementById('candleMeta');
    if(m)m.textContent='Angel candle retrying: '+e.message;
    if(!currentCandleCache.length){
      setTimeout(()=>{ if(document.getElementById('candleToggle')?.checked) loadLiveCandles(); }, 1800);
    }
  }
}

function toggleCandleChart(enabled){
  const body=document.getElementById('candleBody');
  const label=document.getElementById('candleToggleLabel');
  if(body) body.style.display=enabled?'':'none';
  if(label) label.textContent=enabled?'ON':'OFF';
  if(enabled){
    // Load today's historical 5-minute candles once.
    // The current candle is then maintained only from Angel One ticks.
    loadLiveCandles();
  }
}
function startLiveCandles(){
  const toggle=document.getElementById('candleToggle');
  toggleCandleChart(toggle ? toggle.checked : true);
}
window.addEventListener('resize',()=>{if(currentCandleCache?.length && document.getElementById('candleToggle')?.checked)drawCandles(currentCandleCache);});

function toggleCandleSettings(){
  const p=document.getElementById('candleSettingsPanel');
  if(!p)return;
  p.hidden=!p.hidden;
}
function toggleTheme(){
  const light=!document.body.classList.contains('light-mode');
  document.body.classList.toggle('light-mode',light);
  localStorage.setItem('ltpTheme',light?'light':'dark');
  const b=document.getElementById('themeToggle');
  if(b)b.textContent=light?'WHITE / DARK':'DARK / WHITE';
}
function restoreTheme(){
  const t=localStorage.getItem('ltpTheme')||'dark';
  document.body.classList.toggle('light-mode',t==='light');
  const b=document.getElementById('themeToggle');
  if(b)b.textContent=t==='light'?'WHITE / DARK':'DARK / WHITE';
}
document.addEventListener('click',e=>{
  const panel=document.getElementById('candleSettingsPanel');
  const btn=document.getElementById('candleSettingsBtn');
  if(panel && !panel.hidden && !panel.contains(e.target) && e.target!==btn) panel.hidden=true;
});
restoreTheme();

startLiveCandles();
// Startup safety: mobile browsers may report zero chart width during the first
// layout pass. Retry without replacing valid candle data.
[250,700,1400,2500].forEach(ms=>setTimeout(()=>{
  if(document.getElementById('candleToggle')?.checked){
    if(currentCandleCache.length) drawCandles(currentCandleCache);
    else loadLiveCandles();
  }
},ms));
</script>
</body>

</html>
"""


# ============================================================
# HEALTH / BROWSER CONNECTIVITY
# ============================================================

@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "server": "LTP Master",
        "websocket": state.get("websocket", "UNKNOWN"),
        "raw_ws_messages": safe_int(state.get("raw_ws_messages", 0)),
        "last_error": state.get("last_error", ""),
        "time": now_string(),
    })

@app.after_request
def add_no_cache_headers(response):
    if request.path.startswith("/api/") or request.path == "/health":
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response



# ============================================================
# RENDER-SIDE CLOUD RECORDER
# ============================================================
def _cloud_recorder_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": "Bearer " + SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def _cloud_recorder_post(table, rows, on_conflict=None):
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY or not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = _cloud_recorder_headers()
    if on_conflict:
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"
        url += "?on_conflict=" + on_conflict
    response = requests.post(
        url, headers=headers, json=rows, timeout=(8, 20)
    )
    if response.status_code >= 300:
        raise RuntimeError(
            f"Supabase {table}: HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )


def load_last_ltp_from_supabase():
    """Restore the most recent saved NIFTY quote after a Render restart.

    Render's local filesystem is ephemeral, so LAST_LTP_CACHE alone cannot
    guarantee a last-traded price outside market hours.  The cloud snapshot
    table is the durable fallback.  This function is intentionally used only
    while the exchange is closed; it never overwrites a live-session quote.
    """
    if is_market_open():
        return False
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        print("SUPABASE LTP RESTORE SKIPPED: credentials not configured.", flush=True)
        return False

    try:
        url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_SNAPSHOT_TABLE}"
        # Search recent records for the newest VALID quote. The recorder can
        # also write zero-valued diagnostic snapshots while Angel's stream is
        # disconnected; those must not hide an older valid market snapshot.
        params = {
            "select": "id,snapshot_time,nifty,last_update",
            "order": "snapshot_time.desc,id.desc",
            "limit": "500",
        }
        response = requests.get(
            url, headers=_cloud_recorder_headers(), params=params,
            timeout=(5, 20),
        )
        if response.status_code >= 300:
            raise RuntimeError(
                f"HTTP {response.status_code}: {response.text[:400]}"
            )
        records = response.json() or []
        if not records:
            print("SUPABASE LTP RESTORE: no saved snapshots found.", flush=True)
            return False

        record = None
        for candidate in records:
            candidate_quote = candidate.get("nifty") or {}
            if not isinstance(candidate_quote, dict):
                continue
            candidate_ltp = safe_float(candidate_quote.get("ltp"))
            candidate_close = safe_float(candidate_quote.get("close"))
            if candidate_ltp > 0 or candidate_close > 0:
                record = candidate
                break
        if record is None:
            print("SUPABASE LTP RESTORE: no snapshot contains a positive LTP/close.", flush=True)
            return False

        # Fetch the option-chain JSON only for the selected valid snapshot,
        # avoiding a large response containing hundreds of historical chains.
        record_id = record.get("id")
        if record_id is not None:
            rows_response = requests.get(
                url,
                headers=_cloud_recorder_headers(),
                params={"select": "rows,expiry", "id": f"eq.{record_id}", "limit": "1"},
                timeout=(5, 15),
            )
            if rows_response.status_code >= 300:
                raise RuntimeError(
                    f"Supabase option snapshot HTTP {rows_response.status_code}: "
                    f"{rows_response.text[:300]}"
                )
            row_records = rows_response.json() or []
            if row_records:
                record["rows"] = row_records[0].get("rows") or []
                record["expiry"] = row_records[0].get("expiry")

        quote = record.get("nifty") or {}
        if not isinstance(quote, dict):
            quote = {}
        ltp = safe_float(quote.get("ltp"))
        close = safe_float(quote.get("close"))
        # Prefer the last actual trade; use the broker close only if no LTP
        # was captured in the selected snapshot.
        restored_price = ltp if ltp > 0 else close
        if restored_price <= 0:
            print("SUPABASE LTP RESTORE: selected snapshot has no positive LTP/close.", flush=True)
            return False

        stamp = str(
            quote.get("ltp_updated") or quote.get("time") or
            record.get("last_update") or record.get("snapshot_time") or ""
        )
        restored_option_count = 0
        saved_rows = record.get("rows") or []
        with data_lock:
            # Cloud is the persistent latest snapshot and therefore supersedes
            # any older local cache after a container restart.
            nifty_data["ltp"] = restored_price
            if close > 0:
                nifty_data["close"] = close
            for field in ("open", "high", "low", "change", "percent_change"):
                value = safe_float(quote.get(field))
                if value or field in ("change", "percent_change"):
                    nifty_data[field] = value
            nifty_data["time"] = str(quote.get("time") or stamp)
            nifty_data["ltp_source"] = "LAST TRADED (SUPABASE)"
            nifty_data["ltp_updated"] = stamp

            # Restore the last saved option-chain values too. These are
            # historical snapshot values, not falsely labelled live ticks.
            for saved_row in saved_rows:
                if not isinstance(saved_row, dict):
                    continue
                for side in ("ce", "pe"):
                    option = saved_row.get(side) or {}
                    if not isinstance(option, dict):
                        continue
                    symbol = str(option.get("symbol") or "").strip()
                    if not symbol:
                        continue
                    option_ltp = safe_float(option.get("ltp"))
                    option_close = safe_float(option.get("close"))
                    option_price = option_ltp if option_ltp > 0 else option_close
                    target = ensure_live_symbol(symbol)
                    if option_price > 0:
                        target["ltp"] = option_price
                    if option_close > 0:
                        target["close"] = option_close
                    target["volume"] = safe_int(option.get("volume"))
                    target["oi"] = safe_int(option.get("oi"))
                    target["oi_change"] = safe_int(option.get("oi_change"))
                    target["percent_change"] = safe_float(option.get("percent_change"))
                    target["ltp_source"] = "LAST SNAPSHOT (SUPABASE)"
                    target["ltp_updated"] = stamp
                    target["time"] = stamp
                    restored_option_count += 1

        save_ltp_cache()
        print(
            "SUPABASE LAST TRADED NIFTY LTP RESTORED:",
            f"{restored_price:.2f}", stamp,
            "| OPTION SNAPSHOT ROWS:", restored_option_count,
            flush=True,
        )
        return True
    except Exception as restore_error:
        print("SUPABASE LTP RESTORE ERROR:", repr(restore_error), flush=True)
        return False


def _cloud_event_key(date_s, side, event):
    payload = {
        "date": date_s,
        "side": side,
        "time": event.get("time"),
        "status": event.get("status"),
        "direction": event.get("direction"),
        "source": event.get("source"),
        "from": event.get("from"),
        "to": event.get("to"),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _cloud_recorder_save_payload(payload):
    global cloud_recorder_seen_events

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        return

    now = now_ist()
    date_s = now.strftime("%Y-%m-%d")
    analysis = payload.get("level_analysis") or {}
    history = analysis.get("history") or {}

    shift_rows = []
    for side in ("call", "put"):
        for event in history.get(side, []) or []:
            if not isinstance(event, dict):
                continue
            key = _cloud_event_key(date_s, side, event)
            # Cloud ENTRY rule: only completed SHIFT events are persisted.
            if str(event.get("status") or "").upper() != "SHIFT":
                continue
            if key in cloud_recorder_seen_events:
                continue
            shift_rows.append({
                "event_key": key,
                "event_date": date_s,
                "event_time": event.get("time"),
                "side": side,
                "status": event.get("status"),
                "direction": event.get("direction"),
                "source": event.get("source"),
                "from_level": event.get("from"),
                "to_level": event.get("to"),
                "changed": bool(event.get("changed")),
                "expiry": payload.get("expiry"),
                "raw_event": event,
            })

    # Persist only actual SHIFT events in the history table, but always
    # persist the latest market snapshot. This preserves last-known data even
    # during quiet periods when no new shift event is generated.
    if shift_rows:
        _cloud_recorder_post(
            SUPABASE_SHIFT_TABLE, shift_rows, "event_key"
        )
        for row in shift_rows:
            cloud_recorder_seen_events.add(row["event_key"])

    snapshot = {
        "snapshot_time": now.isoformat(),
        "event_date": date_s,
        "expiry": payload.get("expiry"),
        "market_open": payload.get("market_open"),
        "nifty": payload.get("nifty") or {},
        "atm": payload.get("atm"),
        "support": payload.get("support"),
        "resistance": payload.get("resistance"),
        "level_analysis": analysis,
        "rows": payload.get("rows") or [],
        "websocket": payload.get("websocket"),
        "last_update": payload.get("last_update"),
        "greeks_update": payload.get("greeks_update"),
        "greeks_count": payload.get("greeks_count", 0),
        "tick_count": payload.get("tick_count", 0),
        "raw_ws_messages": payload.get("raw_ws_messages", 0),
        "last_nifty_tick_at": payload.get("last_nifty_tick_at", 0),
        "last_option_tick_at": payload.get("last_option_tick_at", 0),
    }
    _cloud_recorder_post(SUPABASE_SNAPSHOT_TABLE, [snapshot])


def _cloud_recorder_loop():
    global cloud_recorder_running
    close_snapshot_saved_date = ""

    with cloud_recorder_lock:
        cloud_recorder_running = True

    try:
        while not cloud_recorder_stop.is_set():
            now = now_ist()

            # Capture one final end-of-session snapshot from today's last
            # received Angel tick. Never write an empty post-restart value.
            if now.weekday() < 5 and now.time() > datetime.strptime("15:30", "%H:%M").time():
                date_s = now.strftime("%Y-%m-%d")
                last_tick = safe_float(state.get("last_nifty_tick_at", 0))
                last_price = safe_float(nifty_data.get("ltp"))
                tick_is_today = False
                if last_tick > 0:
                    try:
                        tick_is_today = datetime.fromtimestamp(last_tick, IST).date() == now.date()
                    except Exception:
                        tick_is_today = False
                tick_is_fresh = last_tick > 0 and (time.time() - last_tick) <= 600
                if (close_snapshot_saved_date != date_s and tick_is_today
                        and tick_is_fresh and last_price > 0):
                    try:
                        final_payload = make_payload()
                        _cloud_recorder_save_payload(final_payload)
                        close_snapshot_saved_date = date_s
                        print("CLOUD RECORDER FINAL CLOSE SNAPSHOT SAVED:", date_s,
                              f"NIFTY={last_price:.2f}", flush=True)
                    except Exception as close_error:
                        print("CLOUD RECORDER FINAL CLOSE SNAPSHOT WARNING:",
                              repr(close_error), flush=True)
                cloud_recorder_stop.wait(30)
                continue

            if now.weekday() >= 5:
                cloud_recorder_stop.wait(30)
                continue

            if now.time() < datetime.strptime("09:15", "%H:%M").time():
                cloud_recorder_stop.wait(15)
                continue

            try:
                payload = make_payload()
                _cloud_recorder_save_payload(payload)
                print(
                    "CLOUD RECORDER SAVED:",
                    now.strftime("%Y-%m-%d %H:%M:%S"),
                    flush=True,
                )
            except Exception as recorder_error:
                print(
                    "CLOUD RECORDER WARNING:",
                    repr(recorder_error),
                    flush=True,
                )

            cloud_recorder_stop.wait(CLOUD_RECORDER_INTERVAL)
    finally:
        with cloud_recorder_lock:
            cloud_recorder_running = False


def start_cloud_recorder():
    global cloud_recorder_thread, cloud_recorder_stop, cloud_recorder_run_id

    if not CLOUD_RECORDER_ENABLED:
        print("Cloud recorder disabled by CLOUD_RECORDER_ENABLED.", flush=True)
        return False

    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        print(
            "Cloud recorder disabled: SUPABASE_URL / "
            "SUPABASE_SERVICE_ROLE_KEY is missing.",
            flush=True,
        )
        return False

    with cloud_recorder_lock:
        if cloud_recorder_thread and cloud_recorder_thread.is_alive():
            return True
        cloud_recorder_stop = threading.Event()
        cloud_recorder_seen_events.clear()
        cloud_recorder_run_id = (
            now_ist().strftime("%Y%m%dT%H%M%S%z") + "-render"
        )
        cloud_recorder_thread = threading.Thread(
            target=_cloud_recorder_loop,
            daemon=True,
            name="render-cloud-recorder",
        )
        cloud_recorder_thread.start()

    print(
        "Render-side cloud recorder started:",
        cloud_recorder_run_id,
        flush=True,
    )
    return True


def stop_cloud_recorder():
    cloud_recorder_stop.set()
    return True


@app.get("/api/recorder/status")
def api_recorder_status():
    return jsonify({
        "enabled": CLOUD_RECORDER_ENABLED,
        "running": bool(
            cloud_recorder_thread and cloud_recorder_thread.is_alive()
        ),
        "run_id": cloud_recorder_run_id,
        "interval_seconds": CLOUD_RECORDER_INTERVAL,
        "market_session": "09:15-15:30 IST",
        "supabase_configured": bool(
            SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY
        ),
        # This count is deliberately SHIFT-only; non-shift snapshots are not entries.
        "shift_entry_total": len(cloud_recorder_seen_events),
        "seen_events": len(cloud_recorder_seen_events),
    })


@app.post("/api/recorder/start")
def api_recorder_start():
    if not CLOUD_RECORDER_MANUAL:
        return jsonify({"ok": False, "error": "Manual recorder control disabled"}), 403
    ok = start_cloud_recorder()
    return jsonify({"ok": ok})


@app.post("/api/recorder/stop")
def api_recorder_stop():
    if not CLOUD_RECORDER_MANUAL:
        return jsonify({"ok": False, "error": "Manual recorder control disabled"}), 403
    stop_cloud_recorder()
    return jsonify({"ok": True, "stopped": True})


# ============================================================
# HOME
# ============================================================

@app.get("/")
def home():

    return render_template_string(
        HTML
    )


# ============================================================
# STARTUP
# ============================================================

def startup():

    global ws_thread
    global greeks_thread
    global rest_thread
    global selected_expiry

    state[
        "server_start"
    ] = now_string()

    print("")

    print(
        "========================================"
    )

    print(
        "NIFTY 50 ANGEL ONE LIVE SERVER"
    )

    print(
        "========================================"
    )

    print(
        "Trading:",
        (
            "ENABLED"
            if ENABLE_TRADING
            else
            "DISABLED"
        )
    )

    print(
        "Port:",
        PORT
    )

    # --------------------------------------------------------
    # Restore cache first.
    # --------------------------------------------------------

    # Do not seed live market data from an old cache during market hours.
    # During closed hours it remains useful as the requested last-known fallback.
    if not is_market_open():
        load_ltp_cache()
        # Prefer the persistent cloud snapshot over Render's ephemeral local
        # cache so the previous trading session's LTP survives restarts.
        load_last_ltp_from_supabase()

    # --------------------------------------------------------
    # Login.
    # --------------------------------------------------------

    if not angel_login():

        print(
            "WARNING: Angel One login failed."
        )

    # --------------------------------------------------------
    # Master.
    # --------------------------------------------------------

    try:

        load_nifty_options()

        selected_expiry = (
            choose_expiry()
        )

        print(
            "Selected expiry:",
            selected_expiry
        )

        load_baseline()
        load_shift_history()

        # Build the visible option rows from the Angel instrument master only.
        # Live prices/OI/volume are filled exclusively by SmartWebSocketV2 ticks.
        ensure_visible_chain_symbols()

        # One-time Angel REST seed only: populate the screen immediately while
        # Smart Stream connects. After startup, market values are tick-only.
        if state.get("logged_in"):
            try:
                fetch_nifty_rest()
                seed_options_rest()
                # Match Angel One's intraday OI-change reference to the
                # opening 09:15 OI, not to Render's startup/reconnect time.
                seed_opening_oi_baseline_from_angel()
            except Exception as seed_error:
                print("Initial Angel REST seed warning:", repr(seed_error), flush=True)

        # Fetch official Angel Greeks once for the selected expiry.  Greeks are
        # not part of Smart Stream ticks, so there is intentionally no 5-second
        # Greeks polling loop.
        if state.get("logged_in"):
            fetch_option_greeks()

    except Exception as e:

        state[
            "last_error"
        ] = str(e)

        print(
            "Startup market-data error:",
            e
        )

    # --------------------------------------------------------
    # NO periodic market-data polling.
    # Live NIFTY/options come only from Angel Smart Stream.
    # --------------------------------------------------------

    # --------------------------------------------------------
    # Derived live state runs separately from the Angel callback.
    # --------------------------------------------------------
    derived_thread = threading.Thread(
        target=derived_live_refresh_loop,
        daemon=True,
        name="derived-live-refresh"
    )
    derived_thread.start()

    # --------------------------------------------------------
    # WebSocket.
    # --------------------------------------------------------

    state["websocket"] = "CONNECTING"
    ws_thread = threading.Thread(
        target=start_websocket,
        daemon=True,
        name="angel-websocket"
    )

    ws_thread.start()

    watchdog_thread = threading.Thread(
        target=websocket_watchdog_loop,
        daemon=True,
        name="angel-websocket-watchdog"
    )
    watchdog_thread.start()

    # Greeks are REST snapshots rather than Smart Stream ticks. Refresh them
    # once per minute while the market is open so the displayed Angel Greeks
    # timestamp represents a real Angel refresh, not a fake browser clock.
    def _greeks_refresh_loop():
        while True:
            try:
                time.sleep(max(3, int(GREEKS_REFRESH_SECONDS or 3)))
                if state.get("logged_in") and is_market_open():
                    fetch_option_greeks()
            except Exception as greeks_loop_error:
                print("Greeks refresh warning:", repr(greeks_loop_error), flush=True)

    greeks_refresh_thread = threading.Thread(
        target=_greeks_refresh_loop,
        daemon=True,
        name="angel-greeks-refresh"
    )
    greeks_refresh_thread.start()

    # Recorder now runs inside Render; GitLab is no longer required.
    start_cloud_recorder()

    print("")

    print(
        "Server startup complete."
    )

    print(
        "Open in browser: http://127.0.0.1:" + str(PORT)
    )
    print(
        "Health check: http://127.0.0.1:" + str(PORT) + "/health"
    )

    print("")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    startup()

    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        threaded=True,
        use_reloader=False
    )
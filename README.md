# NIFTY LTP Shift Recorder — Free-first Render package

এই ZIP-এ আপনার মূল Angel One engine রাখা হয়েছে। Render চালানোর entry point,
health check, automatic market-time recorder এবং Supabase cloud snapshot persistence
কনফিগার করা আছে।

## কী স্বয়ংক্রিয় হবে
- সার্ভার চালু হলে engine নিজে startup/login চেষ্টা করবে।
- Angel One Smart WebSocket নিজে reconnect করার চেষ্টা করবে।
- সোমবার–শুক্রবার 09:15–15:30 IST সময়ের মধ্যে cloud recorder প্রতি 60 সেকেন্ডে
  snapshot পাঠাবে; CALL/PUT-এর প্রকৃত SHIFT হলে history row-ও সংরক্ষণ করবে।
- 15:30-এর পরে recorder আর snapshot নেবে না; পরের দিনে একই সময়সীমায় আবার চালু হবে।
- `/health` endpoint Render health check এবং UptimeRobot monitor-এর জন্য আছে।
- Gunicorn এক worker-এ চলবে, কারণ live WebSocket/state এক process-এ থাকা জরুরি।

## একবারের সেটআপ
1. ZIP extract করুন; ভেতরের `Prosenjit-main` ফোল্ডারের সব ফাইল GitHub/GitLab repo-র root-এ push করুন।
2. Supabase-এ project তৈরি করে SQL Editor-এ `supabase_schema.sql` সম্পূর্ণ Run করুন।
3. Render → New → Blueprint → আপনার Git repo connect → `render.yaml` Apply করুন।
4. Render service → Environment-এ এই 6টি মান দিন:
   - `ANGEL_API_KEY`
   - `ANGEL_CLIENT_CODE`
   - `ANGEL_MPIN`
   - `ANGEL_TOTP_SECRET`
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
5. Save/Deploy করুন। Logs-এ Angel One login success, instrument master এবং WebSocket CONNECTED দেখুন।
6. `https://YOUR-SERVICE.onrender.com/health` খুলে JSON-এ `"ok": true` দেখুন।

## UptimeRobot Keep-Alive
1. UptimeRobot Free account-এ Add New Monitor করুন।
2. Type: HTTP(s); Friendly Name: `NIFTY-LTP-KEEP-ALIVE`
3. URL: `https://YOUR-SERVICE.onrender.com/health`
4. Interval: 5 minutes (Free account-এ option থাকলে)।
5. Save করে monitor-টি UP দেখুন।

## গুরুত্বপূর্ণ বাস্তব সীমাবদ্ধতা
- Render Free-তে sleep/restart পুরোপুরি বন্ধ করার কোনো supported guarantee নেই।
  UptimeRobot-এর ping idle sleep কমাতে/এড়াতে পারে, কিন্তু Render-এর restart, deploy,
  outage বা network interruption আটকাতে পারে না। তাই "কখনো sleep হবে না" বলা যাবে না।
- Render Free-তে প্রতিটি market tick 100% capture হওয়ার নিশ্চয়তা নেই।
- Cloud snapshot/history Supabase-এ থাকে; কিন্তু engine-এর সম্পূর্ণ UI-তে পুরোনো দিনের
  cloud history restore/display workflow এখনো যুক্ত নয়। Snapshot হলো প্রতি মিনিটের
  capture, প্রতিটি tick নয়।
- সপ্তাহের দিন দেখে market window নির্ধারিত হয়। NSE special holiday/holiday calendar
  automatic verify করা হয়নি; সরকারি ছুটির দিনে app বন্ধ/paused রাখতে হবে।
- 09:15/15:30 হলো recorder window; WebSocket background connection process চালু থাকা
  পর্যন্ত reconnect চেষ্টা করতে পারে। এটি Render host-কে জাগিয়ে রাখার নিশ্চয়তা নয়।
- Trading/order execution `ENABLE_TRADING=false`; স্বয়ংক্রিয় order place করে না।
- Angel One এবং Supabase credentials কখনো Git repo বা ZIP-এ বসাবেন না।

## Start command
`gunicorn render_app:app --workers 1 --threads 8 --timeout 180 --bind 0.0.0.0:$PORT`

## Render-এ `AG8001 Invalid Token` হলে
- Render → Service → Environment-এ `ANGEL_API_KEY`, `ANGEL_CLIENT_CODE`, `ANGEL_MPIN`, `ANGEL_TOTP_SECRET` সঠিকভাবে সেট আছে কি না দেখুন।
- এগুলো কখনো GitLab/ZIP-এ লিখবেন না; শুধু Render Environment-এ রাখবেন।
- এই প্যাকেজে Positions API-র `AG8001`/expired JWT শনাক্ত হলে একবার নতুন Angel One session তৈরি করে request retry করা হয়।
- নতুন session তৈরির পরও `AG8001` এলে Angel One-এর login/TOTP/API access বা Render Environment value যাচাই করতে হবে। কোনো API key, MPIN, TOTP secret বা JWT কাউকে পাঠাবেন না।

## Deploy check
- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn render_app:app --workers 1 --threads 8 --timeout 180 --bind 0.0.0.0:$PORT`
- GitLab repo-র root-এ ZIP extract করে `Prosenjit-main` ফোল্ডারের ভেতরের ফাইলগুলো রাখুন; অতিরিক্ত nested folder তৈরি করবেন না।

## Live WebSocket callback compatibility update
- Smart Stream callback এখন dict এবং raw binary frame—দুই ধরনের payload গ্রহণ করতে পারে।
- SDK callback-এর অতিরিক্ত arguments থাকলেও handler crash করবে না।
- Ping/pong frame-কে market tick হিসেবে গণনা করা হবে না।
- No-market-frame watchdog 15 সেকেন্ড পরে reconnect করবে এবং কারণটি Render log-এ লিখবে।
- Render-এর Angel One credentials আগের মতো Environment-এই রাখতে হবে; ZIP-এ কোনো secret নেই।

## WebSocket transport diagnostic update

The live stream uses the project's direct Angel Smart Stream WebSocket transport
instead of the SmartWebSocketV2 SDK wrapper. Raw binary frames are decoded and
logged before tick processing, helping distinguish subscription success from
actual market-data delivery. Credentials remain in Render environment variables;
never put API keys, PINs, passwords or TOTP values in source files.


## Dashboard SSE snapshot envelope fix

The server's `/api/tick-stream` sends its initial `snapshot` event as a
`full_snapshot` envelope containing the actual dashboard payload under
`payload`. The browser now passes this envelope through `applyTickDelta()`
so it unwraps and renders the payload correctly. Previously the browser passed
the envelope directly to `applyLivePayload()`, causing the initial dashboard
status to display `DISCONNECTED / RAW: 0` even while Render logs showed
Angel One binary frames arriving. A received NIFTY delta also updates the
visible diagnostic label to confirm browser-side delivery.


## Browser SSE reconnect stability fix (2026-11-13)
- The browser no longer forcibly closes an EventSource while its built-in retry
  is in the `CONNECTING` state. The previous 1.5-second fallback could repeatedly
  restart the stream and leave the dashboard showing only its first snapshot.
- A single delayed fallback is used only if EventSource reaches `CLOSED`.
- The Angel WebSocket watchdog now closes a socket that remains open in `ERROR`
  after a subscription/authentication failure, allowing the outer reconnect loop
  to run again instead of hanging indefinitely.
- Both the Render engine entry file and the standalone `app.py` dashboard copy
  contain the same browser reconnect correction.

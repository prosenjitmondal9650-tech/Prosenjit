# NIFTY LTP — Render + GitHub deployment

## প্যাকেজের ফাইল
- `app.py` — dashboard, strategy, Angel One tick processor ও Supabase recorder।
- `render_app.py` — Gunicorn entry point; engine একবার চালু করে।
- `requirements.txt` — dependencies।
- `render.yaml` — Render Blueprint।
- `.env.example` — environment variable-এর তালিকা।

## GitHub-এ আপডেট
1. ZIP extract করুন।
2. সব ফাইল repository-র root-এ upload/replace করুন।
3. Commit করুন। Render-এর GitHub auto-deploy চালু থাকলে deploy হবে।
4. Existing Render service হলে Root Directory ফাঁকা রাখুন এবং নিচের Start Command বসান।

## Render Start Command
`gunicorn render_app:app --workers 1 --threads 8 --timeout 180 --bind 0.0.0.0:$PORT`

একটি worker রাখুন; একাধিক worker হলে একই Angel One stream একাধিকবার চালু হতে পারে।

## Render Environment Variables
`ANGEL_API_KEY`, `ANGEL_CLIENT_CODE`, `ANGEL_MPIN`, `ANGEL_TOTP_SECRET`,
`SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `ENABLE_TRADING=false`,
`CLOUD_RECORDER_ENABLED=true`, `CLOUD_RECORDER_INTERVAL=60`

কোনো API key, MPIN, TOTP secret বা Supabase service-role key GitHub-এ রাখবেন না।

## Deploy যাচাই
- `https://YOUR-SERVICE.onrender.com/health`
- Dashboard: `https://YOUR-SERVICE.onrender.com/`
- Live SSE: `https://YOUR-SERVICE.onrender.com/api/tick-stream`

বাজার চলাকালে `/health`-এ `websocket` CONNECTED থাকতে হবে এবং `raw_ws_messages` বাড়তে হবে।
NIFTY/option LTP, OI ও volume Angel One Smart Stream tick থেকে browser-এ SSE দিয়ে যায়।
Supabase recorder configured interval-এ snapshots/history save করে; প্রতি tick-এ insert করে না।

## গুরুত্বপূর্ণ
- `ENABLE_TRADING=false` রাখা হয়েছে; অর্ডার পাঠানো বন্ধ থাকবে।
- Render Free idle হলে sleep করতে পারে। বাজার চলাকালে নিরবচ্ছিন্ন stream/recording-এর জন্য always-on paid instance দরকার।
- মূল dashboard/strategy logic অক্ষত; WebSocket runner-কে এই ফাইলের DirectAngelOneWS parser-এর সঙ্গে যুক্ত করা হয়েছে।

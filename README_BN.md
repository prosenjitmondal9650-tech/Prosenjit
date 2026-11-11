# Render Deployment — NIFTY LTP Shift Engine

## ফাইল
- `app.py`: আপনার আপলোড করা মূল Flask/Angel One অ্যাপ।
- `wsgi.py`: Gunicorn import করার সময় `startup()` চালায় (মূল স্ক্রিপ্টে startup কেবল `__main__`-এ ছিল)।
- `requirements.txt`: Python dependencies।
- `render.yaml`: Render Blueprint config; single worker ব্যবহার করে কারণ live state memory-তে থাকে।
- `.env.example`: প্রয়োজনীয় environment variable-এর নাম; এখানে আসল secret দেবেন না।

## Render-এ চালানো
1. ZIP extract করে সব ফাইল একটি GitHub repository-র root-এ রাখুন।
2. GitHub-এ commit/push করুন।
3. Render Dashboard → New → Blueprint (render.yaml) অথবা New → Web Service → ওই repo নির্বাচন করুন।
4. Web Service হলে:
   - Runtime: Python
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn --worker-class gthread --workers 1 --threads 16 --timeout 120 --bind 0.0.0.0:$PORT wsgi:app`
   - Health Check Path: `/health`
5. Render → Environment-এ নিচের secret values যোগ করুন:
   - `ANGEL_API_KEY`
   - `ANGEL_CLIENT_CODE`
   - `ANGEL_MPIN`
   - `ANGEL_TOTP_SECRET` (Angel One authenticator TOTP secret; OTP code নয়)
   - `SUPABASE_URL` এবং `SUPABASE_SERVICE_ROLE_KEY` (Cloud recorder চাইলে)
6. Save, deploy, তারপর Render Logs দেখুন।
7. `https://YOUR-SERVICE.onrender.com/health` খুলে `ok: true` দেখুন; মূল dashboard হবে service root URL-এ।

## গুরুত্বপূর্ণ
- `ENABLE_TRADING=false` রাখুন। এই অ্যাপে order/square-off API আছে; পরীক্ষা না করে trading চালু করবেন না।
- Supabase না দিলে live dashboard চলতে পারে, কিন্তু durable cloud history/recorder চলবে না।
- Supabase ব্যবহার করলে কোডে প্রত্যাশিত তিনটি table ও তাদের column/schema আগে তৈরি থাকতে হবে; শুধু URL/key দিলেই table তৈরি হয় না। অ্যাপের `_cloud_recorder_save_payload()`-এ ব্যবহৃত field অনুযায়ী SQL schema যাচাই করুন।
- Render-এর local filesystem স্থায়ী নয়। JSON cache/baseline ফাইল restart/deploy-এ হারাতে পারে; cloud recorder-এর জন্য Supabase প্রয়োজন।
- Single worker অপরিহার্য: একাধিক Gunicorn worker হলে প্রত্যেক worker আলাদা Angel login/WebSocket ও আলাদা in-memory chain চালাবে।
- Free/sleeping service live market stream-এর জন্য নির্ভরযোগ্য নয়; always-on paid instance ব্যবহার করুন।
- বাজার বন্ধ থাকলে WebSocket live tick না আসা স্বাভাবিক; health endpoint কেবল server health নিশ্চিত করে, broker stream freshness নয়।
- API key, MPIN, TOTP secret বা Supabase service-role key GitHub-এ commit করবেন না; কোনো secret chat-এও পাঠাবেন না।

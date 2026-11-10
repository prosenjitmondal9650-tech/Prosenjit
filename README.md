# NIFTY LTP Shift Recorder

এই repository-তে NIFTY LTP Shift Recorder-এর Python Flask application এবং PythonAnywhere WSGI entry point রাখা হয়েছে।

## Repository files

- `9_LTP_SHIFT_ENGINE_SR_FIXED_ANGEL_VOLUME_SYNC.py` — মূল application
- `requirements.txt` — Python dependencies
- `wsgi.py` — PythonAnywhere WSGI loader
- `.env.example` — প্রয়োজনীয় environment variable-এর নামের নমুনা; এখানে কোনো আসল secret নেই
- `.gitignore` — cache, runtime data এবং secret Git-এ upload হওয়া আটকায়

## GitHub-এ upload

সব ফাইল repository-র root folder-এ upload করুন। `.github` বা অন্য কোনো অতিরিক্ত deployment workflow এই package-এ নেই। GitHub এখানে source-code repository; শুধু GitHub-এ upload করলেই application 24/7 চালু হবে না।

## PythonAnywhere setup

1. Repository clone/pull করে সব root file `/home/YOUR_USERNAME/mysite/`-এ রাখুন।
2. Python 3.11 virtualenv-এ চালান: `pip install -r requirements.txt`।
3. Web tab-এর WSGI configuration file-এ এই repository-র `wsgi.py`-এর content ব্যবহার করুন; `PROJECT_DIR` আপনার `mysite` path-এর সঙ্গে মিলিয়ে নিন।
4. Web app-এর environment variables-এ `.env.example`-এ থাকা প্রয়োজনীয় Angel One values সেট করুন। Supabase ব্যবহার করলে তার values-ও দিন।
5. Web app Reload করুন এবং `/health` endpoint পরীক্ষা করুন।

## Security

- `ANGEL_API_KEY`, `ANGEL_MPIN`, `ANGEL_TOTP_SECRET`, `SUPABASE_SERVICE_ROLE_KEY` কখনো GitHub-এ লিখবেন না।
- এগুলো hosting provider-এর Environment Variables / Secrets-এ রাখুন।
- `.env` ফাইল commit করবেন না।
- `ENABLE_TRADING` ডিফল্টভাবে `false`; প্রয়োজন ও যথাযথ যাচাই ছাড়া এটি চালু করবেন না।

## Notes

এই README repository packaging ও deployment steps বর্ণনা করে; live market data, broker access, hosting uptime বা free-tier background execution নিশ্চিত করে না।

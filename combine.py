"""
Build worker.py by reading both source workers and combining them.
Run: python combine.py
"""
import os

MULTI = r"d:\gemini_cli\screener-alerts-multi\worker.py"
MTF   = r"d:\gemini_cli\muli-improved\worker.py"
OUT   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")

with open(MULTI, "r", encoding="utf-8") as f:
    multi = f.read()
with open(MTF, "r", encoding="utf-8") as f:
    mtf = f.read()

# ── Extract sections from muli-improved ──
def between(src, start_marker, end_marker):
    s = src.find(start_marker)
    if s == -1: return ""
    e = src.find(end_marker, s + len(start_marker))
    if e == -1: return src[s:]
    return src[s:e]

# Timeframes + candle limits
tf_block = between(mtf, "TIMEFRAMES = {", "# HELPERS")
tf_block = "TIMEFRAMES = {" + tf_block.split("TIMEFRAMES = {", 1)[1] if "TIMEFRAMES = {" in tf_block else tf_block

# Technical analysis (helpers through message formatters and telegram sender)
tech_start = mtf.find("async def js_fetch")
tech_end = mtf.find("# " + "═" * 71 + "\n# KV HELPERS")
tech_block = mtf[tech_start:tech_end].rstrip() if tech_start > 0 and tech_end > 0 else ""

# We need ist_now, is_market_hours, is_entry_window, fmt_price, fmt_pct
# These are in tech_block already. Good.

# ── Extract sections from screener-alerts-multi ──
# Screeners list
scr_start = multi.find("SCREENERS = [")
scr_end = multi.find("\nDEFAULT_SETTINGS")
screeners_block = multi[scr_start:scr_end].rstrip() if scr_start > 0 else ""

# Default settings
ds_start = multi.find("DEFAULT_SETTINGS = {")
ds_end = multi.find("\n\nclass Default")
ds_block = multi[ds_start:ds_end].rstrip() if ds_start > 0 else ""

# Add min_score to DEFAULT_SETTINGS if not present
if '"min_score"' not in ds_block:
    ds_block = ds_block.replace('"total_runs": 0,', '"total_runs": 0,\n    "min_score": 50,')

# Pure functions (extract_csrf, url_encode, parse_table, extract_between)
pf_start = multi.find("def extract_csrf(html):")
pf_end = multi.find("async def send_telegram(")
pure_funcs_block = multi[pf_start:pf_end].rstrip() if pf_start > 0 else ""

# Original send_telegram
orig_send_tg = between(multi, "async def send_telegram(", "\n\n")
orig_send_tg = "async def send_telegram_basic(token, chat_id, text):\n" + orig_send_tg.split("\n", 1)[1]

# Dashboard HTML from multi
dash_start = multi.find('DASHBOARD_HTML = r"""')
dashboard_block = multi[dash_start:].rstrip() if dash_start > 0 else ""

# ── Update dashboard title ──
dashboard_block = dashboard_block.replace(
    "Screener Alerts — Multi",
    "Screener Alerts — MTF"
)
dashboard_block = dashboard_block.replace(
    "Cloudflare Worker • Multiple Screeners → Telegram",
    "Multi-Timeframe Technical Analysis • 8 TFs × 13 Indicators → Telegram"
)
dashboard_block = dashboard_block.replace(
    "📊 <span>Screener Alerts</span> Multi",
    "📈 <span>Screener Alerts</span> MTF"
)

# ── Build combined worker ──
parts = []

# Imports
parts.append("""from js import fetch, Headers
from pyodide.ffi import to_js
from workers import Response, WorkerEntrypoint
import json
import math
""")

# Screeners
parts.append("# " + "═" * 71)
parts.append("# PRECONFIGURED SCREENERS")
parts.append("# " + "═" * 71)
parts.append("")
parts.append(screeners_block)
parts.append("")
parts.append(ds_block)
parts.append("")

# Timeframes from MTF
parts.append("# " + "═" * 71)
parts.append("# TIMEFRAME DEFINITIONS (Yahoo Finance)")
parts.append("# " + "═" * 71)
parts.append("")
# Extract just TIMEFRAMES dict and TF_CANDLE_LIMIT dict
tf_s = mtf.find("TIMEFRAMES = {")
tf_e = mtf.find("\n# " + "═" * 71, tf_s + 10)
if tf_s > 0 and tf_e > 0:
    parts.append(mtf[tf_s:tf_e].rstrip())
parts.append("")

# Technical analysis block
parts.append("# " + "═" * 71)
parts.append("# HELPERS & TECHNICAL ANALYSIS ENGINE")
parts.append("# " + "═" * 71)
parts.append("")
parts.append(tech_block)
parts.append("")

# Pure functions from multi (screener HTML parsing)
parts.append("# " + "═" * 71)
parts.append("# SCREENER.IN HTML PARSER")
parts.append("# " + "═" * 71)
parts.append("")
parts.append(pure_funcs_block)
parts.append("")

# Now build the main class
main_class = '''
# ═══════════════════════════════════════════════════════════════════════════
# MAIN WORKER CLASS
# ═══════════════════════════════════════════════════════════════════════════

class Default(WorkerEntrypoint):

    async def fetch(self, request):
        url = str(request.url)
        method = str(request.method)

        if "/api/settings" in url and method == "GET":
            return self._json(await self._get_settings())
        if "/api/settings" in url and method == "POST":
            body = json.loads(str(await request.text()))
            cur = await self._get_settings()
            cur.update(body)
            await self.env.KV.put("settings", json.dumps(cur))
            return self._json({"ok": True, "settings": cur})

        if "/api/screeners" in url and method == "GET":
            return self._json(await self._get_screeners())
        if "/api/screeners/delete" in url and method == "POST":
            body = json.loads(str(await request.text()))
            del_id = body.get("id", "")
            screeners = await self._get_screeners()
            screeners = [s for s in screeners if s["id"] != del_id]
            await self.env.KV.put("screeners", json.dumps(screeners))
            return self._json({"ok": True, "screeners": screeners})
        if "/api/screeners/toggle" in url and method == "POST":
            body = json.loads(str(await request.text()))
            toggle_id = body.get("id", "")
            screeners = await self._get_screeners()
            for s in screeners:
                if s["id"] == toggle_id:
                    s["enabled"] = not s.get("enabled", True)
                    break
            await self.env.KV.put("screeners", json.dumps(screeners))
            return self._json({"ok": True, "screeners": screeners})
        if "/api/screeners" in url and method == "POST":
            body = json.loads(str(await request.text()))
            screeners = await self._get_screeners()
            found = False
            for i, s in enumerate(screeners):
                if s["id"] == body.get("id"):
                    screeners[i].update(body)
                    found = True
                    break
            if not found:
                screeners.append(body)
            await self.env.KV.put("screeners", json.dumps(screeners))
            return self._json({"ok": True, "screeners": screeners})

        if "/api/telegram/delete" in url and method == "POST":
            body = json.loads(str(await request.text()))
            del_idx = int(body.get("index", -1))
            accts = await self._get_telegram_accounts()
            if 0 <= del_idx < len(accts):
                accts.pop(del_idx)
            await self.env.KV.put("telegram_accounts", json.dumps(accts))
            return self._json({"ok": True, "accounts": accts})
        if "/api/telegram" in url and method == "GET":
            return self._json(await self._get_telegram_accounts())
        if "/api/telegram" in url and method == "POST":
            body = json.loads(str(await request.text()))
            accts = await self._get_telegram_accounts()
            accts.append({"name": body.get("name",""), "token": body.get("token",""), "chat_id": body.get("chat_id","")})
            await self.env.KV.put("telegram_accounts", json.dumps(accts))
            return self._json({"ok": True, "accounts": accts})

        if "/api/trigger" in url and method == "POST":
            screeners = await self._get_screeners()
            count = 0
            for s in screeners:
                if s.get("enabled", True):
                    await self._run_single(s)
                    count += 1
            return self._json({"ok": True, "triggered": count})

        if "/api/cron" in url and method == "POST":
            await self.scheduled(None, self.env, None)
            return self._json({"ok": True, "cron_processed": True})

        if method == "OPTIONS":
            return Response("", headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            })

        return Response(DASHBOARD_HTML, headers={"Content-Type": "text/html"})

    async def scheduled(self, event, env, ctx):
        settings = await self._get_settings()
        if not settings.get("enabled", True):
            return
        from datetime import datetime, timezone, timedelta
        IST = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(IST)
        now_epoch = int(now.timestamp())
        today = now.strftime("%Y-%m-%d")
        now_m = now.hour * 60 + now.minute
        mode = settings.get("schedule_mode", "global")
        screeners = await self._get_screeners()

        if mode == "global":
            interval = int(settings.get("interval_minutes", 1))
            last_epoch = int(settings.get("last_run_epoch", 0))
            if last_epoch and interval > 1:
                if (now_epoch - last_epoch) / 60 < interval:
                    return
            if not self._in_time_window(settings, now_m, today):
                return
            for s in screeners:
                if s.get("enabled", True):
                    await self._run_single(s)
            fresh = await self._get_settings()
            fresh["last_run"] = now.isoformat()
            fresh["last_run_epoch"] = now_epoch
            fresh["total_runs"] = fresh.get("total_runs", 0) + 1
            await self.env.KV.put("settings", json.dumps(fresh))
        else:
            ran_any = False
            for s in screeners:
                if not s.get("enabled", True):
                    continue
                s_interval = int(s.get("interval_minutes", 5))
                s_last = int(s.get("last_run_epoch", 0))
                if s_last and s_interval > 1:
                    if (now_epoch - s_last) / 60 < s_interval:
                        continue
                if not self._in_time_window(s, now_m, today):
                    continue
                await self._run_single(s)
                s["last_run_epoch"] = now_epoch
                ran_any = True
            if ran_any:
                await self.env.KV.put("screeners", json.dumps(screeners))
                fresh = await self._get_settings()
                fresh["last_run"] = now.isoformat()
                fresh["total_runs"] = fresh.get("total_runs", 0) + 1
                await self.env.KV.put("settings", json.dumps(fresh))

    def _in_time_window(self, cfg, now_m, today):
        try:
            sh, sm = map(int, cfg.get("start_time", "09:15").split(":"))
            eh, em = map(int, cfg.get("end_time", "15:30").split(":"))
            if now_m < sh * 60 + sm or now_m > eh * 60 + em:
                return False
        except:
            pass
        sd = cfg.get("start_date", "")
        ed = cfg.get("end_date", "")
        if sd and today < sd: return False
        if ed and today > ed: return False
        return True

    def _json(self, data):
        return Response(json.dumps(data), headers={
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        })

    async def _get_settings(self):
        raw = await self.env.KV.get("settings")
        if raw:
            saved = json.loads(str(raw))
            m = dict(DEFAULT_SETTINGS)
            m.update(saved)
            return m
        return dict(DEFAULT_SETTINGS)

    async def _get_screeners(self):
        raw = await self.env.KV.get("screeners")
        if raw:
            return json.loads(str(raw))
        await self.env.KV.put("screeners", json.dumps(SCREENERS))
        return list(SCREENERS)

    async def _get_telegram_accounts(self):
        raw = await self.env.KV.get("telegram_accounts")
        if raw:
            return json.loads(str(raw))
        seed = [
            {"name": "Account 1", "token": "8602997050:AAEraNu-MoDxk159s0eSC8c_xfKPaej3NRA", "chat_id": "1720179071"},
            {"name": "Account 2", "token": "8337122144:AAGztmijeFxer6ES6gbq0uzPGVFiaCZ-aB0", "chat_id": "8094499819"},
        ]
        await self.env.KV.put("telegram_accounts", json.dumps(seed))
        return seed

    async def _send_all(self, text):
        accts = await self._get_telegram_accounts()
        for a in accts:
            await send_telegram(a["token"], a["chat_id"], text)

    async def _send_all_stock(self, stock, screener_name):
        accts = await self._get_telegram_accounts()
        msg1 = format_message_overview(stock, screener_name)
        msg2 = format_message_details(stock)
        for a in accts:
            await send_telegram(a["token"], a["chat_id"], msg1)
            await send_telegram(a["token"], a["chat_id"], msg2)

    async def _run_single(self, screener):
        sid = screener["id"]
        scr_url = screener["url"]
        scr_query = screener["query"]
        scr_name = screener.get("name", sid)
        settings = await self._get_settings()
        min_score = int(settings.get("min_score", 50))

        try:
            get_headers = Headers.new(to_js({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            }))
            get_resp = await fetch(scr_url, method="GET", headers=get_headers)
            html = await get_resp.text()
            csrf_token = extract_csrf(str(html))

            set_cookie = str(get_resp.headers.get("set-cookie") or "")
            csrf_cookie = ""
            for part in set_cookie.split(";"):
                part = part.strip()
                if part.startswith("csrftoken="):
                    csrf_cookie = part
                    break

            body_str = f"csrfmiddlewaretoken={csrf_token}&query={url_encode(scr_query)}&order="
            post_headers = Headers.new(to_js({
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": scr_url,
                "Origin": "https://www.screener.in",
                "X-Requested-With": "XMLHttpRequest",
                "Cookie": csrf_cookie,
            }))
            post_resp = await fetch(scr_url, method="POST", body=body_str, headers=post_headers)
            result_html = str(await post_resp.text())

            headers_row, rows = parse_table(result_html)

            prev_key = f"prev_names_{sid}"
            prev_json = await self.env.KV.get(prev_key)
            prev_names = json.loads(str(prev_json)) if prev_json else []
            curr_names = [dict(zip(headers_row, r)).get("Name", "") for r in rows]

            entered = [n for n in curr_names if n not in prev_names]
            exited  = [n for n in prev_names if n not in curr_names]

            if entered:
                for row in rows:
                    d = dict(zip(headers_row, row))
                    name = d.get("Name", "")
                    if name not in entered:
                        continue
                    symbol = d.get("_symbol", name.replace(" ","").replace(".","").upper())
                    exchange = "BO" if symbol.isdigit() else "NS"
                    price_str = d.get("CMP / LTP", d.get("Current Price", "0"))
                    try:
                        price = float(str(price_str).replace(",",""))
                    except:
                        price = 0

                    stock = {"name": name, "symbol": symbol, "exchange": exchange,
                             "price": price, "change": d.get("% Chg", d.get("Chg %", "0"))}
                    enriched = await enrich_stock(stock)
                    overall_pct = enriched.get("mtf_summary", {}).get("overall_pct", 0)

                    if overall_pct >= min_score:
                        await self._send_all_stock(enriched, scr_name)
                    else:
                        now_str = ist_now().strftime("%d-%b-%Y %H:%M IST")
                        await self._send_all(
                            f"\\U0001f4ca <b>{scr_name}</b>\\n"
                            f"\\U0001f195 {name} ({symbol}) \\u2014 \\u20b9{price}\\n"
                            f"\\u26a0\\ufe0f MTF Score: {overall_pct}% (below {min_score}% threshold)\\n"
                            f"\\U0001f550 {now_str}"
                        )

            if exited:
                now_str = ist_now().strftime("%d-%b-%Y %H:%M IST")
                await self._send_all(
                    f"\\U0001f4ca <b>{scr_name}</b>\\n"
                    f"\\U0001f6aa <b>EXITED:</b> {', '.join(exited)}\\n"
                    f"\\U0001f550 {now_str}"
                )

            await self.env.KV.put(prev_key, json.dumps(curr_names))

        except Exception as e:
            await self._send_all(f"\\u274c <b>[{scr_name}] Error:</b> {str(e)}")
'''

parts.append(main_class)

# Dashboard HTML
parts.append("")
parts.append("# " + "═" * 71)
parts.append("# DASHBOARD HTML")
parts.append("# " + "═" * 71)
parts.append("")
parts.append(dashboard_block)

# Write output
output = "\n".join(parts)
with open(OUT, "w", encoding="utf-8") as f:
    f.write(output)

print(f"✅ Combined worker written to {OUT}")
print(f"   Total size: {len(output)} bytes, {output.count(chr(10))} lines")

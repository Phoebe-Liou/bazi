"""金金雷達 LINE 機器人核心（排程 line_push.py 與網站 webhook 共用）。

聊天室輸入「飆成精」：
  1. 3 分鐘內有人算過 → 立刻回覆同一份（回覆不扣 LINE 額度）。
  2. 否則先回覆「計算中」，背景連線平台選股（盤中即時：用 TWSE MIS 即時報價補上今天的K線），
     算個股分數前三名，完成後推播到同一個聊天室（推播會扣額度，每月免費 200 則）。
訊息最後會寫明資料時間；個股分數用資料庫最近收盤資料，不是即時。
排程 line_push.py cache 另外存一份「收盤後日K」結果，當作歷史快取。"""
import os, re, json, time, threading, datetime, urllib.request, urllib.error

STRATEGY_KEYWORD = "飆成精"
RESEARCH_ENABLED = os.environ.get("LINE_RESEARCH", "").strip() == "1"   # AI 深度研究的 LINE 指令，預設關閉；要開再到 Render 設 LINE_RESEARCH=1
TOP_N = 3
MAX_CHARS = 4800
API_REPLY = "https://api.line.me/v2/bot/message/reply"
API_PUSH = "https://api.line.me/v2/bot/message/push"
API_MULTI = "https://api.line.me/v2/bot/message/multicast"
API_BROADCAST = "https://api.line.me/v2/bot/message/broadcast"
FOOT = "僅為條件篩選結果，非買賣建議。"

LOG = []   # 最近事件紀錄（記憶體內，重啟會清空），給 /api/line-status 看


def log(kind, **kw):
    LOG.append(dict(t=(datetime.datetime.utcnow() + datetime.timedelta(hours=8)).strftime("%m-%d %H:%M:%S"), kind=kind, **kw))
    del LOG[:-40]


_LIVE = {"running": False, "targets": set(), "lock": threading.Lock()}
_RECENT = {"t": 0.0, "text": None}
RECENT_SECONDS = 180


def taipei_today():
    return (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).date()


def _token():
    return os.environ.get("LINE_CHANNEL_TOKEN", "").strip()


def _post(url, payload):
    token = _token()
    if not token:
        raise RuntimeError("缺少 LINE_CHANNEL_TOKEN")
    body = json.dumps(payload, ensure_ascii=False).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type": "application/json", "Authorization": "Bearer " + token})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode()[:200]
    except urllib.error.HTTPError as e:
        raise RuntimeError("LINE API %s：%s" % (e.code, e.read().decode()[:300]))


def reply(reply_token, text):
    if not reply_token:
        return
    try:
        _post(API_REPLY, {"replyToken": reply_token, "messages": [{"type": "text", "text": text[:MAX_CHARS]}]})
        log("reply_ok", head=text[:20])
    except Exception as e:
        print("LINE reply 失敗", e)
        log("reply_fail", err=str(e)[:200])


def push(to, text):
    return _post(API_PUSH, {"to": to, "messages": [{"type": "text", "text": text[:MAX_CHARS]}]})


def push_many(to, texts):
    """一次最多 5 則（LINE 限制）。"""
    msgs = [{"type": "text", "text": t[:MAX_CHARS]} for t in texts[:5]]
    return _post(API_PUSH, {"to": to, "messages": msgs})


def reply_many(reply_token, texts):
    if not reply_token:
        return
    try:
        msgs = [{"type": "text", "text": t[:MAX_CHARS]} for t in texts[:5]]
        _post(API_REPLY, {"replyToken": reply_token, "messages": msgs})
        log("reply_ok", head=texts[0][:20])
    except Exception as e:
        print("LINE reply 失敗", e)
        log("reply_fail", err=str(e)[:200])


def broadcast_or_multicast(text):
    to = [x.strip() for x in os.environ.get("LINE_TO", "").split(",") if x.strip()]
    msg = {"type": "text", "text": text[:MAX_CHARS]}
    if to:
        return _post(API_MULTI, {"to": to, "messages": [msg]}), "指定收件人"
    return _post(API_BROADCAST, {"messages": [msg]}), "全部好友"


# ── 選股、排名、訊息 ──
def run_scan(app, intraday=False):
    strategies = app.strategies_get()
    name = next((n for n in strategies if STRATEGY_KEYWORD in n), None)
    if not name:
        raise RuntimeError("策略庫找不到名稱含「%s」的策略，請先執行 seed_strategies" % STRATEGY_KEYWORD)
    payload = strategies[name]
    q = {"universe": "both", "mode": payload.get("mode", "all"), "conditions": payload["conditions"], "intraday": bool(intraday)}
    proxy = app._ScanProxy()
    app.Handler._scan_core(proxy, q)
    status, out = proxy.out or (500, {"ok": False, "error": "沒有結果"})
    if not out.get("ok"):
        raise RuntimeError("選股失敗：%s" % out.get("error"))
    return name, out


def rank_by_score(app, rows):
    from concurrent.futures import ThreadPoolExecutor

    def one(z):
        try:
            d = app.db_stock_health(z["code"])
            sc = d.get("adjusted_score")
            if sc is None:
                sc = d.get("available_score")
            return z, sc
        except Exception as e:
            print("體檢失敗", z.get("code"), type(e).__name__)
            return z, None
    with ThreadPoolExecutor(max_workers=4) as ex:
        scored = list(ex.map(one, rows))
    scored.sort(key=lambda t: (t[1] is not None, t[1] if t[1] is not None else -1, t[0].get("change_pct") or -999), reverse=True)
    for z, sc in scored:
        z["_score"] = sc
    print("分數排行：", [(z["code"], sc) for z, sc in scored])
    return [z for z, _ in scored[:TOP_N]], len(rows)


def flow_text(z):
    f = z.get("flow") or {}
    lab = {"warming": "悄悄升溫", "watch": "接近升溫", "hot": "已發燒"}
    ts = [t for t in f.get("themes", []) if t.get("status") in lab]
    if ts:
        return "%s·%s" % (ts[0]["title"], lab[ts[0]["status"]])
    ind = f.get("industry")
    if ind and ind.get("status") in lab:
        return "%s·%s" % (ind["name"], lab[ind["status"]])
    return ""


def foot_text(out):
    info = out.get("intraday")
    if info and info.get("used"):
        return ("⚠ 盤中即時：%s %s %s補上今天K線；法人／融資券／營收為最新已公布日；個股分數用最近收盤資料。%s"
                % (info.get("date"), info.get("time"), info.get("label", "TWSE 即時報價"), FOOT))
    if info is not None:
        why = "抓到%s檔" % info.get("fetched", 0)
        if info.get("errors"):
            why += "，錯誤：" + "；".join(str(x) for x in info["errors"][:2])
        if info.get("date"):
            why += "，報價日" + str(info["date"])
        return "⚠ 沒有取得即時報價（%s），改用資料庫收盤日K。" % why + FOOT
    return "⚠ 資料為資料庫收盤後日K計算，非盤中即時。" + FOOT


def build_message(app, name, out):
    allrows = out.get("rows", [])
    rows, total = rank_by_score(app, allrows) if allrows else ([], 0)
    date = ""
    if rows:
        date = str(rows[0].get("price_date") or "")[5:10].replace("-", "/")
    head = "📈 %s %s｜共 %d 檔，個股分數前 %d 名" % (name, date, total, len(rows))
    if not rows:
        return head + "\n今天沒有符合條件的股票。"
    if out.get("partial"):
        head += "\n⚠ 部分條件資料不完整，結果僅供參考"
    lines = [head]
    _info = out.get("intraday")
    px = "現價" if (_info and _info.get("used")) else "收"   # 盤中還沒收盤，不能叫收盤價
    for i, z in enumerate(rows, 1):
        chg, vol, sc = z.get("change_pct"), z.get("volume"), z.get("_score")
        s = "%d. %s %s｜分數 %s\n   %s %s" % (i, z.get("code"), z.get("name"), sc if sc is not None else "—", px,
                                             ("%.2f" % z["close"]) if z.get("close") is not None else "—")
        if chg is not None:
            s += " (%+.1f%%)" % chg
        if vol is not None:
            s += " 量%s張" % format(int(round(vol / 1000)), ",")
        t = flow_text(z)
        if t:
            s += "\n   ↳ " + t
        lines.append(s)
    if total > len(rows):
        lines.append("其餘 %d 檔請到金金雷達查看" % (total - len(rows)))
    lines.append(foot_text(out))
    return "\n".join(lines)


# ── 資料庫：結果快取、資料是否完整 ──
def ensure_cache(app):
    with app.db_conn() as c:
        with c.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS public.line_scan_cache(strategy text, data_date date, text text, count int, updated_at timestamptz DEFAULT now(), PRIMARY KEY(strategy, data_date))")
        c.commit()


def latest_data_date(app):
    with app.db_conn() as c:
        with c.cursor() as cur:
            cur.execute("SELECT max(trade_date) FROM public.daily_prices")
            r = cur.fetchone()
    return r[0] if r else None


def get_cache(app, data_date):
    try:
        with app.db_conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT text FROM public.line_scan_cache WHERE strategy LIKE %s AND data_date=%s ORDER BY updated_at DESC LIMIT 1",
                            ("%" + STRATEGY_KEYWORD + "%", data_date))
                r = cur.fetchone()
        return r[0] if r else None
    except Exception:
        return None


def cache_has(app, day, name):
    with app.db_conn() as c:
        with c.cursor() as cur:
            cur.execute("SELECT 1 FROM public.line_scan_cache WHERE strategy=%s AND data_date=%s", (name, day))
            return cur.fetchone() is not None


def save_cache(app, day, name, text, n):
    ensure_cache(app)
    with app.db_conn() as c:
        with c.cursor() as cur:
            cur.execute("INSERT INTO public.line_scan_cache(strategy,data_date,text,count) VALUES(%s,%s,%s,%s) ON CONFLICT(strategy,data_date) DO UPDATE SET text=excluded.text,count=excluded.count,updated_at=now()", (name, day, text, n))
        c.commit()


def data_ready(app, day):
    with app.db_conn() as c:
        with c.cursor() as cur:
            cur.execute("SELECT trade_date, count(*) FROM public.daily_prices GROUP BY 1 ORDER BY 1 DESC LIMIT 2")
            rows = cur.fetchall()
    if len(rows) < 2:
        return False, "日K資料不足"
    (d1, n1), (d2, n2) = rows[0], rows[1]
    if str(d1) != str(day):
        return False, "最新日K是 %s，不是今天 %s（可能休市或尚未更新）" % (d1, day)
    if n1 < n2 * 0.9:
        return False, "今天只有 %d 檔（前一日 %d 檔），資料還沒更新完整" % (n1, n2)
    return True, ""


def with_foot(text, data_date):
    return "%s\n\n⚠ 資料為 %s 收盤後日K計算，非盤中即時。" % (text, data_date)


# ── webhook 事件 ──
def _target(ev):
    s = ev.get("source", {})
    return s.get("groupId") or s.get("roomId") or s.get("userId")


def _live_compute(app, data_date):
    try:
        name, out = run_scan(app, intraday=True)
        final = build_message(app, name, out)
        _RECENT["t"], _RECENT["text"] = time.time(), final
        with _LIVE["lock"]:
            targets = list(_LIVE["targets"])
            _LIVE["targets"].clear()
        for t in targets:
            try:
                push(t, final)
                log("push_ok", to=str(t)[:6] + "…")
            except Exception as e:
                print("推播失敗", e)
                log("push_fail", to=str(t)[:6] + "…", err=str(e)[:200])
    except Exception as e:
        with _LIVE["lock"]:
            targets = list(_LIVE["targets"])
            _LIVE["targets"].clear()
        for t in targets:
            try:
                push(t, "選股失敗：%s" % e)
            except Exception as e2:
                log("push_fail", err=str(e2)[:200])
        log("scan_fail", err=str(e)[:200])
    finally:
        with _LIVE["lock"]:
            _LIVE["running"] = False



# ---------- ETF 配息查詢（輸入代號或名稱，回覆配息時間與金額） ----------
def _etf_names(app):
    """ETF 代號→名稱：優先用已核對的官方名錄，沒有就用股票主檔 00 開頭。"""
    out = {}
    try:
        saved = app.saved_etf_catalog()
        if saved:
            for r in saved.get("rows", []):
                out[str(r["stock_id"]).upper()] = r.get("stock_name") or ""
    except Exception:
        pass
    if not out:
        try:
            with app.db_conn() as c:
                with c.cursor() as cur:
                    cur.execute("SELECT stock_id,stock_name FROM public.stocks WHERE stock_id LIKE '00%%'")
                    for a, b in cur.fetchall():
                        out[str(a).upper()] = b or ""
        except Exception:
            pass
    return out


def _etf_price(app, code):
    for table in ("public.etf_daily_prices", "public.daily_prices"):
        try:
            with app.db_conn() as c:
                with c.cursor() as cur:
                    cur.execute("SELECT close_price,trade_date FROM " + table + " WHERE stock_id=%s AND close_price IS NOT NULL ORDER BY trade_date DESC LIMIT 1", (code,))
                    r = cur.fetchone()
                    if r:
                        return float(r[0]), str(r[1])
        except Exception:
            continue
    return None, None


def etf_text(app, code, name):
    head = "📈 %s %s" % (code, name or "")
    try:
        divs = app.twse_etf_dividends()
    except Exception:
        return head + "\n\n官方配息資料暫時連不上，請過一兩分鐘再傳一次代號。"
    ds = divs.get(code, [])
    today = datetime.date.today().strftime("%Y/%m/%d")
    cutoff = (datetime.date.today() - datetime.timedelta(days=365)).strftime("%Y/%m/%d")
    past = [x for x in ds if x.get("ex_date", "") <= today]
    future = [x for x in ds if x.get("ex_date", "") > today]
    price, pdate = _etf_price(app, code)
    lines = [head]
    if price is not None:
        lines.append("收盤價 %.2f 元（%s）" % (price, pdate))
    if not ds:
        lines.append("\n證交所的配息表裡查不到這檔的配息紀錄。\n可能是不配息、剛上市還沒配過，或資料尚未公布；請以基金公司公告為準。")
        return "\n".join(lines)
    if past:
        p = past[0]
        amt = ("每單位 %s 元" % ("%g" % p["amount"])) if p.get("amount") is not None else "金額尚未公告"
        lines.append("\n【最近一次配息】\n金額：%s\n除息日：%s\n發放日：%s" % (amt, p.get("ex_date") or "—", p.get("pay_date") or "尚未公告"))
    if future:
        f = future[-1]
        amt = ("每單位 %s 元" % ("%g" % f["amount"])) if f.get("amount") is not None else "金額尚未公告"
        lines.append("\n【下一次（已公告）】\n金額：%s\n除息日：%s\n發放日：%s" % (amt, f.get("ex_date") or "—", f.get("pay_date") or "尚未公告"))
    else:
        lines.append("\n【下一次】官方還沒公告。")
    vals = [x["amount"] for x in past if x.get("ex_date", "") >= cutoff and x.get("amount") is not None]
    if vals:
        t = "\n近12個月配息合計：%s 元（%d 次）" % ("%g" % round(sum(vals), 4), len(vals))
        if price:
            t += "\n以收盤價換算約 %.2f%%（過去配息不代表未來）" % (sum(vals) / price * 100)
        lines.append(t)
    hist = ["%s　%s 元" % (x.get("ex_date", "—"), "%g" % x["amount"] if x.get("amount") is not None else "—") for x in past[:6]]
    if hist:
        lines.append("\n【最近幾次除息】\n" + "\n".join(hist))
    lines.append("\n資料：證交所 ETF e添富。僅供參考，非買賣建議。")
    return "\n".join(lines)


def etf_lookup(app, t):
    """回傳要回覆的文字；不是 ETF 查詢就回 None。"""
    q = t.strip().upper().replace(" ", "")
    if not q or len(q) > 20:
        return None
    names = _etf_names(app)
    if re.fullmatch(r"\d{4,6}[A-Z]?", q):
        if q in names or q.startswith("00"):
            return etf_text(app, q, names.get(q, ""))
        return "這裡只查 ETF 的配息。請輸入 00 開頭的 ETF 代號，例如 0056、00878。"
    hits = [(c, n) for c, n in names.items() if q in n.upper().replace(" ", "")]
    if len(q) >= 2 and hits:
        hits.sort()
        if len(hits) == 1:
            return etf_text(app, hits[0][0], hits[0][1])
        return "找到 %d 檔，請輸入代號：\n" % len(hits) + "\n".join("%s %s" % h for h in hits[:10]) + ("\n…還有更多，請再多打幾個字" if len(hits) > 10 else "")
    return None


# ---------- AI 深度研究（輸入「研究 2436」或「研究 偉詮電」，結果傳回群組／聊天室） ----------
_RWAIT = {"lock": threading.Lock(), "targets": {}}   # code -> 等這份報告的聊天室


def md_to_line(md):
    out = []
    for raw in str(md or "").splitlines():
        l = raw.rstrip()
        if re.match(r"^\s*\|[\s:|-]+\|\s*$", l):
            continue
        if re.match(r"^\s*\|.*\|\s*$", l):
            cells = [c.strip() for c in l.strip().strip("|").split("|")]
            l = "• " + "｜".join(c for c in cells if c)
        m = re.match(r"^#{1,4}\s+(.*)$", l)
        if m:
            l = "\n【" + m.group(1).replace("**", "") + "】"
        l = re.sub(r"^\s*[-*]\s+", "• ", l).replace("**", "")
        out.append(l)
    return re.sub(r"\n{3,}", "\n\n", "\n".join(out)).strip()


def research_messages(code, name, d):
    head = "🤖 %s %s AI 深度研究\n（AI 上網整理，數字可能有誤，非投資建議）\n" % (code, name)
    body = head + "\n" + md_to_line(d.get("text"))
    src = d.get("sources") or []
    tail = ("\n\n【搜尋來源】\n" + "\n".join("%d. %s" % (i + 1, (x.get("title") or "")[:40]) for i, x in enumerate(src[:6]))) if src else "\n\n（這份沒有附搜尋來源，請特別小心）"
    text = body + tail
    chunks, cur = [], ""
    for para in text.split("\n\n"):
        if len(cur) + len(para) + 2 > 4500 and cur:
            chunks.append(cur)
            cur = ""
        while len(para) > 4500:
            chunks.append(para[:4500])
            para = para[4500:]
        cur = (cur + "\n\n" + para) if cur else para
    if cur:
        chunks.append(cur)
    if len(chunks) > 5:
        chunks = chunks[:5]
        chunks[-1] = chunks[-1][:4300] + "\n\n（內容較長，後面省略；完整版請到金金雷達網站搜尋這檔、按「AI 深度研究」）"
    return chunks


def _short_why(err):
    e = str(err or "")
    if "回應 429" in e or "回應 403" in e or "額度" in e or "次數已用完" in e:
        return "Gemini 的免費額度用完了"
    if any(x in e for x in ("回應 503", "回應 500", "回應 504", "逾時", "塞車")):
        return "Gemini 目前塞車或逾時"
    return "Gemini 這次沒有成功"


def _db_fallback(app, code, why):
    """Gemini 失敗時：改用資料庫資料產生報告（不用 AI、不耗額度）。回傳要送出的訊息清單。"""
    import db_report
    rows = app.search_stock_research(code).get("rows", [])
    row = next((r for r in rows if str(r.get("code")).upper() == str(code).upper()), rows[0] if rows else None)
    if not row:
        return ["%s：%s，資料庫也找不到這檔。" % (code, why)]
    chunks = db_report.chunks(db_report.build(app, row))
    chunks[0] = "🤖 %s，改用你的資料庫資料（不是 AI，沒有業務介紹與目標價）：\n\n%s" % (why, chunks[0])
    return chunks


def _research_wait(app, code, name):
    deadline = time.time() + 660
    try:
        while time.time() < deadline:
            st = app.RESEARCH_STATE.get(code) or {}
            if st.get("status") in ("done", "error"):
                break
            time.sleep(4)
        st = app.RESEARCH_STATE.get(code) or {}
        with _RWAIT["lock"]:
            targets = list(_RWAIT["targets"].pop(code, set()))
        for t in targets:
            try:
                if st.get("status") == "done":
                    push_many(t, research_messages(code, name, st))
                    log("push_ok", to=str(t)[:6] + "…", what="research " + code)
                else:
                    why = _short_why(st.get("error"))
                    try:
                        push_many(t, _db_fallback(app, code, why))
                        log("push_ok", to=str(t)[:6] + "…", what="db_fallback " + code)
                    except Exception as e2:
                        log("db_fallback_error", err=type(e2).__name__ + ": " + str(e2)[:150])
                        push(t, "🤖 %s %s 的 AI 研究沒有完成：%s" % (code, name, (st.get("error") or "等太久了，請稍後再傳一次")[:300]))
            except Exception as e:
                print("研究推播失敗", e)
                log("push_fail", to=str(t)[:6] + "…", err=str(e)[:200])
    except Exception as e:
        log("research_error", err=str(e)[:200])


def research_request(app, tok, tgt, term, quiet=False):
    term = term.strip()
    if not term:
        return None if quiet else "請在後面加代號或名稱，例如：研究 2436"
    m = re.match(r"^(\d{4,6}[A-Za-z]?)", term)
    rows = app.search_stock_research(m.group(1) if m else term).get("rows", [])
    if not rows:
        return None if quiet else "找不到「%s」。請輸入上市櫃公司的代號或名稱，例如：研究 2436" % term[:20]
    if len(rows) > 1 and not (m and rows[0]["code"].upper() == m.group(1).upper()):
        if quiet:
            return None
        return "找到 %d 檔，請改輸入代號：\n" % len(rows) + "\n".join("%s %s" % (r["code"], r["name"]) for r in rows[:8])
    code, name = rows[0]["code"], rows[0]["name"]
    if os.environ.get("LINE_RESEARCH_DB", "").strip() == "1":
        # 預設維持 Gemini（上網版，額度用完時自動出不上網簡版）。想改用資料庫報告：Render 環境變數設 LINE_RESEARCH_DB=1（需另外上傳 db_report.py）
        import db_report
        try:
            txt = db_report.build(app, rows[0])
        except Exception as e:
            log("db_report_error", err=type(e).__name__ + ": " + str(e)[:150])
            return "%s %s 的資料庫報告產生失敗：%s" % (code, name, str(e)[:100])
        reply_many(tok, db_report.chunks(txt))
        return None
    d = app.stock_research(code, start=True)
    if not d.get("ok"):
        try:
            reply_many(tok, _db_fallback(app, code, _short_why(d.get("error"))))
            return None
        except Exception as e2:
            log("db_fallback_error", err=type(e2).__name__ + ": " + str(e2)[:150])
            return "🤖 現在沒辦法做 AI 研究：" + str(d.get("error", "原因不明"))[:200]
    q = d.get("quota") or {}
    qtxt = "（今日剩 %s／%s 次）" % (q.get("left"), q.get("cap")) if q else ""
    if d.get("status") == "done":
        reply_many(tok, research_messages(code, name, d))   # 已有 6 小時內的報告：用回覆傳，不耗推播額度
        return None
    with _RWAIT["lock"]:
        first = code not in _RWAIT["targets"]
        _RWAIT["targets"].setdefault(code, set()).add(tgt)
    if first:
        threading.Thread(target=_research_wait, args=(app, code, name), daemon=True).start()
    return "收到，AI 正在上網查 %s %s 並整理，約 1～3 分鐘，完成後傳到這裡。%s" % (code, name, qtxt)


NEWS_WORDS = ("財經", "財經新聞", "每日財經", "財經重點")


def _news_wait(app):
    deadline = time.time() + 600
    try:
        while time.time() < deadline:
            st = app.daily_news() or {}
            if st.get("status") in ("done", "error") or st.get("ok") is False:
                break
            time.sleep(4)
        st = app.daily_news() or {}
        with _RWAIT["lock"]:
            targets = list(_RWAIT["targets"].pop("NEWS", set()))
        for t in targets:
            try:
                push_many(t, _news_messages(app, st))
            except Exception as e:
                log("push_fail", to=str(t)[:6] + "…", err=str(e)[:200])
    except Exception as e:
        log("news_error", err=str(e)[:200])


def _news_messages(app, d):
    import db_report, db_market
    if d.get("status") == "done" and d.get("text"):
        src = d.get("sources") or []
        tail = ("\n\n【搜尋來源】\n" + "\n".join("%d. %s" % (i + 1, (x.get("title") or "")[:40]) for i, x in enumerate(src[:6]))) if src else ""
        return db_report.chunks("📰 每日財經重點（AI 上網整理，數字請自行查證來源）\n\n" + md_to_line(d["text"]) + tail)
    why = _short_why(d.get("error"))
    try:
        body = db_market.build(app)
    except Exception as e:
        body = "資料庫大盤摘要也產生失敗：%s" % str(e)[:80]
    return db_report.chunks("📰 %s，改用你的資料庫數字（沒有新聞與原因）：\n\n%s" % (why, body))


def news_request(app, tok, tgt):
    d = app.daily_news(start=True)
    if d.get("ok") and d.get("status") == "done":
        reply_many(tok, _news_messages(app, d))   # 今天已查過：直接回覆，不耗額度
        return None
    if not d.get("ok"):
        reply_many(tok, _news_messages(app, d))
        return None
    with _RWAIT["lock"]:
        first = "NEWS" not in _RWAIT["targets"]
        _RWAIT["targets"].setdefault("NEWS", set()).add(tgt)
    if first:
        threading.Thread(target=_news_wait, args=(app,), daemon=True).start()
    return "收到，AI 正在上網整理今天的財經重點，約 1～3 分鐘，完成後傳到這裡。（今天查過一次後，其他人再問會直接回覆）"


def _research_trigger(t):
    """回傳 (關鍵字, 是否安靜模式) 或 None。
    「研究 2436」「產研 偉詮電」：找不到會回覆提示。
    「2436產研」「偉詮電產研」：找不到就不回覆（避免群組閒聊「我在研究」被誤觸發）。"""
    t = t.strip()
    m = re.match(r"^(?:AI)?(?:深度)?(?:研究|產研)[\s:：]*(\S.*)$", t)
    if m:
        return m.group(1), False
    m = re.match(r"^(.{1,12}?)\s*(?:產研|研究)$", t)
    if m and m.group(1).strip():
        return m.group(1).strip(), True
    return None


NEED_MENTION = os.environ.get("LINE_NEED_MENTION", "1").strip() != "0"   # 群組／多人聊天室要「@飆成精」才回應；設 0 可恢復舊行為


def _norm_cmd(rest):
    """@後面剩下的文字 → 指令。4～6 碼數字：00 開頭是 ETF（原樣交給 ETF 查詢），其他是個股產研。"""
    rest = rest.strip()
    if not rest:
        return STRATEGY_KEYWORD
    if rest.startswith("重算"):
        return STRATEGY_KEYWORD + " " + rest
    if re.fullmatch(r"\d{4,6}[A-Za-z]?", rest) and not rest.startswith("00"):
        return "產研" + rest
    return rest


def _strip_mention(ev, t):
    """群組／聊天室：回傳 (是否叫了機器人, 指令文字)。一對一聊天不需要 @。
    群組叫法：「@飆成精」「@2317」（個股產研）「@0050」（ETF）「@偉詮電」（公司名產研，找不到不回），
    或用 LINE 選單點選機器人再接指令。"""
    src = (ev.get("source", {}).get("type") or "")
    t = t.strip()
    plain = re.match(r"^[@＠]\s*(\S+)\s*(.*)$", t)
    if plain and not plain.group(1).startswith(STRATEGY_KEYWORD):
        # 「@5340產研」「@5340 產研」「@產研5340」「@2317」「@偉詮電」都接受：先拿掉「產研／研究」字樣，再取第一個詞
        body = re.sub(r"(?:AI)?(?:深度)?(?:產研|研究)", " ", plain.group(1) + " " + plain.group(2)).split()
        tok = body[0] if body else ""
        if tok in NEWS_WORDS:
            return True, tok
        if re.fullmatch(r"\d{4,6}[A-Za-z]?", tok):
            return True, _norm_cmd(tok)
        if tok and not (ev.get("message", {}).get("mention") or {}).get("mentionees"):
            return True, tok + "產研"   # @公司名：安靜模式，找不到公司就不回應
    if src not in ("group", "room") or not NEED_MENTION:
        return True, re.sub(r"^[@＠]\s*", "", t) if plain else t
    msg = ev.get("message", {})
    hit = False
    cut = []
    for m in (msg.get("mention") or {}).get("mentionees", []) or []:
        if m.get("isSelf"):
            hit = True
            cut.append((m.get("index", 0), m.get("length", 0)))
    for i, l in sorted(cut, reverse=True):
        t = t[:i] + t[i + l:]
    if re.search(r"[@＠]\s*" + STRATEGY_KEYWORD, t):
        hit = True
        t = re.sub(r"[@＠]\s*" + STRATEGY_KEYWORD, "", t)
    if not hit:
        return False, t
    return True, _norm_cmd(t)


def handle_events(app, payload):
    for ev in payload.get("events", []):
        try:
            if ev.get("type") != "message" or ev.get("message", {}).get("type") != "text":
                continue
            t = (ev["message"].get("text") or "").strip()
            tok = ev.get("replyToken")
            log("msg", src=(ev.get("source", {}).get("type") or "?"), text=t[:30])
            called, t = _strip_mention(ev, t)
            if not called:
                continue   # 群組閒聊沒有 @飆成精，不回應
            if STRATEGY_KEYWORD in t:
                force = ("重算" in t)
                dd = latest_data_date(app)
                if (not force) and _RECENT["text"] and time.time() - _RECENT["t"] < RECENT_SECONDS:
                    reply(tok, _RECENT["text"] + "\n（%d 秒前算的；要重新連線請輸入「飆成精 重算」）" % int(time.time() - _RECENT["t"]))
                    continue
                tgt = _target(ev)
                with _LIVE["lock"]:
                    if tgt:
                        _LIVE["targets"].add(tgt)
                    if _LIVE["running"] and time.time() - _LIVE.get("since", 0) > 360:
                        _LIVE["running"] = False   # 上一次計算卡超過 6 分鐘，視為失敗，重新開始
                        log("stuck_reset")
                    start = not _LIVE["running"]
                    if start:
                        _LIVE["running"] = True
                        _LIVE["since"] = time.time()
                reply(tok, "收到，正在連線平台選股並計算個股分數，約 1～2 分鐘，完成後會傳到這裡。" if start
                      else "已有一份計算進行中，完成後會一併傳到這裡。")
                if start:
                    threading.Thread(target=_live_compute, args=(app, dd), daemon=True).start()
            elif RESEARCH_ENABLED and t.strip() in NEWS_WORDS:
                msg = news_request(app, tok, _target(ev))
                if msg:
                    reply(tok, msg)
            elif RESEARCH_ENABLED and _research_trigger(t) is not None:
                term, quiet = _research_trigger(t)
                msg = research_request(app, tok, _target(ev), term, quiet=quiet)
                if msg:
                    reply(tok, msg)
            elif t not in ("說明", "help", "?", "？") and (etf_msg := etf_lookup(app, t)) is not None:
                reply(tok, etf_msg)
            elif t in ("說明", "help", "?", "？"):
                reply(tok, "（群組裡請用 @ 叫我：「@飆成精」＝選股、「@2317」＝個股產研、「@0050」＝ETF 配息；一對一聊天不用 @）\n輸入「飆成精」：連線平台盤中即時選股，回覆個股分數前三名（約 1～2 分鐘）；3 分鐘內算過會立刻回覆。\n輸入「飆成精 重算」：不用舊結果，重新連線。\n輸入 ETF 代號或名稱（例如 0056、00878、高股息）：回覆配息金額、除息日、發放日。" + ("\n輸入「@財經」：今日全球財經重點（每天只查一次，之後直接回覆）。\n輸入「@2436」或「@偉詮電」：AI 上網整理深度產業研究（1～3 分鐘，每天有次數上限；上網額度用完時改出不上網簡版）。" if RESEARCH_ENABLED else ""))
        except Exception as e:
            print("LINE 事件處理失敗", type(e).__name__, e)
            log("handler_error", err=type(e).__name__ + ": " + str(e)[:200])

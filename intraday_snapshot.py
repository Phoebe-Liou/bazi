"""盤中即時報價快照 → public.intraday_quotes（由 GitHub Actions 盤中約每 5 分鐘執行）。
原因：Render 連不上證交所 MIS（連線被關閉），但 GitHub Actions 連得上。
作法：Actions 抓全市場即時報價存進資料庫；網站與 LINE 機器人在自己抓不到時，改讀這張表。
只存「今天的暫時日K」：開高低收、累計量（股）、報價時間。拿不到成交價的股票不存，不用 0 代替。
用法：python intraday_snapshot.py          （寫入）
      python intraday_snapshot.py dry      （只抓不寫，印樣本）"""
import json, os, sys, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
import psycopg2, psycopg2.extras

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
API = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&_=%d&ex_ch=%s"


def fnum(rec, k):
    try:
        return float(str(rec.get(k)).replace(",", ""))
    except Exception:
        return None


def fetch(ids):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(API % (int(time.time() * 1000), urllib.parse.quote(ids)),
                                         headers={"User-Agent": UA, "Referer": "https://mis.twse.com.tw/stock/fibest.jsp",
                                                  "X-Requested-With": "XMLHttpRequest"})
            return json.loads(urllib.request.urlopen(req, timeout=20).read().decode("utf-8", "replace"))
        except Exception as e:
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise last


def main():
    dry = len(sys.argv) > 1 and sys.argv[1] == "dry"
    conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15, sslmode="require")
    with conn.cursor() as cur:
        cur.execute("SELECT stock_id, market FROM public.stocks WHERE market IN ('上市','上櫃')")
        stocks = cur.fetchall()
    ids = ["%s_%s.tw" % ("tse" if m == "上市" else "otc", c) for c, m in stocks]
    ids.append("tse_t00.tw")  # 加權指數
    rows, errors, qtime = {}, [], ""
    stat = {"回傳": 0, "無成交價z": 0, "無法估算": 0, "缺開高低": 0, "日期異常": 0}
    nz_sample = []

    deadline = time.time() + 270   # 整個抓取最多 4.5 分鐘，超過就用已拿到的先寫入

    def fetch_one(chunk):
        if time.time() > deadline:
            return chunk, None, "逾時略過"
        try:
            payload = fetch("|".join(chunk))
            if payload.get("rtcode") != "0000":
                raise ValueError(payload.get("rtmessage", "行情來源回傳錯誤"))
            return chunk, payload, ""
        except Exception as e:
            return chunk, None, str(e)[:80]

    def collect(idlist):
        """分批併發抓（3 條）；回傳『這次沒拿到成交價』的代號清單。"""
        nonlocal qtime
        missing = []
        chunks = [idlist[i:i + 50] for i in range(0, len(idlist), 50)]
        with ThreadPoolExecutor(3) as ex:
            results = list(ex.map(fetch_one, chunks))
        for chunk, payload, err in results:
            if payload is None:
                if err:
                    errors.append(err)
                continue
            for rec in payload.get("msgArray", []):
                stat["回傳"] += 1
                code = str(rec.get("c")); d = str(rec.get("d") or "")
                z, o, h, l, v = (fnum(rec, k) for k in ("z", "o", "h", "l", "v"))
                if len(d) != 8:
                    stat["日期異常"] += 1; continue
                est = False
                if z is None:
                    stat["無成交價z"] += 1
                    if len(nz_sample) < 3:
                        nz_sample.append({k: rec.get(k) for k in ("c", "z", "pz", "o", "h", "l", "v", "tv", "y", "t", "a", "b")})
                    missing.append(("tse_" if rec.get("ex") == "tse" else "otc_") + code + ".tw")
                    # 沒有成交價：用最佳買進價估算（價差過大就不估），並標記為估算
                    try:
                        a1 = float(str(rec.get("a", "")).split("_")[0]); b1 = float(str(rec.get("b", "")).split("_")[0])
                    except Exception:
                        a1 = b1 = 0.0
                    mid = (a1 + b1) / 2 if a1 > 0 and b1 > 0 and a1 >= b1 else None
                    if mid is None or (a1 - b1) / mid > 0.015:
                        stat["無法估算"] += 1
                        continue
                    z = b1; est = True
                if None in (o, h, l):
                    stat["缺開高低"] += 1; continue
                if code == "t00":
                    code = "TAIEX"; shares = None
                else:
                    shares = (v or 0) * 1000
                t = str(rec.get("t") or "")
                qtime = max(qtime, t)
                if est and code in rows and not rows[code][-1]:
                    continue   # 已經有真實成交價，不用估算值覆蓋
                rows[code] = ("%s-%s-%s" % (d[:4], d[4:6], d[6:]), o, h, l, z, shares, t, est)
        print("本輪完成：已有報價 %d 檔，沒成交價 %d 檔，錯誤 %d 次" % (len(rows), len(missing), len(errors)), flush=True)
        return missing

    missing = collect(ids)
    # 證交所的成交價 z 只在「該檔剛成交」時才有；沒拿到的補抓一輪
    if missing and time.time() < deadline - 60:
        time.sleep(5)
        stat["回傳"] = 0
        missing = collect(missing)
    rows = [(c,) + v for c, v in rows.items()]
    print("股票數 %d，取得報價 %d 檔，最新報價時間 %s，錯誤 %s" % (len(stocks), len(rows), qtime or "—", errors[:3]))
    print("統計：", stat)
    for x in nz_sample:
        print("無成交價樣本：", x)
    for r in rows[:3]:
        print("樣本：", r)
    if dry or not rows:
        print("未寫入（dry 或沒有資料）")
        return
    with conn, conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS public.intraday_quotes (
            stock_id text PRIMARY KEY, trade_date date NOT NULL, open numeric, high numeric, low numeric,
            close numeric, volume_shares numeric, quote_time text, updated_at timestamptz NOT NULL DEFAULT now())""")
        cur.execute("ALTER TABLE public.intraday_quotes ADD COLUMN IF NOT EXISTS est boolean NOT NULL DEFAULT false")
        psycopg2.extras.execute_values(cur, """INSERT INTO public.intraday_quotes
            (stock_id, trade_date, open, high, low, close, volume_shares, quote_time, est, updated_at) VALUES %s
            ON CONFLICT (stock_id) DO UPDATE SET trade_date=EXCLUDED.trade_date, open=EXCLUDED.open, high=EXCLUDED.high,
              low=EXCLUDED.low, close=EXCLUDED.close, volume_shares=EXCLUDED.volume_shares,
              quote_time=EXCLUDED.quote_time, est=EXCLUDED.est, updated_at=now()""",
            rows, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,now())")
    print("已寫入 %d 筆（其中 %d 筆為最佳買進價估算）" % (len(rows), sum(1 for r in rows if r[-1])))


if __name__ == "__main__":
    main()

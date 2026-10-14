import base64, hmac, hashlib
import json, os, re, math, time, urllib.request, urllib.parse, urllib.error, sqlite3, threading, html
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2=None
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from contextlib import contextmanager
import market_feeds
import health_engine, industry_map, statistics, financial_feeds, etf_research_tools, mops_history, taiex_feed, rotation_engine, health_scoring, estimates, debt_quality, roe_ttm, us_tw_link, taifex_night, risk_gauges, flow_radar, heat_monitor

ROOT=Path(__file__).parent
PORT=int(os.environ.get("PORT","8000"))
CACHE={}
TTL=900
FINMIND_GUARD={"cooldown_until":0.0,"reason":"","last_error":"","last_ok":0.0}
FINMIND_COOLDOWN_SECONDS=3600
WAREHOUSE_JOB={"running":False,"stop":False,"requests":0,"rows_updated":0,"errors":0,"started_at":None,"last_code":"","message":"尚未開始"}
WAREHOUSE_THREAD=None
REVENUE_JOB={"running":False,"rows_updated":0,"stocks_covered":0,"latest_date":None,"message":"尚未開始","started_at":None,"finished_at":None,"requests":0,"failed":0,"pending":None,"last_code":""}
REVENUE_THREAD=None
CHIP_JOB={"running":False,"rows_updated":0,"message":"尚未更新","started_at":None,"finished_at":None}
CHIP_THREAD=None
SELECTED_JOB={"running":False,"stocks_done":0,"stocks_total":0,"rows_updated":0,"current_code":"","message":"尚未開始","started_at":None,"finished_at":None}
SELECTED_THREAD=None
INTRADAY_JOB={'running':False,'message':'尚未擷取盤中快照','done':0,'total':0,'last_started':0}
INTRADAY_LOCK=threading.Lock()

class FinMindCooldown(RuntimeError): pass

def finmind_guard_status():
    now=time.time(); left=max(0,int(FINMIND_GUARD["cooldown_until"]-now))
    cooling=left>0
    return {
        "cooling":cooling,
        "retry_after_seconds":left,
        "retry_at":datetime.fromtimestamp(FINMIND_GUARD["cooldown_until"]).isoformat(timespec="minutes") if cooling else None,
        "reason":FINMIND_GUARD["reason"] if cooling else "",
        "last_error":FINMIND_GUARD["last_error"] if cooling else "",
        "last_ok":datetime.fromtimestamp(FINMIND_GUARD["last_ok"]).isoformat(timespec="seconds") if FINMIND_GUARD["last_ok"] else None
    }

def finmind_trip(reason):
    FINMIND_GUARD["cooldown_until"]=time.time()+FINMIND_COOLDOWN_SECONDS
    FINMIND_GUARD["reason"]=reason
    FINMIND_GUARD["last_error"]=reason

def looks_rate_limited(status, text):
    t=(text or "").lower()
    return status in (402,429) or any(x in t for x in [
        "upper limit","rate limit","too many request","too many requests",
        "request limit","api limit","quota","頻率","額度","上限"
    ])


def fetch_json(url, timeout=25):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
    with urllib.request.urlopen(req,timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))

def num(x):
    try:
        if x is None:return None
        s=str(x).replace(",","").replace("%","").strip()
        if s in ("","--","-"):return None
        return float(s)
    except:return None

DATABASE_URL=os.environ.get("DATABASE_URL","").strip()
DB_PATH=os.environ.get("STOCK_DB_PATH","/tmp/strong_stock_radar_v18.sqlite3")
USE_POSTGRES=bool(DATABASE_URL)
DB_LOCK=threading.RLock()

_DB_POOL=[]            # 閒置連線：(連線, 放回時間)。每次重新連 Supabase（TCP＋TLS＋驗證）要數百毫秒，體檢一次要開三十幾次，所以重複使用
_DB_POOL_LOCK=threading.Lock()
_DB_POOL_MAX=6
_DB_POOL_IDLE=120

def _db_connect():
    conn=psycopg2.connect(DATABASE_URL, connect_timeout=12, sslmode="require")
    try:
        with conn.cursor() as _c:_c.execute("SET search_path TO public")   # 連線池可能不帶預設 search_path，未加 public. 的表名會找不到
        conn.commit()
    except Exception:
        conn.rollback()
    return conn

@contextmanager
def db_conn():
    pooled=False
    if USE_POSTGRES:
        if psycopg2 is None: raise RuntimeError("DATABASE_URL 已設定，但缺少 psycopg2-binary")
        conn=None
        while conn is None:
            with _DB_POOL_LOCK:
                item=_DB_POOL.pop() if _DB_POOL else None
            if item is None:break
            cand,stamp=item
            if cand.closed or time.time()-stamp>_DB_POOL_IDLE:
                try:cand.close()
                except Exception:pass
                continue
            try:                      # 取出時確認連線還活著；伺服器已斷線就丟掉換新的
                with cand.cursor() as _c:_c.execute("SELECT 1")
                cand.rollback();conn=cand
            except Exception:
                try:cand.close()
                except Exception:pass
        if conn is None:conn=_db_connect()
        pooled=True
    else:
        conn=sqlite3.connect(DB_PATH,timeout=30);conn.row_factory=sqlite3.Row;conn.execute("PRAGMA journal_mode=WAL")
    try:
        with conn:yield conn
    finally:
        keep=False
        if pooled:
            try:keep=(not conn.closed) and conn.status==psycopg2.extensions.STATUS_READY
            except Exception:keep=False
        if keep:
            with _DB_POOL_LOCK:
                if len(_DB_POOL)<_DB_POOL_MAX:
                    _DB_POOL.append((conn,time.time()));keep=True
                else:keep=False
        if not keep:
            try:conn.close()
            except Exception:pass

def db_init():
    if USE_POSTGRES:
        with DB_LOCK, db_conn() as c:
            with c.cursor() as cur:
                cur.execute("""CREATE TABLE IF NOT EXISTS public.saved_strategies (
                    strategy_name text PRIMARY KEY, payload jsonb NOT NULL,
                    created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
                )""")
                cur.execute("""CREATE TABLE IF NOT EXISTS public.warehouse_failures (
                    stock_id text PRIMARY KEY, error_text text, fail_count integer NOT NULL DEFAULT 1,
                    last_failed_at timestamptz NOT NULL DEFAULT now(), retry_after timestamptz NOT NULL DEFAULT now() + interval '30 minutes'
                )""")
                cur.execute("""CREATE TABLE IF NOT EXISTS public.monthly_revenue (
                    stock_id text NOT NULL, date date NOT NULL, revenue numeric,
                    revenue_year integer, revenue_month integer, source_name text NOT NULL DEFAULT 'FinMind',
                    updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY(stock_id,date)
                )""")
                cur.execute("""CREATE TABLE IF NOT EXISTS public.revenue_failures (
                    stock_id text PRIMARY KEY, error_text text,
                    last_failed_at timestamptz NOT NULL DEFAULT now(),
                    retry_after timestamptz NOT NULL DEFAULT now() + interval '1 day'
                )""")
                cur.execute("""CREATE TABLE IF NOT EXISTS public.market_chips (
                    dataset text NOT NULL, trade_date date NOT NULL, category text NOT NULL,
                    buy numeric, sell numeric, today_balance numeric, yesterday_balance numeric,
                    source_name text NOT NULL DEFAULT 'FinMind', updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY(dataset,trade_date,category)
                )""")
                cur.execute("""CREATE TABLE IF NOT EXISTS public.institutional_trading (
                    stock_id text NOT NULL, trade_date date NOT NULL,
                    foreign_net numeric, trust_net numeric, dealer_net numeric,
                    source_name text NOT NULL DEFAULT 'FinMind', updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY(stock_id,trade_date)
                )""")
                cur.execute("""CREATE TABLE IF NOT EXISTS public.financials (
                    stock_id text NOT NULL, period_date date NOT NULL, eps numeric,
                    gross_margin numeric, operating_margin numeric,
                    source_name text NOT NULL DEFAULT 'FinMind', updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY(stock_id,period_date)
                )""")
                cur.execute("""CREATE TABLE IF NOT EXISTS public.margin_short (
                    stock_id text NOT NULL, trade_date date NOT NULL,
                    margin_balance numeric, short_balance numeric,
                    source_name text NOT NULL DEFAULT 'FinMind', updated_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY(stock_id,trade_date)
                )""")
        return
    with DB_LOCK,db_conn() as c:c.executescript("""CREATE TABLE IF NOT EXISTS stocks(code TEXT PRIMARY KEY,name TEXT,market TEXT,industry TEXT,updated_at TEXT);CREATE TABLE IF NOT EXISTS daily_prices(code TEXT NOT NULL,date TEXT NOT NULL,open REAL,high REAL,low REAL,close REAL,volume REAL,turnover REAL,PRIMARY KEY(code,date));CREATE INDEX IF NOT EXISTS idx_daily_code_date ON daily_prices(code,date);CREATE TABLE IF NOT EXISTS warehouse_meta(key TEXT PRIMARY KEY,value TEXT);CREATE TABLE IF NOT EXISTS saved_strategies(strategy_name TEXT PRIMARY KEY,payload TEXT NOT NULL,updated_at TEXT);CREATE TABLE IF NOT EXISTS monthly_revenue(stock_id TEXT,date TEXT,revenue REAL,revenue_year INTEGER,revenue_month INTEGER,source_name TEXT,PRIMARY KEY(stock_id,date));CREATE TABLE IF NOT EXISTS market_chips(dataset TEXT,trade_date TEXT,category TEXT,buy REAL,sell REAL,today_balance REAL,yesterday_balance REAL,source_name TEXT,updated_at TEXT,PRIMARY KEY(dataset,trade_date,category));""")

def db_meta_get(k,d=""):
    if USE_POSTGRES:
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT extra_info FROM public.data_sync_status WHERE dataset_name=%s",(k,));r=cur.fetchone()
                return (r.get("extra_info") or {}).get("value",d) if r else d
    with db_conn() as c:
        r=c.execute("SELECT value FROM warehouse_meta WHERE key=?",(k,)).fetchone();return r["value"] if r else d

def db_meta_set(k,v):
    if USE_POSTGRES:
        with DB_LOCK,db_conn() as c:
            with c.cursor() as cur:cur.execute("""INSERT INTO public.data_sync_status(dataset_name,source_name,status,last_sync_at,extra_info)
                VALUES(%s,'app','ok',now(),%s::jsonb) ON CONFLICT(dataset_name) DO UPDATE SET last_sync_at=now(),status='ok',extra_info=EXCLUDED.extra_info""",(k,json.dumps({"value":str(v)})))
        return
    with DB_LOCK,db_conn() as c:c.execute("INSERT INTO warehouse_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(k,str(v)))

def db_upsert_stocks(rows):
    now=datetime.now().isoformat(timespec="seconds")
    if USE_POSTGRES:
        vals=[(str(x.get("code","")),x.get("name","") or "",x.get("market","") or "",x.get("industry","") or "") for x in rows if x.get("code")]
        with DB_LOCK,db_conn() as c:
            with c.cursor() as cur: psycopg2.extras.execute_values(cur,"""INSERT INTO public.stocks(stock_id,stock_name,market,industry_name) VALUES %s
                ON CONFLICT(stock_id) DO UPDATE SET stock_name=EXCLUDED.stock_name,market=EXCLUDED.market,industry_name=EXCLUDED.industry_name,updated_at=now()""",vals,page_size=500)
        return
    vals=[(str(x.get("code","")),x.get("name",""),x.get("market",""),x.get("industry",""),now) for x in rows if x.get("code")]
    with DB_LOCK,db_conn() as c:c.executemany("INSERT INTO stocks(code,name,market,industry,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(code) DO UPDATE SET name=excluded.name,market=excluded.market,industry=excluded.industry,updated_at=excluded.updated_at",vals)

def db_upsert_history(code,rows):
    vals=[(code,x["date"],num(x.get("open")),num(x.get("max")),num(x.get("min")),num(x.get("close")),num(x.get("Trading_Volume")),num(x.get("Trading_money"))) for x in rows if x.get("date")]
    if not vals:return 0
    if USE_POSTGRES:
        with DB_LOCK,db_conn() as c:
            with c.cursor() as cur: psycopg2.extras.execute_values(cur,"""INSERT INTO public.daily_prices(stock_id,trade_date,open_price,high_price,low_price,close_price,volume,turnover) VALUES %s
                ON CONFLICT(stock_id,trade_date) DO UPDATE SET open_price=EXCLUDED.open_price,high_price=EXCLUDED.high_price,low_price=EXCLUDED.low_price,close_price=EXCLUDED.close_price,volume=EXCLUDED.volume,turnover=EXCLUDED.turnover,updated_at=now()""",vals,page_size=1000)
    else:
        with DB_LOCK,db_conn() as c:c.executemany("INSERT INTO daily_prices(code,date,open,high,low,close,volume,turnover) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(code,date) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume,turnover=excluded.turnover",vals)
    return len(vals)

def db_upsert_monthly_revenue(rows):
    """Persist FinMind's all-company month revenue in bounded batches."""
    vals=[]
    for x in rows:
        code=str(x.get('stock_id') or '').strip();day=str(x.get('date') or '')[:10]
        if not code or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',day):continue
        rev=num(x.get('revenue'))
        if rev is None:continue
        vals.append((code,day,rev,int(x['revenue_year']) if x.get('revenue_year') else None,
                     int(x['revenue_month']) if x.get('revenue_month') else None,'FinMind'))
    if not vals:return 0
    with DB_LOCK,db_conn() as c:
        if USE_POSTGRES:
            schema=revenue_schema()
            names=[schema['code']];indexes=[0];casts=['text']
            date_text=schema['date_type'] in ('text','character varying','character')
            if schema.get('date'):
                names.append(schema['date']);indexes.append(1)
                casts.append('text' if date_text else 'date')
            names.append(schema['revenue']);indexes.append(2);casts.append('numeric')
            for key,ix in [('year',3),('month',4),('source',5)]:
                if schema.get(key):
                    names.append(schema[key]);indexes.append(ix)
                    casts.append({'year':'integer','month':'integer','source':'text'}[key])
            if not schema.get('year') or not schema.get('month'):
                vals=[(r[0],f'{r[3]:04d}-{r[4]:02d}-01',*r[2:]) if r[3] and r[4] else r for r in vals]
            if date_text and schema['date'] in ('revenue_month','year_month'):
                vals=[(r[0],r[1][:7],*r[2:]) for r in vals]
            if not schema.get('date'):
                if any(not r[3] or not r[4] or not 1<=r[4]<=12 for r in vals):
                    raise ValueError('來源缺少有效的營收年度或月份，未寫入年月分欄資料表')
            vals=[tuple(row[ix] for ix in indexes) for row in vals]
            quoted=','.join('"'+name+'"' for name in names)
            table='public.monthly_revenue'
            key_names=[schema['code']]+([schema['date']] if schema.get('date') else [schema['year'],schema['month']])
            match=' AND '.join('t."'+name+'"=v."'+name+'"' for name in key_names)
            assignments=','.join('"'+name+'"=v."'+name+'"' for name in names if name not in key_names)
            if schema.get('updated'):assignments+=',"'+schema['updated']+'"=now()'
            template='('+','.join('%s::'+cast for cast in casts)+')'
            with c.cursor() as cur:
                # Existing warehouses may use revenue_date instead of date and
                # may lack optional columns. Preserve their columns and key.
                sql='WITH v('+quoted+') AS (VALUES %s), changed AS (UPDATE '+table+' t SET '+assignments+' FROM v WHERE '+match+' RETURNING 1) INSERT INTO '+table+' ('+quoted+') SELECT '+quoted+' FROM v WHERE NOT EXISTS(SELECT 1 FROM '+table+' t WHERE '+match+') ON CONFLICT DO NOTHING'
                psycopg2.extras.execute_values(cur,sql,vals,template=template,page_size=1000)
        else:c.executemany('''INSERT INTO monthly_revenue(stock_id,date,revenue,revenue_year,revenue_month,source_name)
            VALUES(?,?,?,?,?,?) ON CONFLICT(stock_id,date) DO UPDATE SET revenue=excluded.revenue''',vals)
    return len(vals)

def db_history(code,limit=600):
    if USE_POSTGRES:
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT trade_date AS date,open_price AS open,high_price AS high,low_price AS low,close_price AS close,volume,turnover FROM public.daily_prices WHERE stock_id=%s ORDER BY trade_date DESC LIMIT %s",(code,limit));r=cur.fetchall()
        out=[dict(x) for x in reversed(r)]
        for x in out:
            x["date"]=x["date"].isoformat() if hasattr(x["date"],"isoformat") else str(x["date"])
            for k in ("open","high","low","close","volume","turnover"):
                if x.get(k) is not None:x[k]=float(x[k])
        return out
    with db_conn() as c:r=c.execute("SELECT date,open,high,low,close,volume,turnover FROM daily_prices WHERE code=? ORDER BY date DESC LIMIT ?",(code,limit)).fetchall()
    return [dict(x) for x in reversed(r)]

def db_histories_for_universe(base):
    """Stream all warehouse prices over one DB connection, instead of 1963 SSL round trips."""
    allowed={str(x['code']) for x in base}
    if not allowed:return
    if USE_POSTGRES:
        with db_conn() as c:
            with c.cursor(name='screen_price_stream',cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.itersize=3000
                cur.execute('''SELECT stock_id,trade_date AS date,open_price AS open,
                    high_price AS high,low_price AS low,close_price AS close,volume,turnover
                    FROM public.daily_prices WHERE stock_id=ANY(%s)
                    ORDER BY stock_id,trade_date''',(list(allowed),))
                code=None;rows=[]
                for record in cur:
                    x=dict(record);new_code=str(x.pop('stock_id'))
                    if code is not None and new_code!=code:
                        yield code,rows[-600:];rows=[]
                    code=new_code;x['date']=str(x['date'])
                    for k in ('open','high','low','close','volume','turnover'):
                        if x.get(k) is not None:x[k]=float(x[k])
                    rows.append(x)
                if code is not None:yield code,rows[-600:]
    else:
        with db_conn() as c:
            cur=c.execute('SELECT code,date,open,high,low,close,volume,turnover FROM daily_prices ORDER BY code,date')
            code=None;rows=[]
            for record in cur:
                x=dict(record);new_code=x.pop('code')
                if new_code not in allowed:continue
                if code is not None and new_code!=code:yield code,rows[-600:];rows=[]
                code=new_code;rows.append(x)
            if code is not None:yield code,rows[-600:]

def db_universe(market="both"):
    if USE_POSTGRES:
        q="SELECT stock_id AS code,stock_name AS name,market,COALESCE(industry_name,'') AS industry FROM public.stocks";args=[]
        if market=="listed":q+=" WHERE market='上市'"
        elif market=="otc":q+=" WHERE market='上櫃'"
        q+=" ORDER BY stock_id"
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:cur.execute(q,args);return [dict(x) for x in cur.fetchall()]
    q="SELECT code,name,market,industry FROM stocks"
    if market=="listed":q+=" WHERE market='上市'"
    elif market=="otc":q+=" WHERE market='上櫃'"
    q+=" ORDER BY code"
    with db_conn() as c:return [dict(x) for x in c.execute(q)]

def search_stock_research(query):
    term=str(query or '').strip()[:35]
    if not term:return {'ok':True,'rows':[],'message':'輸入代號或公司名稱'}
    if USE_POSTGRES:
        # Parameters escape LIKE metacharacters too: searches are literal text.
        escaped=term.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""SELECT s.stock_id AS code,s.stock_name AS name,s.market,
                  COALESCE(s.industry_name,'') AS industry,p.trade_date AS price_date,
                  p.close_price AS close,p.volume
                  FROM public.stocks s LEFT JOIN LATERAL (
                    SELECT trade_date,close_price,volume FROM public.daily_prices
                    WHERE stock_id=s.stock_id ORDER BY trade_date DESC LIMIT 1
                  ) p ON true
                  WHERE s.stock_id ~ '^[0-9]{4}$' AND s.stock_id NOT LIKE '00%%'
                  AND (s.stock_id LIKE %s ESCAPE '\\' OR s.stock_name ILIKE %s ESCAPE '\\')
                  ORDER BY CASE WHEN s.stock_id=%s THEN 0 WHEN s.stock_id LIKE %s ESCAPE '\\' THEN 1 ELSE 2 END,s.stock_id
                  LIMIT 20""",(escaped+'%','%'+escaped+'%',term,escaped+'%'))
                rows=[dict(row) for row in cur.fetchall()]
    else:
        rows=[dict(x) for x in db_universe() if len(str(x['code']))==4 and str(x['code']).isdigit() and not str(x['code']).startswith('00') and (term in str(x['code']) or term.casefold() in str(x.get('name','')).casefold())][:20]
        for row in rows:
            h=db_history(row['code'],1);last=h[-1] if h else {}
            row.update(price_date=last.get('date'),close=last.get('close'),volume=last.get('volume'))
    for row in rows:
        row['price_date']=str(row['price_date']) if row.get('price_date') else None
        row['close']=num(row.get('close'));row['volume']=num(row.get('volume'))
    for r in rows:
        try:
            h=health_from_hist(db_history(r['code'],260))
            r['health']={'score':h.get('score'),'level':h.get('level'),'coverage':h.get('coverage')}
        except Exception:r['health']=None
    try:
        _tags=flow_radar.annotate(__import__('sys').modules[__name__],[r['code'] for r in rows]) if rows else {}
        for r in rows:r['flow']=_tags.get(str(r['code']))
    except Exception:pass
    return {'ok':True,'rows':rows,'message':f'找到 {len(rows)} 筆（最多20筆）；收盤價以資料日期為準，點體檢可看健檢分數與缺項。'}


# ---------- 個股 AI 深度研究（Gemini＋Google 搜尋；金鑰放 Render 環境變數 GEMINI_API_KEY） ----------
RESEARCH_STATE={}          # code -> {'status','text','sources','error','at','model'}
RESEARCH_LOCK=threading.Lock()
RESEARCH_DAY={'day':'','n':0}

def _tw_today():return datetime.now(ZoneInfo('Asia/Taipei')).date().isoformat()

def _research_cap():
    try:return int(os.environ.get('GEMINI_DAILY_MAX','20') or 20)
    except Exception:return 20

def research_used(inc=0):
    """今天已用幾次（台北日期）。有資料庫就存資料庫，重新部署不會歸零；否則用記憶體。"""
    day=_tw_today()
    if USE_POSTGRES:
        try:
            with db_conn() as c:
                with c.cursor() as cur:
                    cur.execute("CREATE TABLE IF NOT EXISTS public.ai_research_quota(day date PRIMARY KEY,n integer NOT NULL DEFAULT 0)")
                    if inc:cur.execute("INSERT INTO public.ai_research_quota(day,n) VALUES (%s,%s) ON CONFLICT(day) DO UPDATE SET n=public.ai_research_quota.n+%s",(day,inc,inc))
                    cur.execute("SELECT n FROM public.ai_research_quota WHERE day=%s",(day,))
                    r=cur.fetchone();return int(r[0]) if r else 0
        except Exception:pass
    if RESEARCH_DAY['day']!=day:RESEARCH_DAY.update(day=day,n=0)
    RESEARCH_DAY['n']+=inc
    return RESEARCH_DAY['n']

def research_quota():
    cap=_research_cap();used=research_used()
    return {'cap':cap,'used':used,'left':max(0,cap-used),'day':_tw_today()}

def _stock_basic(code):
    """只查這一檔的名稱、產業、最新收盤（輕量，不跑體檢與資金熱度）。"""
    try:
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""SELECT s.stock_id AS code,s.stock_name AS name,COALESCE(s.industry_name,'') AS industry,p.trade_date AS price_date,p.close_price AS close
                  FROM public.stocks s LEFT JOIN LATERAL (SELECT trade_date,close_price FROM public.daily_prices WHERE stock_id=s.stock_id ORDER BY trade_date DESC LIMIT 1) p ON true
                  WHERE s.stock_id=%s""",(code,))
                r=cur.fetchone()
        if not r:return {}
        r=dict(r);r['price_date']=str(r['price_date']) if r.get('price_date') else None;r['close']=num(r.get('close'));return r
    except Exception:return {}

def research_prompt(code,name,industry,close,price_date):
    ref='；'.join(x for x in (industry and '產業分類：'+industry, close and '本站資料庫收盤價：%s 元（%s）'%(close,price_date or '日期未記錄')) if x)
    return ("請幫我針對 [%s%s] 進行「全覆蓋」深度產業研究與投資價值評估，用繁體中文，格式如下：\n\n"
     "## 1. 核心業務與前瞻項目拆解\n- 基本盤業務（現有營收主力、應用領域、占比）。\n- 前瞻與跨界延伸項目表（技術細節、供應鏈定位、進度、毛利／營收潛力），請用表格。\n"
     "## 2. SWOT 戰略矩陣（優勢、劣勢、機會、威脅）\n"
     "## 3. 最新財報與法說會重點剖析（本業／業外獲利、毛利率趨勢、存貨／營運風險分析）\n"
     "## 4. 目標價調查與機構可信度（最新目標價、開出機構、機構評價與邏輯）\n"
     "## 5. 股價點位與進場策略建議（安全邊際評估、追高風險、分批／拉回進場區間）\n\n"
     "硬性規定：請用 Google 搜尋最新公開資料；每個數字、目標價、財報都標明來源與日期（財報標明是哪一季）；查不到就寫「查不到」，不可推測或編造；最後加一行「以上為 AI 整理，非投資建議」。%s")%(code,name,('\n\n參考（來自使用者自己的資料庫，請以最新公開資料為準）：'+ref+'。') if ref else '')

def _gemini_call(model,key,prompt,use_search,waits=(0,10,25),total_limit=240,on_text=None):
    """串流接收（alt=sse）：Gemini 邊寫邊傳，不必等整份寫完才收到第一個字，長報告不容易讀取逾時。"""
    body={'contents':[{'parts':[{'text':prompt}]}]}
    if use_search:body['tools']=[{'google_search':{}}]
    url='https://generativelanguage.googleapis.com/v1beta/models/%s:streamGenerateContent?alt=sse'%urllib.parse.quote(model)
    req=urllib.request.Request(url,data=json.dumps(body).encode(),method='POST',headers={'Content-Type':'application/json','x-goog-api-key':key})
    texts=[];sources=[];seen=set();finish=None;feedback=None;ok=False
    for attempt,wait in enumerate(waits):   # 503／500／504、逾時：自動重試
        if wait:time.sleep(wait)
        texts=[];sources=[];seen=set();finish=None;feedback=None;t0=time.time()
        try:
            with urllib.request.urlopen(req,timeout=100) as r:
                for raw in r:
                    if time.time()-t0>total_limit:raise TimeoutError('超過 %d 秒仍未寫完'%total_limit)
                    line=raw.decode('utf-8','ignore').strip()
                    if not line.startswith('data:'):continue
                    try:d=json.loads(line[5:].strip())
                    except Exception:continue
                    if d.get('error'):
                        er=d['error'];raise RuntimeError('Gemini 回應 %s：%s'%(er.get('code','?'),json.dumps(er,ensure_ascii=False)[:300]))
                    feedback=d.get('promptFeedback') or feedback
                    cand=(d.get('candidates') or [{}])[0]
                    for p in (cand.get('content') or {}).get('parts',[]):
                        if p.get('text') and not p.get('thought'):
                            texts.append(p['text'])
                            if on_text:
                                try:on_text(''.join(texts))
                                except Exception:pass
                    finish=cand.get('finishReason') or finish
                    for c_ in ((cand.get('groundingMetadata') or {}).get('groundingChunks') or []):
                        w=c_.get('web') or {}
                        if w.get('uri') and w['uri'] not in seen:seen.add(w['uri']);sources.append({'title':w.get('title') or w['uri'],'url':w['uri']})
            ok=True;break
        except urllib.error.HTTPError as e:
            if e.code in (500,503,504) and attempt<len(waits)-1:continue
            raise RuntimeError('Gemini 回應 %s：%s%s'%(e.code,e.read().decode()[:300],'（已自動重試 %d 次，Gemini 目前塞車，請 5～10 分鐘後再按）'%attempt if e.code in (500,503,504) else ''))
        except RuntimeError as e:
            if any(x in str(e) for x in ('回應 500','回應 503','回應 504')) and attempt<len(waits)-1:continue
            raise
        except (TimeoutError,OSError) as e:   # socket 逾時、連線中斷：當成 504 處理，才會觸發備援模型
            if attempt<len(waits)-1 and time.time()-t0<30:continue   # 很快就斷線才重試；等很久才逾時的不重試，直接換備援
            raise RuntimeError('Gemini 回應 504：連線逾時或中斷（%s）（已自動重試 %d 次）'%(str(e)[:80],attempt))
    text=''.join(texts)
    if not ok or not text.strip():raise RuntimeError('Gemini 沒有回傳內容（'+str(finish or feedback or '原因不明')[:120]+'）')
    return text,sources[:25]

def _alt_models(key,exclude):
    """向 Google 查這把金鑰能用的模型，挑 flash 系列當備援（排除語音／圖片／即時／嵌入類）。"""
    try:
        req=urllib.request.Request('https://generativelanguage.googleapis.com/v1beta/models?pageSize=200',headers={'x-goog-api-key':key})
        with urllib.request.urlopen(req,timeout=20) as r:d=json.loads(r.read().decode())
    except Exception:return []
    bad=('tts','image','live','audio','embedding','embed','vision','robotics','computer','imagen','veo','aqa','learnlm','gemma','native')
    out=[]
    for m in d.get('models',[]):
        n=str(m.get('name','')).replace('models/','')
        if 'generateContent' not in (m.get('supportedGenerationMethods') or []):continue
        if 'flash' not in n or any(b in n for b in bad) or n in exclude:continue
        out.append(n)
    def ver(n):
        m=re.search(r'gemini-(\d+(?:\.\d+)?)',n);return -float(m.group(1)) if m else 0
    out.sort(key=lambda n:('lite' in n,'preview' in n or 'exp' in n,ver(n),n))   # 先非 lite、正式版，再新版本優先
    return out[:2]

def _research_worker(code,prompt,fallback_prompt=None):
    key=os.environ.get('GEMINI_API_KEY','').strip();model=os.environ.get('GEMINI_MODEL','gemini-3.8-flash').strip()
    st=RESEARCH_STATE[code]
    try:
        used=model
        try:
            try:
                text,sources=_gemini_call(model,key,prompt,True,on_text=lambda t:st.__setitem__('partial',t))
            except RuntimeError as e0:
                if not any(x in str(e0) for x in ('回應 503','回應 500','回應 504')):raise
                last=e0;text=None   # 主模型塞車：換備援模型（各試一次，間隔短）
                code_of=lambda e:(re.search(r'回應 (\d+)',str(e)) or [None,'?'])[1]
                tried=[model+'→'+code_of(e0)]
                alts=_alt_models(key,{model})
                if not alts:tried.append('（查不到備援模型）')
                for alt in alts:
                    try:
                        text,sources=_gemini_call(alt,key,prompt,True,waits=(0,),total_limit=100,on_text=lambda t:st.__setitem__('partial',t));used=alt;break
                    except RuntimeError as e1:
                        last=e1;tried.append(alt+'→'+code_of(e1))
                if text is None:raise RuntimeError(str(last)[:260]+' ｜已依序嘗試：'+'、'.join(tried))
        except RuntimeError as e:
            # 免費方案常沒有 Google 搜尋額度（429／403）。改成「不上網」的有限版，並在報告最前面明講。
            if fallback_prompt and any(x in str(e) for x in ('回應 429','回應 403','回應 503','回應 500','回應 504','逾時')):
                es=str(e)
                why='Google 搜尋的免費額度今天用完了' if ('回應 429' in es or '回應 403' in es) else 'Gemini 目前塞車或回應逾時'
                text=None;lasterr=e
                for m_ in [used,model]+_alt_models(key,{used,model}):   # 簡版很短，逐一換模型試，各最多重試一次
                    try:
                        text,sources=_gemini_call(m_,key,fallback_prompt,False,waits=(0,8),total_limit=90);used=m_;break
                    except RuntimeError as e2:lasterr=e2
                if text is None:raise RuntimeError('上網版失敗（'+why+'），簡版也失敗：'+str(lasterr)[:200])
                text='⚠️ 【不上網版】'+why+'，所以這份沒有上網搜尋，只靠 AI 既有知識整理公司業務與 SWOT，內容可能過時或有誤；最新財報、法說會、目標價、進場點位本次不提供，請另外查證。想要上網版，等額度重置（約台北時間下午 3～4 點）或 Gemini 不塞車時再傳一次。\n\n'+text
            else:raise
        st.update(status='done',text=text,sources=sources,error=None,at=time.time(),model=used,partial=None)
    except Exception as exc:
        st.update(status='error',error=str(exc)[:400],at=time.time())
        try:research_used(inc=-1)   # 失敗的不算次數
        except Exception:pass

def research_prompt_light(code,name,industry):
    return ("請用繁體中文，簡要介紹 [%s%s]%s。你不能上網，只能憑既有知識回答；不確定的地方一律寫「不確定」，絕對不要編造數字、目標價或日期。格式：\n"
     "## 1. 核心業務\n- 基本盤業務（營收主力、應用領域；占比不確定就寫不確定）。\n- 前瞻／跨界延伸項目（用表格：項目、技術重點、供應鏈位置、進度是否確定）。\n"
     "## 2. SWOT 戰略矩陣\n"
     "最後只寫一行：「最新財報、目標價與進場點位需要上網查證，本版未提供。」")%(code,name,('（產業分類：'+industry+'）') if industry else '')

def stock_research(code,start=False):
    code=re.sub(r'[^0-9A-Za-z]','',str(code or ''))[:8]
    if not code:return {'ok':False,'error':'缺少代號'}
    if not os.environ.get('GEMINI_API_KEY','').strip():
        return {'ok':False,'error':'尚未設定 GEMINI_API_KEY。到 aistudio.google.com 取得免費金鑰，貼到 Render 的 Environment 後重新部署。','need_key':True}
    st=RESEARCH_STATE.get(code)
    if st and st['status']=='done' and time.time()-st['at']<6*3600:return dict(ok=True,**st,cached=True,quota=research_quota())   # 6 小時內重複查同一檔，直接給舊報告，不耗額度
    if st and st['status']=='running':
        if time.time()-st.get('started',st['at'])>600:   # 卡超過 4 分鐘：判定失敗並退還次數，不要讓畫面一直轉
            st.update(status='error',error='超過 10 分鐘沒有回應（可能是 Gemini 或網路卡住），請稍後再試；這次不算次數。',at=time.time())
            try:research_used(inc=-1)
            except Exception:pass
            return dict(ok=True,**st,quota=research_quota())
        return dict(ok=True,**st,quota=research_quota())
    if st and st['status']=='error' and not start and time.time()-st['at']<900:return dict(ok=True,**st,quota=research_quota())   # 讓畫面看得到失敗原因，不要誤判成「任務遺失」
    if not start:return {'ok':True,'status':'none','quota':research_quota()}
    cap=_research_cap()
    with RESEARCH_LOCK:
        if research_used()>=cap:return {'ok':False,'error':'今天的 AI 研究次數已用完（%d／%d 次），避免用光免費額度；台北時間明天零點後重新計算，或調高 GEMINI_DAILY_MAX。'%(cap,cap),'quota':research_quota()}
        if any(v.get('status')=='running' for v in RESEARCH_STATE.values()):return {'ok':False,'error':'目前有另一份研究在產生，請 1～2 分鐘後再試。'}
        research_used(inc=1)
        row=_stock_basic(code)
        RESEARCH_STATE[code]={'status':'running','text':None,'sources':[],'error':None,'at':time.time(),'started':time.time(),'model':None}
    threading.Thread(target=_research_worker,args=(code,research_prompt(code,row.get('name',''),row.get('industry',''),row.get('close'),row.get('price_date')),research_prompt_light(code,row.get('name',''),row.get('industry',''))),daemon=True).start()
    return dict(ok=True,**RESEARCH_STATE[code],quota=research_quota())

NEWS_STATE={}
def news_prompt(today):
    return ("## Role\n你是一位資深財經分析師與財經新聞主編。\n\n## Objective\n今天是台北時間 %s。請用 Google 搜尋，整理最近一個交易日（或今天）全球財經最重要的 3～5 則新聞與市場動態，產出一份「三分鐘內讀完」的繁體中文每日財經重點。\n\n"
     "## 範圍\n全球總經、美股／台股／亞股、匯率與油價、加密貨幣、重大地緣政治或政策。排除八卦娛樂、沒有數據支持的傳言、炒作式投資建議。\n\n"
     "## 硬性規定\n1. 只能寫搜尋到的真實新聞；每則標明來源媒體與日期；查不到就寫「查不到」，嚴禁虛構新聞或數字，不可用過期新聞充數。\n"
     "2. 數字與百分比要精確。\n3. 必須交代台股：加權指數收盤與漲跌、外資買賣超（查不到就寫查不到）。\n"
     "4. 語氣專業、客觀、中立，不給買賣建議。\n5. 全文控制在 1200～1500 字，不要用表格，多用條列與粗體。\n\n"
     "## 輸出格式\n### 📅 %s 每日財經重點速覽\n#### 一、今日市場總覽\n- **新聞標題**（來源、日期）\n  - 事件摘要：\n  - 市場影響：\n"
     "#### 二、關鍵總經數據與焦點（近期重要數據、央行動態、產業風向）\n#### 三、台股重點\n#### 四、分析師短評（1～2 段）\n最後加一行「以上為 AI 整理，非投資建議，請自行查證來源」。")%(today,today)
def _news_worker(day,prompt):
    key=os.environ.get('GEMINI_API_KEY','').strip();model=os.environ.get('GEMINI_MODEL','gemini-3.8-flash').strip()
    st=NEWS_STATE[day]
    try:
        text=None;last=None;used=model;sources=[]
        for i,m in enumerate([model]+_alt_models(key,{model})):
            try:
                text,sources=_gemini_call(m,key,prompt,True,waits=(0,10) if i==0 else (0,),total_limit=150);used=m;break
            except RuntimeError as e:
                last=e
                if not any(x in str(e) for x in ('回應 503','回應 500','回應 504','逾時')):break
        if text is None:raise RuntimeError(str(last)[:300])
        st.update(status='done',text=text,sources=sources,error=None,at=time.time(),model=used)
    except Exception as exc:
        st.update(status='error',error=str(exc)[:400],at=time.time())
        try:research_used(inc=-1)
        except Exception:pass

def daily_news(start=False):
    """每天只查一次：第一個人要的時候才去問 Gemini，之後當天所有人／群組都拿同一份，不再耗額度。"""
    day=_tw_today()
    if not os.environ.get('GEMINI_API_KEY','').strip():return {'ok':False,'error':'尚未設定 GEMINI_API_KEY'}
    st=NEWS_STATE.get(day)
    if st and st['status']=='done':return dict(ok=True,**st,cached=True)
    if st and st['status']=='running':
        if time.time()-st['started']>480:
            st.update(status='error',error='超過 8 分鐘沒有回應',at=time.time())
            try:research_used(inc=-1)
            except Exception:pass
        return dict(ok=True,**st)
    if st and st['status']=='error' and time.time()-st['at']<600:return {'ok':False,'error':st['error']}   # 剛失敗：10 分鐘內不重打，避免連續耗額度
    if not start:return {'ok':True,'status':'none'}
    cap=_research_cap()
    with RESEARCH_LOCK:
        if research_used()>=cap:return {'ok':False,'error':'今天的 AI 查詢次數已用完（%d／%d 次）'%(cap,cap)}
        research_used(inc=1)
        NEWS_STATE.clear()
        NEWS_STATE[day]={'status':'running','text':None,'sources':[],'error':None,'at':time.time(),'started':time.time(),'model':None}
    threading.Thread(target=_news_worker,args=(day,news_prompt(day)),daemon=True).start()
    return dict(ok=True,**NEWS_STATE[day])

def db_status():
    if USE_POSTGRES:
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT COUNT(*) n FROM public.stocks");n=cur.fetchone()["n"]
                cur.execute("SELECT COUNT(*) n,MIN(trade_date) mn,MAX(trade_date) mx FROM public.daily_prices");d=cur.fetchone()
                cur.execute("SELECT COUNT(*) n FROM information_schema.tables WHERE table_schema='public'");tables=cur.fetchone()["n"]
        return {"stocks":n,"bars":d["n"],"from":str(d["mn"] or ""),"to":str(d["mx"] or ""),"last_update":db_meta_get("last_update",""),"persistent":True,"engine":"PostgreSQL","connected":True,"tables":tables}
    with db_conn() as c:
        n=c.execute("SELECT COUNT(*) n FROM stocks").fetchone()["n"];b=c.execute("SELECT COUNT(*) n FROM daily_prices").fetchone()["n"];d=c.execute("SELECT MIN(date) mn,MAX(date) mx FROM daily_prices").fetchone()
    return {"stocks":n,"bars":b,"from":d["mn"],"to":d["mx"],"last_update":db_meta_get("last_update",""),"persistent":not DB_PATH.startswith("/tmp/"),"engine":"SQLite","connected":True,"tables":4}

def db_health():
    try:return {"ok":True,**db_status()}
    except Exception:return {"ok":False,"connected":False,"engine":"PostgreSQL" if USE_POSTGRES else "SQLite","error":"資料庫連線失敗，請檢查伺服器環境設定"}

def strategies_get():
    with db_conn() as c:
        if USE_POSTGRES:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:cur.execute("SELECT strategy_name,payload FROM public.saved_strategies ORDER BY updated_at DESC");return {r["strategy_name"]:r["payload"] for r in cur.fetchall()}
        rows=c.execute("SELECT strategy_name,payload FROM saved_strategies").fetchall();return {r["strategy_name"]:json.loads(r["payload"]) for r in rows}

def strategy_save(name,payload):
    with DB_LOCK,db_conn() as c:
        if USE_POSTGRES:
            with c.cursor() as cur:cur.execute("INSERT INTO public.saved_strategies(strategy_name,payload) VALUES(%s,%s::jsonb) ON CONFLICT(strategy_name) DO UPDATE SET payload=EXCLUDED.payload,updated_at=now()",(name,json.dumps(payload,ensure_ascii=False)))
        else:c.execute("INSERT INTO saved_strategies(strategy_name,payload,updated_at) VALUES(?,?,?) ON CONFLICT(strategy_name) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",(name,json.dumps(payload,ensure_ascii=False),datetime.now().isoformat()))

def strategy_delete(name):
    with DB_LOCK,db_conn() as c:
        if USE_POSTGRES:
            with c.cursor() as cur:cur.execute("DELETE FROM public.saved_strategies WHERE strategy_name=%s",(name,))
        else:c.execute("DELETE FROM saved_strategies WHERE strategy_name=?",(name,))

try:
    db_init()
except Exception as e:
    # Keep the web service alive; do not print credentials/DSN details.
    print("[DB INIT WARNING] database initialization failed")

def twse():
    k="twse"; now=time.time()
    if k in CACHE and now-CACHE[k][0]<TTL:return CACHE[k][1]
    d=fetch_json("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL")
    out=[]
    for x in d:
        c=str(x.get("Code","")).strip()
        if not re.fullmatch(r"\d{4}",c):continue
        cl=num(x.get("ClosingPrice")); ch=num(x.get("Change")); prev=cl-ch if cl is not None and ch is not None else None
        out.append({"market":"上市","code":c,"name":x.get("Name",""),"open":num(x.get("OpeningPrice")),"high":num(x.get("HighestPrice")),
                    "low":num(x.get("LowestPrice")),"close":cl,"volume":num(x.get("TradeVolume")),
                    "change_pct":ch/prev*100 if prev else None})
    CACHE[k]=(now,out); return out

def tpex():
    k="tpex"; now=time.time()
    if k in CACHE and now-CACHE[k][0]<TTL:return CACHE[k][1]
    d=fetch_json("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes")
    out=[]
    for x in d:
        c=str(x.get("SecuritiesCompanyCode") or x.get("SecuritiesCode") or x.get("Code") or "").strip()
        if not re.fullmatch(r"\d{4}",c):continue
        cl=num(x.get("Close") or x.get("ClosingPrice") or x.get("ClosePrice"))
        if cl is None:continue
        out.append({"market":"上櫃","code":c,"name":x.get("CompanyName") or x.get("SecuritiesCompanyName") or x.get("Name") or "",
                    "open":num(x.get("Open") or x.get("OpeningPrice")),"high":num(x.get("High") or x.get("HighestPrice")),
                    "low":num(x.get("Low") or x.get("LowestPrice")),"close":cl,
                    "volume":num(x.get("TradingShares") or x.get("TradeVolume") or x.get("TradingVolume")),
                    "change_pct":num(x.get("ChangePercent") or x.get("ChangePercentage"))})
    CACHE[k]=(now,out); return out


INDUSTRY_NAMES={"01":"水泥工業","02":"食品工業","03":"塑膠工業","04":"紡織纖維","05":"電機機械","06":"電器電纜","08":"玻璃陶瓷","09":"造紙工業","10":"鋼鐵工業","11":"橡膠工業","12":"汽車工業","14":"建材營造","15":"航運業","16":"觀光餐旅","17":"金融保險","18":"貿易百貨","20":"其他業","21":"化學工業","22":"生技醫療業","23":"油電燃氣業","24":"半導體業","25":"電腦及週邊設備業","26":"光電業","27":"通信網路業","28":"電子零組件業","29":"電子通路業","30":"資訊服務業","31":"其他電子業","32":"文化創意業","33":"農業科技業","34":"電子商務業","35":"綠能環保","36":"數位雲端","37":"運動休閒","38":"居家生活"}

def industries():
    """Official exchange company profile classifications. Returns code -> industry."""
    k="industries"; now=time.time()
    if k in CACHE and now-CACHE[k][0]<86400:return CACHE[k][1]
    mp={}
    # TWSE company profile OpenAPI
    for url,market in [
        ("https://openapi.twse.com.tw/v1/opendata/t187ap03_L","上市"),
        ("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O","上櫃")
    ]:
        try:
            d=fetch_json(url)
            for x in d:
                code=str(x.get("公司代號") or x.get("SecuritiesCompanyCode") or x.get("公司代碼") or x.get("Code") or "").strip()
                ind=str(x.get("產業別") or x.get("產業類別") or x.get("Industry") or x.get("產業別名稱") or "").strip()
                ind=INDUSTRY_NAMES.get(ind,ind)
                name=str(x.get("公司簡稱") or x.get("公司名稱") or x.get("CompanyName") or "").strip()
                if re.fullmatch(r"\d{4}",code) and ind:
                    mp[code]={"industry":ind,"market":market,"name":name}
        except Exception:
            pass
    # fallback labels from TWSE security master if industry field is exposed
    if not mp:
        try:
            d=fetch_json("https://openapi.twse.com.tw/v1/opendata/t187ap03_L")
            for x in d:
                code=str(x.get("公司代號","")).strip(); ind=str(x.get("產業別","")).strip(); ind=INDUSTRY_NAMES.get(ind,ind)
                if code and ind:mp[code]={"industry":ind,"market":"上市","name":str(x.get("公司簡稱",""))}
        except: pass
    CACHE[k]=(now,mp);return mp


def finmind(dataset, data_id=None, start_date=None, end_date=None, cache_hours=12):
    token=os.environ.get("FINMIND_TOKEN","").strip()
    key=("fm",dataset,data_id,start_date,end_date); now=time.time()
    # Cache is always allowed, even while FinMind is cooling down.
    if key in CACHE and now-CACHE[key][0] < cache_hours*3600:return CACHE[key][1]
    guard=finmind_guard_status()
    if guard["cooling"]:
        raise FinMindCooldown(f'FinMind 冷卻中，約 {guard["retry_after_seconds"]//60+1} 分鐘後再試')
    p={"dataset":dataset}
    if data_id:p["data_id"]=data_id
    if start_date:p["start_date"]=start_date
    if end_date:p["end_date"]=end_date
    if token:p["token"]=token
    url="https://api.finmindtrade.com/api/v4/data?"+urllib.parse.urlencode(p)
    try:
        o=fetch_json(url)
    except urllib.error.HTTPError as e:
        try: body=e.read().decode("utf-8","ignore")
        except: body=str(e)
        if looks_rate_limited(e.code,body):
            finmind_trip(f"HTTP {e.code}：FinMind 額度/頻率限制")
            raise FinMindCooldown("FinMind 額度或頻率已達限制，已啟動 60 分鐘保護")
        raise
    except Exception as e:
        if looks_rate_limited(0,str(e)):
            finmind_trip("FinMind 額度/頻率限制："+str(e)[:100])
            raise FinMindCooldown("FinMind 額度或頻率已達限制，已啟動 60 分鐘保護")
        raise
    # Some APIs return rate-limit information inside a normal JSON response.
    if isinstance(o,dict):
        status=int(o.get("status",200) or 200) if str(o.get("status",200)).isdigit() else 200
        msg=str(o.get("msg") or o.get("message") or o.get("error") or "")
        if looks_rate_limited(status,msg):
            finmind_trip((msg or f"FinMind status {status}")[:160])
            raise FinMindCooldown("FinMind 額度或頻率已達限制，已啟動 60 分鐘保護")
    if not isinstance(o,dict) or "data" not in o:raise RuntimeError(str(o)[:200])
    FINMIND_GUARD["last_ok"]=time.time();FINMIND_GUARD["last_error"]="";FINMIND_GUARD["reason"]=""
    CACHE[key]=(now,o["data"]);return o["data"]

def token_status():
    token=os.environ.get("FINMIND_TOKEN","").strip()
    if not token:return {"configured":False,"valid":False,"message":"尚未設定 FINMIND_TOKEN",**finmind_guard_status()}
    guard=finmind_guard_status()
    if guard["cooling"]:
        return {"configured":True,"valid":False,"message":"FinMind 保護冷卻中",**guard}
    try:
        d=finmind("TaiwanStockPrice","2330",(date.today()-timedelta(days=10)).isoformat(),cache_hours=1)
        return {"configured":True,"valid":bool(d),"message":"FinMind Token 已設定，API 可正常讀取" if d else "Token 已設定，但測試資料為空",**finmind_guard_status()}
    except Exception as e:
        return {"configured":True,"valid":False,"message":"Token 已設定，但 API 測試失敗："+str(e)[:120],**finmind_guard_status()}


_COLS_CACHE={}
def _pg_table_columns(table):
    if not USE_POSTGRES:return set()
    hit=_COLS_CACHE.get(table);now=time.time()
    if hit and now-hit[0]<(3600 if hit[1] else 60):return set(hit[1])   # 有欄位的表快取 1 小時；查不到的表只快取 1 分鐘（剛建表也能很快被看到）
    try:
        with db_conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s",(table,))
                cols={r[0] for r in cur.fetchall()}
        _COLS_CACHE[table]=(now,frozenset(cols))
        return cols
    except Exception:return set()

def revenue_schema():
    """Resolve the existing warehouse schema; do not assume CREATE IF NOT EXISTS migrated it."""
    cols=_pg_table_columns('monthly_revenue')
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='monthly_revenue'")
            types=dict(cur.fetchall())
    aliases={'code':('stock_id','stock_code','code'),'date':('date','revenue_date','period_date','month','year_month','revenue_month'),
             'revenue':('revenue','monthly_revenue'),'year':('revenue_year',),
             'month':('revenue_month',),'source':('source_name','source'),'updated':('updated_at',)}
    schema={key:next((name for name in names if name in cols),None) for key,names in aliases.items()}
    if schema.get('date')=='revenue_month' and types.get('revenue_month') in ('integer','smallint','bigint','numeric'):
        schema['date']=None
    if schema.get('month')==schema.get('date'):schema['month']=None
    schema['date_type']=types.get(schema.get('date'),'date')
    has_period=bool(schema.get('date') or (schema.get('year') and schema.get('month')))
    if not schema.get('code') or not schema.get('revenue') or not has_period:
        raise ValueError('月營收資料表缺少股票代號、營收值或期間（日期／年度＋月份）；現有欄位：'+','.join(sorted(cols)))
    return schema

def revenue_date_sql(schema):
    if not schema.get('date'):
        year='"'+schema['year']+'"::integer';month='"'+schema['month']+'"::integer'
        return '(CASE WHEN '+year+' BETWEEN 1900 AND 9999 AND '+month+' BETWEEN 1 AND 12 THEN make_date('+year+','+month+',1) ELSE NULL END)'
    column='"'+schema['date']+'"'
    return "(CASE WHEN "+column+"::text ~ '^\\d{4}-\\d{2}$' THEN "+column+"::text||'-01' ELSE "+column+"::text END)::date"

def core_data_status():
    """Inventory only: never calls FinMind. Makes missing datasets visible instead of silently treating them as zero."""
    wanted=['daily_prices','market_index_daily','monthly_revenue','market_chips','institutional_trading','research_institutional_daily','company_research_profiles','health_financial_raw','official_financial_reports','margin_short','financials','industries','industry_chains','industry_chain_nodes','company_industry_links','intraday_bars']
    out=[]
    if not USE_POSTGRES:return {'ok':False,'datasets':out}
    with db_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for t in wanted:
                try:
                    cur.execute("SELECT to_regclass(%s) AS x",('public.'+t,)); exists=bool(cur.fetchone()['x'])
                    if not exists: out.append({'dataset':t,'exists':False,'rows':0,'latest':None}); continue
                    cur.execute('SELECT COUNT(*) n FROM public."'+t+'"'); n=cur.fetchone()['n']
                    cols=_pg_table_columns(t); latest=None
                    if t=='monthly_revenue':
                        cur.execute('SELECT MAX('+revenue_date_sql(revenue_schema())+') v FROM public.monthly_revenue')
                        v=cur.fetchone()['v'];latest=str(v) if v is not None else None
                    for dc in (() if t=='monthly_revenue' else ('date','trade_date','revenue_date','period_date','report_date','updated_at','last_sync_at')):
                        if dc in cols:
                            try:
                                cur.execute('SELECT MAX("'+dc+'") v FROM public."'+t+'"'); v=cur.fetchone()['v'];latest=str(v) if v is not None else None
                            except Exception: c.rollback()
                            break
                    out.append({'dataset':t,'exists':True,'rows':int(n),'latest':latest})
                except Exception:
                    c.rollback();out.append({'dataset':t,'exists':True,'rows':None,'latest':None})
    return {'ok':True,'datasets':out}

def db_revenue_yoy_series(code, months=24):
    if not USE_POSTGRES:return []
    return db_all_revenue_yoy(str(code)).get(str(code),[])[-months:]


_CHIP_SPECS=[
  ('research_institutional_daily', {'foreign_net':('foreign_net',),'trust_net':('trust_net',),'dealer_net':('dealer_net',)}),
  ('institutional_trading', {'foreign_net':('foreign_net','foreign_buy_sell'),'trust_net':('trust_net','investment_trust_net'),'dealer_net':('dealer_net','dealer_buy_sell')}),
  ('margin_short', {'margin_balance':('margin_balance','margin_purchase_balance'),'short_balance':('short_balance','short_sale_balance'),'margin_change':('margin_change',),'short_change':('short_change',)}),
  ('financials', {'eps':('eps',),'gross_margin':('gross_margin',),'operating_margin':('operating_margin',),'capital_100m':('capital_100m',),'pe':('pe',),'dividend_yield':('dividend_yield',),'book_value_per_share':('book_value_per_share',),'pb':('pb',),'pretax_margin':('pretax_margin',),'net_margin_pct':('net_margin_pct','net_margin')})]

def db_chip_fundamental_all():
    """與 db_chip_fundamental 同一套規則，但每張表只查一次（每檔取最新一列），全市場選股用。
    原本逐檔呼叫要開上萬次資料庫連線；這裡合計只需約 10 次查詢，結果相同。"""
    result={}
    if not USE_POSTGRES:return result
    for table,mapping in _CHIP_SPECS:
        cols=_pg_table_columns(table)
        if not cols:continue
        cc=next((x for x in ('stock_id','code') if x in cols),None);dc=next((x for x in ('date','trade_date','period_date','updated_at') if x in cols),None)
        if not cc:continue
        selected=[];aliases=[]
        for outk,cands in mapping.items():
            col=next((x for x in cands if x in cols),None)
            if col:selected.append('"'+col+'" AS "'+outk+'"');aliases.append(outk)
        if not selected:continue
        try:
            q='SELECT DISTINCT ON ("'+cc+'") "'+cc+'" AS "__sid"'+(','+','.join(selected))+((',"'+dc+'" AS data_date') if dc else '')+' FROM public."'+table+'"'+((' ORDER BY "'+cc+'","'+dc+'" DESC') if dc else '')
            with db_conn() as c:
                with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:cur.execute(q);rows=cur.fetchall()
        except Exception:continue
        for r in rows:
            out=result.setdefault(str(r.get('__sid')),{})
            if r.get('data_date') is not None:out[table+'_date']=str(r['data_date'])[:10]
            for k in aliases:
                if r.get(k) is not None and k not in out:out[k]=num(r.get(k))
    for code,profile in db_company_profiles_all().items():
        if isinstance(profile,dict) and profile.get('capital_100m') is not None:result.setdefault(code,{})['capital_100m']=profile['capital_100m']
    return result

def db_chip_fundamental(code):
    """Best-effort read of warehouse chip/fundamental tables only. Never contacts FinMind."""
    out={}
    if not USE_POSTGRES:return out
    # Keep this deliberately conservative: unknown schemas stay missing, never fabricated as 0.
    specs=_CHIP_SPECS
    for table,mapping in specs:
        cols=_pg_table_columns(table)
        if not cols:continue
        cc=next((x for x in ('stock_id','code') if x in cols),None);dc=next((x for x in ('date','trade_date','period_date','updated_at') if x in cols),None)
        if not cc:continue
        selected=[]; aliases=[]
        for outk,cands in mapping.items():
            col=next((x for x in cands if x in cols),None)
            if col:selected.append('"'+col+'" AS "'+outk+'"');aliases.append(outk)
        if not selected:continue
        try:
            q='SELECT '+','.join(selected)+((',"'+dc+'" AS data_date') if dc else '')+' FROM public."'+table+'" WHERE "'+cc+'"=%s'+((' ORDER BY "'+dc+'" DESC') if dc else '')+' LIMIT 1'
            with db_conn() as c:
                with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:cur.execute(q,(str(code),));r=cur.fetchone()
            if r:
                if r.get('data_date') is not None:out[table+'_date']=str(r['data_date'])[:10]
                for k in aliases:
                    if r.get(k) is not None and k not in out:out[k]=num(r.get(k))
        except Exception:pass
    profile=db_company_research(code)
    if profile.get('capital_100m') is not None:out['capital_100m']=profile['capital_100m']
    return out

def db_holder_summary(codes):
    """千張大戶（集保戶股權分散表，每週）最新占比、週變化、累積變化；沒有資料就回空 dict。"""
    codes=[str(c) for c in codes if c]
    if not USE_POSTGRES or not codes:return {}
    try:
        with db_conn() as c:
            with c.cursor() as cur:
                cur.execute("SELECT stock_id,data_date,pct_1000,pct_400,holders_1000,holders_total FROM public.holder_weekly WHERE stock_id=ANY(%s) ORDER BY stock_id,data_date",(codes,))
                rows=cur.fetchall()
    except Exception:
        return {}
    by={}
    for sid,d,p1,p4,h1,ht in rows:
        by.setdefault(str(sid),[]).append((str(d),num(p1),num(p4),num(h1),num(ht)))
    out={}
    for sid,v in by.items():
        v=v[-14:];last=v[-1];prev=v[-2] if len(v)>1 else None;first=v[0]
        out[sid]={"date":last[0],"pct_1000":last[1],"pct_400":last[2],"holders_1000":last[3],"weeks":len(v),
                  "chg_1w":None if not prev or last[1] is None or prev[1] is None else last[1]-prev[1],
                  "chg_total":None if len(v)<2 or last[1] is None or first[1] is None else last[1]-first[1],
                  "holders_chg":None if not prev or last[3] is None or prev[3] is None else last[3]-prev[3]}
    return out

def db_margin_ratio_history(days=12):
    """券資比(券餘額÷融資餘額×100) of the last N warehouse trading days, oldest -> newest, per stock."""
    if not USE_POSTGRES:return {}
    with db_conn() as c:
        with c.cursor() as cur:
            cur.execute("WITH d AS (SELECT DISTINCT trade_date FROM public.margin_short ORDER BY trade_date DESC LIMIT %s) SELECT stock_id,margin_balance,short_balance FROM public.margin_short WHERE trade_date IN (SELECT trade_date FROM d) ORDER BY stock_id,trade_date",(days,))
            rows=cur.fetchall()
    out={}
    for code,mb,sb in rows:
        mb=num(mb);sb=num(sb)
        out.setdefault(str(code),[]).append((sb/mb*100) if mb and sb is not None else None)
    return out

def db_all_revenue_yoy(code=None,kind='yoy'):
    """Read all monthly revenue with one connection for a full-market scan."""
    if not USE_POSTGRES:return {}
    schema=revenue_schema()
    dc=revenue_date_sql(schema)
    yc='"'+schema['year']+'"' if schema.get('year') else 'EXTRACT(YEAR FROM '+dc+')'
    mc='"'+schema['month']+'"' if schema.get('month') else 'EXTRACT(MONTH FROM '+dc+')'
    with db_conn() as c:
        with c.cursor() as cur:
            cur.execute('SELECT "'+schema['code']+'",'+yc+','+mc+',"'+schema['revenue']+'" FROM public.monthly_revenue'+(' WHERE "'+schema['code']+'"=%s' if code else '')+' ORDER BY 1,2,3',(code,) if code else ())
            rows=cur.fetchall()
    grouped={}
    for code,year,month,revenue in rows:
        if year and month and revenue is not None:grouped.setdefault(str(code),{})[(int(year),int(month))]=float(revenue)
    result={}
    for code,months in grouped.items():
        latest=max(months);y,m=latest;series=[]
        for _ in range(24):
            this=months.get((y,m))
            prior=months.get((y-1,m)) if kind=='yoy' else months.get((y-1,12) if m==1 else (y,m-1))  # mom: previous month
            series.append((this/prior-1)*100 if this is not None and prior not in (None,0) else None)
            m-=1
            if m==0:y-=1;m=12
        result[code]=list(reversed(series))
    return result

def chip_fundamental(code):
    k=("cf",code);now=time.time()
    if k in CACHE and now-CACHE[k][0]<12*3600:return CACHE[k][1]
    start=(date.today()-timedelta(days=120)).isoformat()
    out={}
    # institutional: latest disclosed day
    try:
        d=finmind("TaiwanStockInstitutionalInvestorsBuySell",code,start,cache_hours=12)
        if d:
            last=max(str(x.get("date","")) for x in d)
            z=[x for x in d if str(x.get("date",""))==last]
            nets={}
            for x in z:nets[x.get("name","")]=float(x.get("buy",0) or 0)-float(x.get("sell",0) or 0)
            out["foreign_net"]=nets.get("Foreign_Investor",0)
            out["trust_net"]=nets.get("Investment_Trust",0)
            out["dealer_net"]=nets.get("Dealer",0)+nets.get("Dealer_self",0)+nets.get("Dealer_Hedging",0)
            out["institution_date"]=last
    except:pass
    # margin/short: robustly accept FinMind's category rows
    try:
        d=finmind("TaiwanStockMarginPurchaseShortSale",code,start,cache_hours=12)
        if d:
            last=max(str(x.get("date","")) for x in d); z=[x for x in d if str(x.get("date",""))==last]
            for x in z:
                nm=str(x.get("name",""))
                bal=num(x.get("TodayBalance")); yes=num(x.get("YesBalance"))
                if nm=="MarginPurchase":
                    out["margin_balance"]=bal;out["margin_change"]=(bal-yes) if bal is not None and yes is not None else None
                elif nm=="ShortSale":
                    out["short_balance"]=bal;out["short_change"]=(bal-yes) if bal is not None and yes is not None else None
            out["margin_date"]=last
    except:pass
    # monthly revenue: latest YoY growth, calculate if API does not provide it
    try:
        d=finmind("TaiwanStockMonthRevenue",code,(date.today()-timedelta(days=500)).isoformat(),cache_hours=24)
        if d:
            d=sorted(d,key=lambda x:str(x.get("date","")))
            x=d[-1]; yoy=num(x.get("revenue_year_growth_rate") or x.get("revenue_year_growth"))
            if yoy is None and len(d)>=13:
                cur=num(x.get("revenue")); prev=num(d[-13].get("revenue"))
                yoy=(cur/prev-1)*100 if cur is not None and prev else None
            out["revenue_yoy"]=yoy;out["revenue_date"]=x.get("date")
    except:pass
    # financial statements: latest EPS and common profitability labels if present
    try:
        d=finmind("TaiwanStockFinancialStatements",code,(date.today()-timedelta(days=800)).isoformat(),cache_hours=24)
        if d:
            dates=sorted(set(str(x.get("date","")) for x in d if x.get("date")))
            if dates:
                last=dates[-1]; z=[x for x in d if str(x.get("date",""))==last]
                def pick(keys):
                    for x in z:
                        typ=str(x.get("type",""));name=str(x.get("name",""))
                        if any(k.lower() in (typ+" "+name).lower() for k in keys):
                            vv=num(x.get("value"))
                            if vv is not None:return vv
                    return None
                out["eps"]=pick(["EPS","EarningsPerShare","每股盈餘"])
                out["gross_profit"]=pick(["GrossProfit","營業毛利"])
                out["operating_income"]=pick(["OperatingIncome","營業利益"])
                out["net_income"]=pick(["IncomeAfterTaxes","NetIncome","本期淨利","稅後淨利"])
                out["financial_date"]=last
    except:pass

    # Foreign shareholding
    try:
        d=finmind("TaiwanStockShareholding",code,start,cache_hours=12)
        if d:
            x=sorted(d,key=lambda z:str(z.get("date","")))[-1]
            # schema names have changed historically; accept common variants
            hold=num(x.get("ForeignInvestmentShares") or x.get("shares") or x.get("ForeignInvestmentRemainingShares"))
            pct=num(x.get("ForeignInvestmentSharesRatio") or x.get("ratio") or x.get("ForeignInvestmentSharesPercentage"))
            out["foreign_holding"]=hold;out["foreign_holding_pct"]=pct
    except:pass
    # Holding distribution: aggregate large holders >=400 lots when share percentage is available
    try:
        d=finmind("TaiwanStockHoldingSharesPer",code,start,cache_hours=24)
        if d:
            last=max(str(x.get("date","")) for x in d); z=[x for x in d if str(x.get("date",""))==last]
            vals=[]
            for x in z:
                lvl=str(x.get("HoldingSharesLevel","") or x.get("level",""))
                p=num(x.get("percent") or x.get("HoldingSharesPercent"))
                if p is not None and any(t in lvl for t in ["400-600","600-800","800-1000","1000以上","1,000以上","1000001"]): vals.append(p)
            if vals:out["large_holder_pct"]=sum(vals)
    except:pass
    # Day trading
    try:
        d=finmind("TaiwanStockDayTrading",code,start,cache_hours=12)
        if d:
            x=sorted(d,key=lambda z:str(z.get("date","")))[-1]
            dv=num(x.get("BuyAfterSale") or x.get("day_trading_volume") or x.get("Volume"))
            out["daytrade_volume"]=dv
    except:pass
    # Balance sheet for equity/book value/capital
    try:
        d=finmind("TaiwanStockBalanceSheet",code,(date.today()-timedelta(days=800)).isoformat(),cache_hours=24)
        if d:
            last=max(str(x.get("date","")) for x in d); z=[x for x in d if str(x.get("date",""))==last]
            def bp(keys):
                for x in z:
                    t=(str(x.get("type",""))+" "+str(x.get("name",""))).lower()
                    if any(k.lower() in t for k in keys):
                        v=num(x.get("value"))
                        if v is not None:return v
            equity=bp(["Equity","權益總額","TotalEquity"])
            capital=bp(["CapitalStock","股本","CommonStock"])
            shares=(capital/10) if capital else None
            if capital is not None:out["capital_100m"]=capital/100000000
            if equity is not None and shares:out["book_value_per_share"]=equity/shares
    except:pass
    # Market value / valuation fields when available
    try:
        d=finmind("TaiwanStockPER",code,start,cache_hours=12)
        if d:
            x=sorted(d,key=lambda z:str(z.get("date","")))[-1]
            out["pe"]=num(x.get("PER") or x.get("pe"))
            out["pb"]=num(x.get("PBR") or x.get("pb"))
            out["dividend_yield"]=num(x.get("dividend_yield") or x.get("DividendYield"))
    except:pass
    # Profitability ratios derived from latest income statement
    try:
        d=finmind("TaiwanStockFinancialStatements",code,(date.today()-timedelta(days=800)).isoformat(),cache_hours=24)
        if d:
            last=max(str(x.get("date","")) for x in d); z=[x for x in d if str(x.get("date",""))==last]
            def fp(keys):
                for x in z:
                    t=(str(x.get("type",""))+" "+str(x.get("name",""))).lower()
                    if any(k.lower() in t for k in keys):
                        v=num(x.get("value"))
                        if v is not None:return v
            rev=fp(["Revenue","營業收入"]); gp=fp(["GrossProfit","營業毛利"]); op=fp(["OperatingIncome","營業利益"])
            pre=fp(["IncomeBeforeTax","稅前淨利"]); net=fp(["IncomeAfterTaxes","NetIncome","本期淨利","稅後淨利"])
            if rev:
                if gp is not None:out["gross_margin"]=gp/rev*100
                if op is not None:out["operating_margin"]=op/rev*100
                if pre is not None:out["pretax_margin"]=pre/rev*100
                if net is not None:out["net_margin_pct"]=net/rev*100
    except:pass
    # Derived chip ratios from current daily row are completed later in scan.
    CACHE[k]=(now,out);return out

GLOBAL_SYMBOLS=[
 ("美國","道瓊","^DJI"),("美國","S&P 500","^GSPC"),("美國","Nasdaq","^IXIC"),("美國","費城半導體","^SOX"),("風險","VIX","^VIX"),
 ("亞洲","日經225","^N225"),("亞洲","韓國KOSPI","^KS11"),("亞洲","上證綜合","000001.SS"),
 ("資金環境","美元指數","DX-Y.NYB"),("資金環境","美國10年債殖利率","^TNX"),("商品","黃金","GC=F"),("商品","原油","CL=F")]
def global_markets():
    k="global";now=time.time()
    if k in CACHE and now-CACHE[k][0]<300:return CACHE[k][1]
    rows=[]
    for group,name,sym in GLOBAL_SYMBOLS:
        try:
            u="https://query1.finance.yahoo.com/v8/finance/chart/"+urllib.parse.quote(sym,safe="")+"?range=5d&interval=1d"
            r=fetch_json(u).get("chart",{}).get("result",[{}])[0]
            meta=r.get("meta",{});q=r.get("indicators",{}).get("quote",[{}])[0]
            closes=[num(x) for x in q.get("close",[])];closes=[x for x in closes if x is not None]
            cur=num(meta.get("regularMarketPrice")) or (closes[-1] if closes else None)
            prev=num(meta.get("chartPreviousClose") or meta.get("previousClose"))
            if prev is None and len(closes)>1:prev=closes[-2]
            ch=(cur/prev-1)*100 if cur is not None and prev else None
            rows.append({"group":group,"name":name,"symbol":sym,"price":cur,"change_pct":ch,
                         "market_time":meta.get("regularMarketTime"),"currency":meta.get("currency","")})
        except Exception as e:rows.append({"group":group,"name":name,"symbol":sym,"error":str(e)[:80]})
    o={"ok":True,"asof":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"rows":rows}
    CACHE[k]=(now,o);return o

def taiwan_market_close():
    """Official TWSE completed-day summary, with explicit trading date."""
    key='twse_market_close';now=time.time()
    if key in CACHE and now-CACHE[key][0]<900:return CACHE[key][1]
    data=fetch_json('https://openapi.twse.com.tw/v1/exchangeReport/FMTQIK')
    if not isinstance(data,list) or not data:return {'ok':False,'error':'證交所日報資料暫無資料'}
    rows=[x for x in data if re.fullmatch(r'\d{7}',str(x.get('Date','')))]
    if not rows:return {'ok':False,'error':'證交所日報資料格式未辨識'}
    x=max(rows,key=lambda a:str(a['Date']))
    d=str(x['Date']);trade_date=f'{int(d[:3])+1911:04d}-{d[3:5]}-{d[5:7]}'
    price=num(x.get('TAIEX'));change=num(x.get('Change'))
    o={'ok':True,'status':'盤後日報','trade_date':trade_date,'source':'臺灣證券交易所 FMTQIK',
       'taiex':price,'change':change,'change_pct':(change/(price-change)*100) if price is not None and change is not None and price!=change else None,
       'turnover':num(x.get('TradeValue')),'tpex':None,'up':None,'down':None,
       'retrieved_at':datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='minutes')}
    CACHE[key]=(now,o);return o

def capital_flow():
    base=twse()+tpex(); im=industries(); groups={};total=0.0;used=0
    for r in base:
        try:px=float(r.get("close") or 0);vol=float(r.get("volume") or 0);ch=float(r.get("change_pct") or 0)
        except:continue
        if px<=0 or vol<=0:continue
        amt=px*vol;total+=amt;used+=1
        ind=im.get(r["code"],{}).get("industry") or "其他/未分類"
        g=groups.setdefault(ind,{"industry":ind,"turnover":0.0,"stocks":0,"up":0,"down":0,"num":0.0,"leaders":[]})
        g["turnover"]+=amt;g["stocks"]+=1;g["up"]+=1 if ch>0 else 0;g["down"]+=1 if ch<0 else 0;g["num"]+=ch*amt
        g["leaders"].append({"code":r["code"],"name":r.get("name",""),"change_pct":ch,"turnover":amt})
    rows=[]
    for g in groups.values():
        g["share_pct"]=g["turnover"]/total*100 if total else 0;g["avg_change_pct"]=g["num"]/g["turnover"] if g["turnover"] else 0
        g["breadth_pct"]=(g["up"]-g["down"])/g["stocks"]*100 if g["stocks"] else 0
        g["flow_score"]=g["share_pct"]+max(-10,min(10,g["breadth_pct"]/10))+max(-10,min(10,g["avg_change_pct"]*2))
        g["leaders"]=sorted(g["leaders"],key=lambda x:x["turnover"],reverse=True)[:5];del g["num"];rows.append(g)
    rows.sort(key=lambda x:x["flow_score"],reverse=True)
    return {"ok":True,"asof":datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'),
            "data_status":"來源未附可驗證分鐘時點；盤中／盤後狀態待確認","market_stocks":used,"rows":rows[:15],
            "definition":"以來源最近一筆日報價乘成交量估算；無可驗證分鐘快照時不繪製 09:10／09:30／10:00 趨勢。"}

def history(code, days=300):
    k=("h",code);now=time.time()
    if k in CACHE and now-CACHE[k][0]<3600:return CACHE[k][1]
    start=(date.today()-timedelta(days=500)).isoformat()
    params={"dataset":"TaiwanStockPrice","data_id":code,"start_date":start}
    token=os.environ.get("FINMIND_TOKEN","").strip()
    if token: params["token"]=token
    q=urllib.parse.urlencode(params)
    o=fetch_json("https://api.finmindtrade.com/api/v4/data?"+q)
    rows=[]
    for x in o.get("data",[]):
        rows.append({"date":x.get("date"),"open":num(x.get("open")),"high":num(x.get("max")),"low":num(x.get("min")),
                     "close":num(x.get("close")),"volume":num(x.get("Trading_Volume"))})
    rows=[r for r in rows if r["close"] is not None][-days:]
    if not rows: raise RuntimeError("無歷史日線")
    CACHE[k]=(now,rows);return rows

def resample(rows, tf):
    if tf=="day": return rows
    # weekly/monthly from official daily history
    groups={}
    for r in rows:
        y,m,d=map(int,r["date"].split("-"))
        dt=date(y,m,d)
        key=(dt.isocalendar().year,dt.isocalendar().week) if tf=="week" else (y,m)
        groups.setdefault(key,[]).append(r)
    out=[]
    for _,g in sorted(groups.items()):
        out.append({"date":g[-1]["date"],"open":g[0]["open"],"high":max(x["high"] for x in g if x["high"] is not None),
                    "low":min(x["low"] for x in g if x["low"] is not None),"close":g[-1]["close"],
                    "volume":sum((x["volume"] or 0) for x in g)})
    return out

def ema(a,p):
    if not a:return []
    k=2/(p+1);o=[a[0]]
    for v in a[1:]:o.append(v*k+o[-1]*(1-k))
    return o

def indicator_series(rows, field):
    c=[r["close"] for r in rows]; h=[r["high"] for r in rows]; l=[r["low"] for r in rows]; v=[r["volume"] or 0 for r in rows]
    if field=="volume":return [num(r.get('volume'))/1000 if num(r.get('volume')) is not None else None for r in rows]
    if field in ("open","high","low","close"): return [r[field] for r in rows]
    if field=="avg_price": return c  # N 日均價採 N 根收盤價的算術平均
    if field=="change_pct":
        return [None]+[((c[i]/c[i-1]-1)*100) if c[i-1] else None for i in range(1,len(c))]
    if field=="obv":
        out=[0]
        for i in range(1,len(c)):out.append(out[-1]+(v[i] if c[i]>c[i-1] else (-v[i] if c[i]<c[i-1] else 0)))
        return out
    if field=="rsi14":
        out=[None]*len(c)
        for i in range(14,len(c)):
            ds=[c[j]-c[j-1] for j in range(i-13,i+1)]
            ag=sum(max(x,0) for x in ds)/14; al=sum(max(-x,0) for x in ds)/14
            out[i]=100 if al==0 else 100-100/(1+ag/al)
        return out
    if field in ("dif","macd"):
        e12=ema(c,12);e26=ema(c,26);dif=[e12[i]-e26[i] for i in range(len(c))];sig=ema(dif,9)
        return dif if field=="dif" else [(dif[i]-sig[i])*2 for i in range(len(c))]
    if field in ("k","d"):
        ks=[];ds=[];K=D=50.
        for i in range(len(c)):
            if i<8:ks.append(None);ds.append(None);continue
            hi=max(h[i-8:i+1]);lo=min(l[i-8:i+1]);rsv=50 if hi==lo else (c[i]-lo)/(hi-lo)*100
            K=K*2/3+rsv/3;D=D*2/3+K/3;ks.append(K);ds.append(D)
        return ks if field=="k" else ds
    def sma(arr,p):
        o=[None]*len(arr)
        for i in range(p-1,len(arr)):
            z=[x for x in arr[i-p+1:i+1] if x is not None]
            if len(z)==p:o[i]=sum(z)/p
        return o
    if field=="bias20":
        m=sma(c,20); return [None if m[i] in (None,0) else (c[i]-m[i])/m[i]*100 for i in range(len(c))]
    if field=="mtm10":
        return [None if i<10 else c[i]-c[i-10] for i in range(len(c))]
    if field=="mtm_ma10":
        mt=[None if i<10 else c[i]-c[i-10] for i in range(len(c))]; return sma(mt,10)
    if field=="wr14":
        o=[None]*len(c)
        for i in range(13,len(c)):
            hi=max(h[i-13:i+1]);lo=min(l[i-13:i+1]);o[i]=0 if hi==lo else (hi-c[i])/(hi-lo)*-100
        return o
    if field=="psy12":
        o=[None]*len(c)
        for i in range(12,len(c)):o[i]=sum(1 for j in range(i-11,i+1) if c[j]>c[j-1])/12*100
        return o
    if field in ("osc","osc_ma"):
        e12=ema(c,12);e26=ema(c,26);osc=[e12[i]-e26[i] for i in range(len(c))]
        return osc if field=="osc" else ema(osc,9)
    if field in ("boll_up","boll_low"):
        m=sma(c,20);o=[None]*len(c)
        for i in range(19,len(c)):
            sd=(sum((x-m[i])**2 for x in c[i-19:i+1])/20)**0.5
            o[i]=m[i]+2*sd if field=="boll_up" else m[i]-2*sd
        return o
    if field=="cci20":
        tp=[(h[i]+l[i]+c[i])/3 for i in range(len(c))];ma=sma(tp,20);o=[None]*len(c)
        for i in range(19,len(c)):
            md=sum(abs(x-ma[i]) for x in tp[i-19:i+1])/20
            o[i]=0 if md==0 else (tp[i]-ma[i])/(0.015*md)
        return o
    if field=="dm":
        ep=ema(c,12);eq=ema(c,26);dif=[ep[i]-eq[i] for i in range(len(c))];sig=ema(dif,9)
        return [dif[i]-sig[i] for i in range(len(c))]
    if field=="bbi":
        m3,m6,m12,m24=sma(c,3),sma(c,6),sma(c,12),sma(c,24)
        return [None if any(x is None for x in (m3[i],m6[i],m12[i],m24[i])) else (m3[i]+m6[i]+m12[i]+m24[i])/4 for i in range(len(c))]
    if field in ("ar26","br26"):
        o=[None]*len(c)
        for i in range(25,len(c)):
            if field=="ar26":
                up=sum(max(h[j]-rows[j]["open"],0) for j in range(i-25,i+1));dn=sum(max(rows[j]["open"]-l[j],0) for j in range(i-25,i+1))
            else:
                up=sum(max(h[j]-c[j-1],0) for j in range(i-24,i+1));dn=sum(max(c[j-1]-l[j],0) for j in range(i-24,i+1))
            o[i]=None if dn==0 else up/dn*100
        return o
    if field=="vr26":
        o=[None]*len(c)
        for i in range(25,len(c)):
            av=bv=cv=0
            for j in range(i-25,i+1):
                if j==0:continue
                if c[j]>c[j-1]:av+=v[j]
                elif c[j]<c[j-1]:bv+=v[j]
                else:cv+=v[j]
            den=bv+cv/2;o[i]=None if den==0 else (av+cv/2)/den*100
        return o
    if field in ("di_plus","di_minus","adx14"):
        n=len(c);tr=[0]*n;pdm=[0]*n;mdm=[0]*n
        for i in range(1,n):
            tr[i]=max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
            up=h[i]-h[i-1];dn=l[i-1]-l[i];pdm[i]=up if up>dn and up>0 else 0;mdm[i]=dn if dn>up and dn>0 else 0
        plus=[None]*n;minus=[None]*n;dx=[None]*n
        for i in range(14,n):
            atr=sum(tr[i-13:i+1]); plus[i]=None if atr==0 else sum(pdm[i-13:i+1])/atr*100;minus[i]=None if atr==0 else sum(mdm[i-13:i+1])/atr*100
            if plus[i] is not None and minus[i] is not None and plus[i]+minus[i]!=0:dx[i]=abs(plus[i]-minus[i])/(plus[i]+minus[i])*100
        if field=="di_plus":return plus
        if field=="di_minus":return minus
        out=[None]*n
        for i in range(27,n):
            z=[x for x in dx[i-13:i+1] if x is not None]
            if len(z)==14:out[i]=sum(z)/14
        return out
    if field=="sar":
        # standard parabolic SAR approximation, AF .02 to .20
        n=len(c);o=[None]*n
        if n<2:return o
        bull=c[1]>=c[0];sar=l[0] if bull else h[0];ep=h[0] if bull else l[0];af=.02;o[0]=sar
        for i in range(1,n):
            sar=sar+af*(ep-sar)
            if bull:
                sar=min(sar,l[i-1],l[i-2] if i>1 else l[i-1])
                if l[i]<sar:bull=False;sar=ep;ep=l[i];af=.02
                elif h[i]>ep:ep=h[i];af=min(.2,af+.02)
            else:
                sar=max(sar,h[i-1],h[i-2] if i>1 else h[i-1])
                if h[i]>sar:bull=True;sar=ep;ep=h[i];af=.02
                elif l[i]<ep:ep=l[i];af=min(.2,af+.02)
            o[i]=sar
        return o
    return [None]*len(c)


def revenue_yoy_series(code, months=24):
    """Monthly revenue YoY series, oldest -> newest. Uses official FinMind TaiwanStockMonthRevenue."""
    end=date.today()
    start=end-datetime.timedelta(days=max(550, months*35))
    rows=finmind("TaiwanStockMonthRevenue",code,start.isoformat(),end.isoformat(),cache_hours=12) or []
    out=[]
    for r in rows:
        v=num(r.get("revenue_year_growth_rate"))
        if v is None: v=num(r.get("revenue_yoy"))
        if v is not None: out.append(v)
    return out[-months:]

_K30_CACHE={}
def kminute_history(code, minutes=30, bars_needed=100):
    """Build N-minute bars from FinMind TaiwanStockKBar one-minute rows.
    Requires a FinMind plan/token that can access TaiwanStockKBar.
    """
    minutes=max(1,int(minutes))
    key=(code,minutes,bars_needed)
    hit=_K30_CACHE.get(key)
    if hit and time.time()-hit[0] < 43200: return hit[1]
    token=os.getenv("FINMIND_TOKEN","").strip()
    if not token: return []
    bars=[]; d=date.today(); attempts=0
    # Smaller intervals need fewer calendar days; cap protects API quota.
    max_days=24
    while len(bars)<bars_needed and attempts<max_days:
        if d.weekday()<5:
            try: rows=finmind("TaiwanStockKBar",code,d.isoformat(),d.isoformat(),cache_hours=12) or []
            except Exception: rows=[]
            mins=[]
            for r in rows:
                tm=str(r.get("minute") or r.get("time") or "")
                if not tm: continue
                mins.append({"date":str(r.get("date") or d.isoformat())+" "+tm,
                    "open":num(r.get("open")),"high":num(r.get("high")),"low":num(r.get("low")),
                    "close":num(r.get("close")),"volume":num(r.get("volume")) or 0})
            mins=sorted(mins,key=lambda x:x["date"])
            for i in range(0,len(mins),minutes):
                g=mins[i:i+minutes]
                if not g or g[0]["open"] is None: continue
                hs=[x["high"] for x in g if x["high"] is not None]; ls=[x["low"] for x in g if x["low"] is not None]
                if not hs or not ls: continue
                bars.append({"date":g[-1]["date"],"open":g[0]["open"],"high":max(hs),"low":min(ls),
                    "close":next((x["close"] for x in reversed(g) if x["close"] is not None),None),
                    "volume":sum(x["volume"] for x in g)})
        d-=datetime.timedelta(days=1); attempts+=1
    bars=sorted(bars,key=lambda x:x["date"])[-bars_needed:]
    _K30_CACHE[key]=(time.time(),bars)
    return bars

def k30_history(code,bars_needed=100):
    return kminute_history(code,30,bars_needed)

_MEMO=threading.local()  # per-stock cache used only while a scan is running

def rows_for_tf(hist, tf, code=None, bars_needed=100):
    minute_map={"1m":1,"5m":5,"10m":10,"15m":15,"30m":30,"60m":60}
    if tf in minute_map:
        # Screening is warehouse-only. Intraday bars need a separate persisted importer.
        return []
    m=getattr(_MEMO,'d',None)
    if m is None:return resample(hist,tf)
    k=('rs',id(hist),tf);hit=m.get(k)
    if hit is None:
        hit=(hist,resample(hist,tf));m[k]=hit  # keep hist referenced so id() stays unique
    return hit[1]

def aggregate(vals, mode):
    if not vals or any(x is None for x in vals):return None
    if not vals:return None
    if mode=="min":return min(vals)
    if mode=="max":return max(vals)
    if mode=="avg":return sum(vals)/len(vals)
    if mode=="sum":return sum(vals)
    return vals[-1]

def expression(rows, e, extras=None):
    extras=extras or {}
    field=e.get("field","close")
    if "constant" in e:return num(e.get("constant"))
    offset=max(0,int(e.get("offset",0))); window=max(1,int(e.get("window",1))); agg=e.get("agg","latest")
    if field in extras:
        raw=extras.get(field)
        if raw is None:return None
        if isinstance(raw,list):
            end=len(raw)-offset
            if end<=0:return None
            if end<window:return None
            val=aggregate(raw[end-window:end],agg)
        else:
            val=float(raw)
        return None if val is None else float(val)
    memo=getattr(_MEMO,'d',None);mk=('ind',id(rows),field);hit=memo.get(mk) if memo is not None else None
    if hit is None:
        s=indicator_series(rows,field)
        # A zero OHLC price is an invalid/missing bar, not a real new low.
        if field in ('open','high','low','close','avg_price'):
            s=[x if num(x) is not None and num(x)>0 else None for x in s]
        if memo is not None:memo[mk]=(rows,s)
    else:s=hit[1]
    end=len(s)-offset
    if end<window:return None
    val=aggregate(s[end-window:end],agg)
    return None if val is None else float(val)

ARITH={
    "+":lambda x,y:x+y,
    "-":lambda x,y:x-y,
    "*":lambda x,y:x*y,
    "/":lambda x,y:None if y==0 else x/y,
}
OPS={">":lambda x,y:x>y,">=":lambda x,y:x>=y,"<":lambda x,y:x<y,"<=":lambda x,y:x<=y,
     "=":lambda x,y:abs(x-y)<1e-8,"!=":lambda x,y:abs(x-y)>=1e-8}

def side_value(hist, side, extras=None, code=None):
    """A side is term A, optionally arithmetic-op term B. Each term can use its own timeframe."""
    def term(t):
        if "constant" in t:return num(t.get("constant"))
        tf=t.get("tf","day")
        need=max(100,int(t.get("window",1))+int(t.get("offset",0))+5)
        rr=rows_for_tf(hist,tf,code,need)
        return expression(rr,t,extras)
    av=term(side.get("a",side))
    if av is None:return None
    op=side.get("arith","")
    if op:
        bv=term(side.get("b",{}))
        if bv is None:return None
        av=ARITH.get(op,lambda x,y:None)(av,bv)
        if av is None:return None
    tail=side.get('tail') or {}
    if tail.get('op'):
        number=num(tail.get('number'))
        if number is None:return None
        av=ARITH.get(tail['op'],lambda x,y:None)(av,number)
    return av

def check(rows, c, extras=None, code=None):
    # Backward compatibility with V10-V14 strategies.
    if "lhs" in c and "left" not in c:
        left={"a":c["lhs"],"arith":""}
        if c.get("rhs_type")=="expr": right={"a":c.get("rhs",{}),"arith":""}
        else: right={"number":num(c.get("rhs_value"))}
    else:
        left=c.get("left",{})
        right=c.get("right",{})
    av=side_value(rows,left,extras,code)
    if "number" in right: bv=num(right.get("number"))
    else: bv=side_value(rows,right,extras,code)
    op=c.get("op",">=")
    if op not in OPS:raise ValueError("不支援的比較符號")
    return (None,av,bv) if av is None or bv is None else (OPS[op](av,bv),av,bv)



def evaluate_condition_groups(conds, values):
    """Three-valued AND/OR/NOT; missing inputs never become a confirmed match."""
    tokens=[]
    for i,(c,val) in enumerate(zip(conds,values)):
        if i:tokens.append('OR' if c.get('logic')=='any' else 'AND')
        tokens += ['(']*min(10,max(0,int(c.get('group_open',0))))
        if c.get('not'):tokens.append('NOT')
        tokens.append(val)
        tokens += [')']*min(10,max(0,int(c.get('group_close',0))))
    prec={'OR':1,'AND':2,'NOT':3};out=[];ops=[]
    for t in tokens:
        if t is True or t is False or t is None:out.append(t)
        elif t=='(':ops.append(t)
        elif t==')':
            while ops and ops[-1]!='(':out.append(ops.pop())
            if not ops:raise ValueError('括號沒有成對')
            ops.pop()
        else:
            while ops and ops[-1]!='(' and prec[ops[-1]]>=prec[t]:out.append(ops.pop())
            ops.append(t)
    if '(' in ops:raise ValueError('括號沒有成對')
    out.extend(reversed(ops));st=[]
    for t in out:
        if t is True or t is False or t is None:st.append(t)
        elif t=='NOT':
            if not st:raise ValueError('NOT 缺少條件')
            a=st.pop();st.append(None if a is None else not a)
        else:
            if len(st)<2:raise ValueError('AND/OR 缺少條件')
            b=st.pop();a=st.pop()
            st.append((False if a is False or b is False else None if a is None or b is None else True) if t=='AND' else
                      (True if a is True or b is True else None if a is None or b is None else False))
    if len(st)!=1 and conds:raise ValueError('公式不完整')
    return st[0] if st else True

EXTRA_TABLES={
    'revenue_yoy':'monthly_revenue','revenue_mom':'monthly_revenue',
    **{x:'institutional_trading' for x in ('foreign_net','trust_net','dealer_net','foreign_holding','foreign_holding_pct')},
    **{x:'margin_short' for x in ('margin_balance','short_balance','margin_change','short_change','margin_volume_ratio','short_volume_ratio','short_margin_ratio')},
    **{x:'financials' for x in ('eps','gross_margin','operating_margin','capital_100m','pe','dividend_yield','book_value_per_share','pb','pretax_margin','net_margin_pct')},
    **{x:'daytrade' for x in ('daytrade_volume','daytrade_ratio')},
    'large_holder_pct':'large_holders',
    **{x:'unsupported' for x in ('margin_utilization','short_utilization','open_interest_proxy')}
}
DATA_LABELS={'monthly_revenue':'月營收','institutional_trading':'法人籌碼','margin_short':'融資融券','financials':'財報','daytrade':'當沖','large_holders':'大戶持股','intraday_bars':'分鐘 K','unsupported':'官方定義與資料來源'}

def condition_terms(c):
    for side in ('left','right'):
        for term in (c.get(side) or {}).values():
            if isinstance(term,dict) and term.get('field') and 'constant' not in term:yield term
    if c.get('lhs'):yield c['lhs']
    if c.get('rhs_type')=='expr' and c.get('rhs'):yield c['rhs']

def scan_missing_sources(conds):
    missing=set()
    terms=[t for c in conds for t in condition_terms(c)]
    if any(t.get('tf') in ('1m','5m','10m','15m','30m','60m') for t in terms):missing.add('intraday_bars')
    needed={t['field']:EXTRA_TABLES[t['field']] for t in terms if t.get('field') in EXTRA_TABLES}
    for field,table in needed.items():
        if table=='unsupported':missing.add(table);continue
        if not USE_POSTGRES:missing.add(table);continue
        if table=='institutional_trading' and _pg_table_columns('research_institutional_daily'):table='research_institutional_daily'
        if field=='capital_100m' and _pg_table_columns('company_research_profiles'):
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT EXISTS(SELECT 1 FROM public.company_research_profiles WHERE payload->>'capital_100m' IS NOT NULL)")
                    if cur.fetchone()[0]:continue
        cols=_pg_table_columns(table)
        if not cols:missing.add(table);continue
        # A few imported EPS rows do not mean that capital or margins exist.
        aliases={
            'revenue_yoy':('revenue_yoy','revenue_year_growth_rate','yoy','growth_yoy','revenue','monthly_revenue'),
            'revenue_mom':('revenue','monthly_revenue'),
            'net_margin_pct':('net_margin_pct','net_margin'),
            'foreign_net':('foreign_net','foreign_buy_sell'),
            'trust_net':('trust_net','investment_trust_net'),
            'dealer_net':('dealer_net','dealer_buy_sell'),
            'margin_balance':('margin_balance','margin_purchase_balance'),
            'short_balance':('short_balance','short_sale_balance'),
            'margin_volume_ratio':('margin_balance','margin_purchase_balance'),
            'short_volume_ratio':('short_balance','short_sale_balance'),
            'short_margin_ratio':('margin_balance','margin_purchase_balance'),
            'daytrade_ratio':('daytrade_volume',),
        }
        column=next((c for c in aliases.get(field,(field,)) if c in cols),None)
        if column not in cols:missing.add(field);continue
        with db_conn() as c:
            with c.cursor() as cur:
                cur.execute('SELECT EXISTS(SELECT 1 FROM public."'+table+'" WHERE "'+column+'" IS NOT NULL LIMIT 1)')
                if not cur.fetchone()[0]:missing.add(field)
    labels={'capital_100m':'股本','revenue_yoy':'月營收年增率','revenue_mom':'月營收月增率','eps':'EPS','gross_margin':'毛利率','operating_margin':'營益率'}
    return ['⚠️ 缺少'+labels.get(t,DATA_LABELS.get(t,t))+'資料' for t in sorted(missing)]

def scan_plan(conds):
    """Every row is evaluated: comparisons filter; pure arithmetic produces a numeric column."""
    reasons=[]
    for c in conds:
        missing=scan_missing_sources([c])
        reasons.append('；'.join(missing) if missing else '')
    return reasons


def warehouse_mark_failure(code, err):
    if not USE_POSTGRES:return
    with DB_LOCK,db_conn() as c:
        with c.cursor() as cur:
            cur.execute("""INSERT INTO public.warehouse_failures(stock_id,error_text,fail_count,last_failed_at,retry_after)
            VALUES(%s,%s,1,now(),now()+interval '30 minutes')
            ON CONFLICT(stock_id) DO UPDATE SET error_text=EXCLUDED.error_text,fail_count=warehouse_failures.fail_count+1,
            last_failed_at=now(),retry_after=now()+interval '30 minutes'""",(str(code),str(err)[:300]))

def warehouse_clear_failure(code):
    if not USE_POSTGRES:return
    with DB_LOCK,db_conn() as c:
        with c.cursor() as cur:cur.execute("DELETE FROM public.warehouse_failures WHERE stock_id=%s",(str(code),))

def industry_momentum():
    """Sector and attributed theme rotation, using persisted daily bars only."""
    if not USE_POSTGRES:return {"ok":False,"error":"需要 PostgreSQL"}
    with db_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""SELECT count(*) FROM public.stocks
              WHERE stock_id ~ '^[1-9][0-9]{3}$' AND market IN ('上市','上櫃')
                AND COALESCE(industry_name,'')<>''""")
            expected=int(cur.fetchone()['count'])
            cur.execute("""SELECT industry_name,count(*) AS stock_count FROM public.stocks
              WHERE stock_id ~ '^[1-9][0-9]{3}$' AND market IN ('上市','上櫃')
                AND COALESCE(industry_name,'')<>'' GROUP BY industry_name""")
            expected_by_sector={r['industry_name']:int(r['stock_count']) for r in cur.fetchall()}
            cur.execute("""SELECT p.trade_date AS date,p.stock_id AS code,
              s.stock_name AS name,s.industry_name AS industry,p.close_price AS close,
              p.volume,p.turnover
              FROM public.daily_prices p JOIN public.stocks s USING(stock_id)
              WHERE p.trade_date IN
                (SELECT trade_date FROM public.daily_prices GROUP BY trade_date ORDER BY trade_date DESC LIMIT 31)
              AND s.stock_id ~ '^[1-9][0-9]{3}$'
              AND s.market IN ('上市','上櫃') AND COALESCE(s.industry_name,'')<>''
              ORDER BY p.trade_date,p.stock_id""")
            prices=[{'date':str(r['date']),'code':r['code'],'name':r['name'],
                     'industry':r['industry'],**{k:num(r[k]) for k in ('close','volume','turnover')}}
                    for r in cur.fetchall()]
    result=rotation_engine.compute(prices,industry_map.THEMES,expected,expected_by_sector)
    result['rows']=result['sectors']
    result['theme_source']=industry_map.DOCUMENT['source']
    result['retrieved_on']=industry_map.DOCUMENT['retrieved_on']
    result['theme_links']={t['title']:{'id':t['id'],'url':t['url']} for t in industry_map.THEMES}
    result['sector_links']={r['title']:r['url'] for r in json.loads(
        (ROOT/'industry_directory.json').read_text(encoding='utf-8'))['entries']}
    result['computed_at']=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')
    return result


FINANCIAL_JOB={'running':False,'message':'尚未更新官方財報','rows_updated':0,'failed':0,'feeds':[],'started_at':None,'finished_at':None}
FINANCIAL_LOCK=threading.Lock()
FINANCIAL_HISTORY_JOB={'running':False,'message':'尚未回補歷史財報','rows_updated':0,'skipped':0,'failed':0,'periods':[],'started_at':None,'finished_at':None}
FINANCIAL_HISTORY_LOCK=threading.Lock()
TAIEX_JOB={'running':False,'message':'尚未回補加權指數','rows_updated':0,'months_done':0,'failed':0,'errors':[],'started_at':None,'finished_at':None}
TAIEX_LOCK=threading.Lock()

def db_taiex_upsert(rows):
    if not USE_POSTGRES:raise RuntimeError('加權指數需要 PostgreSQL')
    with DB_LOCK,db_conn() as c:
        with c.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS public.market_index_daily(index_code text NOT NULL,trade_date date NOT NULL,close_value numeric NOT NULL,source_url text NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(index_code,trade_date))")
            psycopg2.extras.execute_values(cur,"INSERT INTO public.market_index_daily(index_code,trade_date,close_value,source_url) VALUES %s ON CONFLICT(index_code,trade_date) DO UPDATE SET close_value=EXCLUDED.close_value,source_url=EXCLUDED.source_url,updated_at=now()",[('TAIEX',r['date'],r['close'],r['source_url']) for r in rows],template='(%s,%s::date,%s,%s)')
    return len(rows)

def _taiex_worker():
    try:
        today=datetime.now(ZoneInfo('Asia/Taipei')).date()
        for year,month in taiex_feed.months_ending(today):
            label=f'{year}-{month:02d}'
            TAIEX_JOB['message']='讀取證交所加權指數 '+label
            try:
                rows=taiex_feed.fetch(year,month)
                count=db_taiex_upsert(rows)
                TAIEX_JOB['rows_updated']+=count;TAIEX_JOB['months_done']+=1
                time.sleep(3)
            except Exception as exc:
                TAIEX_JOB['failed']+=1
                TAIEX_JOB['errors'].append({'month':label,'error':str(exc)[:180]})
        TAIEX_JOB['message']='加權指數回補完成' if not TAIEX_JOB['failed'] else '部分月份失敗；已寫入的月份保留，請查看原因'
    except Exception as exc:TAIEX_JOB['message']='指數回補中斷：'+str(exc)[:180];TAIEX_JOB['failed']+=1
    finally:TAIEX_JOB.update(running=False,finished_at=datetime.now().isoformat(timespec='seconds'))

def taiex_start():
    if not USE_POSTGRES:return {'ok':False,'error':'請先連接 PostgreSQL'}
    with TAIEX_LOCK:
        if TAIEX_JOB['running']:return {'ok':True,'already_running':True,'job':dict(TAIEX_JOB)}
        TAIEX_JOB.update(running=True,message='開始回補近十五個月加權指數',rows_updated=0,months_done=0,failed=0,errors=[],started_at=datetime.now().isoformat(timespec='seconds'),finished_at=None)
        threading.Thread(target=_taiex_worker,daemon=True).start()
        return {'ok':True,'job':dict(TAIEX_JOB)}

def db_official_financial_upsert(records):
    if not USE_POSTGRES:raise RuntimeError('官方財報更新需要 PostgreSQL')
    with DB_LOCK,db_conn() as c:
        with c.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS public.official_financial_reports(stock_id text NOT NULL,kind text NOT NULL,report_date date NOT NULL,payload jsonb NOT NULL,original_row jsonb NOT NULL,source_url text NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(stock_id,kind,report_date))")
            values=[(r['code'],r['kind'],r['date'],json.dumps(r['payload'],ensure_ascii=False),json.dumps(r['original'],ensure_ascii=False),r['payload']['__source_url']) for r in records]
            psycopg2.extras.execute_values(cur,"INSERT INTO public.official_financial_reports(stock_id,kind,report_date,payload,original_row,source_url) VALUES %s ON CONFLICT(stock_id,kind,report_date) DO UPDATE SET payload=official_financial_reports.payload || EXCLUDED.payload,original_row=EXCLUDED.original_row,source_url=EXCLUDED.source_url,updated_at=now()",values,template='(%s,%s,%s::date,%s::jsonb,%s::jsonb,%s)',page_size=200)
    return len(records)

def _financial_worker():
    try:
        for feed in financial_feeds.FEEDS:
            label=('上市' if feed['market']=='L' else '上櫃')+('損益表' if feed['kind']=='income' else '資產負債表')
            FINANCIAL_JOB['message']='讀取官方'+label+'…'
            try:
                records,meta=financial_feeds.fetch_feed(feed)
                count=db_official_financial_upsert(records)
                FINANCIAL_JOB['rows_updated']+=count
                FINANCIAL_JOB['feeds'].append({'label':label,'ok':True,'written':count,'latest_period':max(r['date'] for r in records),**meta})
            except Exception as exc:
                FINANCIAL_JOB['failed']+=1
                FINANCIAL_JOB['feeds'].append({'label':label,'ok':False,'error':str(exc)[:800]})
        FINANCIAL_JOB['message']=('部分來源失敗；已成功資料保留' if FINANCIAL_JOB['rows_updated'] else '更新失敗；未寫入任何資料') if FINANCIAL_JOB['failed'] else '官方財報更新完成（一般業；累計損益與期末資產負債）'
    finally:
        FINANCIAL_JOB.update(running=False,finished_at=datetime.now().isoformat(timespec='seconds'))

def financial_start():
    if not USE_POSTGRES:return {'ok':False,'error':'請先設定 PostgreSQL 連線，未啟動更新'}
    with FINANCIAL_LOCK:
        if FINANCIAL_JOB['running']:return {'ok':True,'already_running':True,'job':dict(FINANCIAL_JOB)}
        FINANCIAL_JOB.update(running=True,message='開始官方財報整批更新；不使用 FinMind',rows_updated=0,failed=0,feeds=[],started_at=datetime.now().isoformat(timespec='seconds'),finished_at=None)
        threading.Thread(target=_financial_worker,daemon=True).start()
        return {'ok':True,'job':dict(FINANCIAL_JOB)}

def _historical_period_count(kind,market,day):
    if not _pg_table_columns('official_financial_reports'):return 0
    with db_conn() as c:
        with c.cursor() as cur:
            cur.execute("SELECT count(*) FROM public.official_financial_reports WHERE kind=%s AND report_date=%s AND payload->>'__historical_verified'='true' AND payload->>'__source_market'=%s",(kind,day,market))
            return cur.fetchone()[0]

def _financial_history_worker():
    try:
        blocked=set()
        for year,quarter in mops_history.recent_quarters():
            for kind in ('income','balance'):
                for market in ('sii','otc'):
                    label=f'{year} Q{quarter} '+('上市' if market=='sii' else '上櫃')+('損益' if kind=='income' else '資產負債')
                    if (kind,market) in blocked:continue
                    FINANCIAL_HISTORY_JOB['message']='讀取 '+label
                    try:
                        day=mops_history.quarter_end(year,quarter)
                        if _historical_period_count(kind,market,day)>=20:
                            FINANCIAL_HISTORY_JOB['skipped']+=1;continue
                        records=mops_history.fetch(kind,market,year,quarter)
                        for record in records:record['payload'].update(__source_market=market,__historical_verified=True)
                        count=db_official_financial_upsert(records)
                        FINANCIAL_HISTORY_JOB['rows_updated']+=count
                        FINANCIAL_HISTORY_JOB['periods'].append({'label':label,'ok':True,'written':count})
                        time.sleep(2)
                    except Exception as exc:
                        FINANCIAL_HISTORY_JOB['failed']+=1
                        FINANCIAL_HISTORY_JOB['periods'].append({'label':label,'ok':False,'error':str(exc)[:250]})
                        if any(s in str(exc) for s in ('拒絕請求','有效公司僅','HTTP Error 403','HTTP Error 429')):blocked.add((kind,market))
        written=FINANCIAL_HISTORY_JOB['rows_updated'];failed=FINANCIAL_HISTORY_JOB['failed']
        FINANCIAL_HISTORY_JOB['message']=('部分期別失敗；詳見下方原因' if written else '歷史回補失敗；未覆蓋既有資料') if failed else '歷史財報回補完成'
    except Exception as exc:FINANCIAL_HISTORY_JOB['message']='回補中斷：'+str(exc)[:300];FINANCIAL_HISTORY_JOB['failed']+=1
    finally:FINANCIAL_HISTORY_JOB.update(running=False,finished_at=datetime.now().isoformat(timespec='seconds'))

def financial_history_start():
    if not USE_POSTGRES:return {'ok':False,'error':'歷史財報需要 PostgreSQL'}
    with FINANCIAL_HISTORY_LOCK:
        if FINANCIAL_HISTORY_JOB['running']:return {'ok':True,'already_running':True,'job':dict(FINANCIAL_HISTORY_JOB)}
        FINANCIAL_HISTORY_JOB.update(running=True,message='開始回補近九季合併財報',rows_updated=0,skipped=0,failed=0,periods=[],started_at=datetime.now().isoformat(timespec='seconds'),finished_at=None)
        threading.Thread(target=_financial_history_worker,daemon=True).start()
        return {'ok':True,'job':dict(FINANCIAL_HISTORY_JOB)}


def db_health_raw_upsert(code,dataset,records):
    allowed=('TaiwanStockFinancialStatements','TaiwanStockBalanceSheet','TaiwanStockCashFlowsStatement','TaiwanStockPER')
    if dataset not in allowed or not USE_POSTGRES:return 0
    grouped={}
    for r in records:
        day=str(r.get('date') or '')[:10]
        try:date.fromisoformat(day)
        except ValueError:continue
        values=grouped.setdefault(day,{})
        # Preserve explicit source period metadata; a December date alone is not proof of annual flows.
        basis=r.get('period_basis')
        if basis in ('annual','single_quarter'):values['__period_basis']=basis
        if dataset=='TaiwanStockPER':
            for key in ('PER','PBR'):
                value=num(r.get(key))
                if value is not None:values[key]=value
        else:
            value=num(r.get('value'))
            if value is None:continue
            for key in (r.get('type'),r.get('origin_name')):
                if key:values[str(key)]=value
    if not grouped:return 0
    with DB_LOCK,db_conn() as c:
        with c.cursor() as cur:
            cur.execute('CREATE TABLE IF NOT EXISTS public.health_financial_raw (stock_id text NOT NULL,dataset text NOT NULL,report_date date NOT NULL,payload jsonb NOT NULL,source_name text NOT NULL DEFAULT \'FinMind\',updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(stock_id,dataset,report_date))')
            psycopg2.extras.execute_values(cur,'INSERT INTO public.health_financial_raw(stock_id,dataset,report_date,payload) VALUES %s ON CONFLICT(stock_id,dataset,report_date) DO UPDATE SET payload=EXCLUDED.payload,updated_at=now()',[(code,dataset,day,json.dumps(values,ensure_ascii=False)) for day,values in grouped.items()],template='(%s,%s,%s::date,%s::jsonb)',page_size=100)
    return len(grouped)

def db_health_history(table,code,fields,limit=130):
    if not USE_POSTGRES:return []
    cols=_pg_table_columns(table)
    cc=next((x for x in ('stock_id','stock_code','code') if x in cols),None);dc=next((x for x in ('trade_date','period_date','date') if x in cols),None)
    chosen=[x for x in fields if x in cols]
    if not cc or not dc or not chosen:return []
    with db_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SELECT "'+dc+'" AS date,'+','.join('"'+x+'"' for x in chosen)+' FROM public."'+table+'" WHERE "'+cc+'"=%s ORDER BY "'+dc+'" DESC LIMIT %s',(str(code),limit))
            return [{k:str(v) if k=='date' else num(v) for k,v in dict(r).items()} for r in reversed(cur.fetchall())]

_HEALTH_CACHE={}
_PEER_CACHE={}
def db_stock_health(code):
    k=str(code);now=time.time();hit=_HEALTH_CACHE.get(k)
    if hit and now-hit[0]<900:
        out=dict(hit[1]);out['cached']=True;return out
    out=_db_stock_health(code)
    if out.get('ok') and not out.get('data_errors'):
        _HEALTH_CACHE[k]=(now,out)
        if len(_HEALTH_CACHE)>300:_HEALTH_CACHE.pop(next(iter(_HEALTH_CACHE)))
    return out

def _db_stock_health(code):
    t0=time.time();tm={}
    def mark(name):tm[name]=round(time.time()-t0,2)
    hist=db_history(code,260);mark('日K');extras=db_chip_fundamental(code) if USE_POSTGRES else {};errors=[];mark('籌碼基本面')
    stock={}
    if USE_POSTGRES:
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT stock_id AS code,stock_name AS name,market,COALESCE(industry_name,'') AS industry FROM public.stocks WHERE stock_id=%s",(str(code),))
                r=cur.fetchone();stock=dict(r) if r else {}
    else:stock=next((r for r in db_universe('both') if r['code']==str(code)),{})
    mark('股票資料')
    industry=stock.get('industry','');raw={};revenue=[];institutions=[];margins=[];benchmark=[]
    if USE_POSTGRES:
        try:
            revenue=db_company_revenue(code);yoy=db_revenue_yoy_series(code,24)
            extras['revenue_yoy_series']=yoy
            if yoy:extras['revenue_yoy']=yoy[-1]
            if revenue:extras['revenue_period']=revenue[0]['period'][:7]
        except Exception as e:errors.append('月營收：'+type(e).__name__)
        mark('月營收')
        try:
            if _pg_table_columns('health_financial_raw'):
                with db_conn() as c:
                    with c.cursor() as cur:
                        cur.execute('SELECT dataset,report_date,payload FROM public.health_financial_raw WHERE stock_id=%s ORDER BY report_date',(str(code),))
                        for dataset,day,payload in cur.fetchall():
                            kind={'TaiwanStockFinancialStatements':'income','TaiwanStockBalanceSheet':'balance','TaiwanStockCashFlowsStatement':'cash','TaiwanStockPER':'valuation'}.get(dataset)
                            if kind:raw.setdefault(kind,{})[str(day)]=json.loads(payload) if isinstance(payload,str) else payload
            if _pg_table_columns('official_financial_reports'):
                with db_conn() as c:
                    with c.cursor() as cur:
                        cur.execute('SELECT kind,report_date,payload FROM public.official_financial_reports WHERE stock_id=%s ORDER BY report_date',(str(code),))
                        financial_feeds.merge_official(raw,[{'kind':kind,'date':str(day),'payload':json.loads(payload) if isinstance(payload,str) else payload} for kind,day,payload in cur.fetchall()])
            extras.update(health_engine.financial_facts(raw))
            if raw.get('valuation'):
                day=max(raw['valuation']);extras['pe']=num(raw['valuation'][day].get('PER'));extras['valuation_date']=day;extras['pb']=num(raw['valuation'][day].get('PBR'))
            legacy=db_health_history('financials',code,['gross_margin','operating_margin','roe_pct','debt_ratio_pct','current_ratio_pct','quick_ratio_pct','financial_revenue_yoy'],12)
            if legacy and not raw.get('income') and not raw.get('balance'):
                extras['financials_date']=legacy[-1]['date']
                for k,v in legacy[-1].items():
                    if k!='date' and v is not None and extras.get(k) is None:extras[k]=v
                if not extras.get('financial_history'):extras['financial_history']=legacy
            # Compare gross margin only with available companies in the same official category and report period.
            peer_table='official_financial_reports' if extras.get('income_source_url') else 'health_financial_raw'
            peer_key=(peer_table,industry,extras.get('financials_date'),extras.get('income_basis'))
            if peer_key in _PEER_CACHE and time.time()-_PEER_CACHE[peer_key][0]<21600:
                extras['peer_count'],pm=_PEER_CACHE[peer_key][1]
                if pm is not None:extras['peer_gross_margin']=pm
            elif industry and extras.get('financials_date') and _pg_table_columns(peer_table):
                with db_conn() as c:
                    with c.cursor() as cur:
                        dataset="r.kind='income'" if peer_table=='official_financial_reports' else "r.dataset='TaiwanStockFinancialStatements'"
                        cur.execute('SELECT r.payload FROM public.'+peer_table+' r JOIN public.stocks s ON s.stock_id=r.stock_id WHERE '+dataset+' AND r.report_date=%s AND s.industry_name=%s AND r.stock_id<>%s',(extras['financials_date'],industry,str(code)))
                        peers=[]
                        for (payload,) in cur.fetchall():
                            payload=json.loads(payload) if isinstance(payload,str) else payload
                            if payload.get('__period_basis','unknown')!=extras.get('income_basis'):continue
                            facts=health_engine.financial_facts({'income':{extras['financials_date']:payload}})
                            if facts.get('gross_margin') is not None:peers.append(facts['gross_margin'])
                        extras['peer_count']=len(peers)
                        if len(peers)>=5:extras['peer_gross_margin']=statistics.median(peers)
                        _PEER_CACHE[peer_key]=(time.time(),(len(peers),statistics.median(peers) if len(peers)>=5 else None))
        except Exception as e:errors.append('財報：'+type(e).__name__)
        mark('財報＋同業比較')
        try:
            table='research_institutional_daily' if _pg_table_columns('research_institutional_daily') else 'institutional_trading'
            institutions=db_health_history(table,code,['foreign_net','trust_net','dealer_net'],20)
            margins=db_health_history('margin_short',code,['margin_balance','short_balance'],30)
            # A stock/ETF cannot stand in for the market index; use only explicitly persisted TAIEX bars.
            if _pg_table_columns('market_index_daily'):
                cols=_pg_table_columns('market_index_daily')
                if {'index_code','trade_date','close_value'}<=cols:
                    with db_conn() as c:
                        with c.cursor() as cur:
                            cur.execute("SELECT trade_date,close_value FROM public.market_index_daily WHERE index_code='TAIEX' ORDER BY trade_date DESC LIMIT 260")
                            benchmark=[{'date':str(day),'close':num(value)} for day,value in reversed(cur.fetchall())]
        except Exception as e:errors.append('籌碼／指數：'+type(e).__name__)
        mark('法人／融資券／大盤')
    ttm=roe_ttm.for_stock(db_conn,code,USE_POSTGRES,extras.get('pe'),extras.get('pb'))
    roe_ttm.apply(extras,ttm);mark('ROE')
    estimate_notes=estimates.fill(extras,revenue);mark('估值推估')
    result=health_engine.report(hist,extras,revenue,institutions,margins,benchmark,industry)
    mark('健康度計算');result.update(health_scoring.finalize(result))
    result['valuation']={'pe':extras.get('pe'),'pb':extras.get('pb'),'date':extras.get('valuation_date')}
    result['estimates']=estimate_notes
    result['debt_quality']=debt_quality.for_stock(db_conn,code,USE_POSTGRES,industry);mark('負債品質')
    # Keep the dashboard evidence next to its calculated health score. In particular,
    # an absent institutional trading day is not treated as a zero-net day.
    revenue_chart=[]
    revenue_by_period={str(x['period'])[:7]:x.get('revenue') for x in revenue}
    for month,value in sorted(revenue_by_period.items())[-12:]:
        previous=revenue_by_period.get(str(int(month[:4])-1)+month[4:])
        revenue_chart.append({'period':month,'revenue':value,
            'yoy':(value/previous-1)*100 if value is not None and previous is not None and previous>0 else None})
    institution_chart=[]
    for row in institutions[-60:]:
        institution_chart.append({'date':str(row.get('date')),
            **{k:num(row.get(k)) for k in ('foreign_net','trust_net','dealer_net')}})
    result.update(ok=True,code=str(code),name=stock.get('name',''),industry=industry,
        market=stock.get('market',''),prices=hist[-240:],
        revenue_history=revenue_chart,financial_history=extras.get('financial_history',[]),
        institutions_history=institution_chart,
        data_errors=errors)
    mark('合計');result['timings']=tm
    return result


def health_from_hist(hist, extras=None):
    if not hist or len(hist)<20:return {"score":None,"level":"⚪","coverage":"日 K 少於 20 根，尚無完整健康度。","items":[],"categories":{x:"尚無資料" for x in ('財務','成長','籌碼','價量','風險')}}
    closes=[num(x.get('close')) for x in hist if num(x.get('close')) is not None and num(x.get('close'))>0]
    vols=[num(x.get('volume')) or 0 for x in hist if num(x.get('close')) is not None and num(x.get('close'))>0]
    if len(closes)<20:return {"score":None,"level":"⚪","coverage":"有效日 K 少於 20 根，尚無完整健康度。","items":[],"categories":{x:"尚無資料" for x in ('財務','成長','籌碼','價量','風險')}}
    c=closes[-1]; ma20=sum(closes[-20:])/20; ma60=sum(closes[-60:])/min(60,len(closes));
    hi=max(closes[-60:]); dd=(c/hi-1)*100 if hi else 0
    rets=[closes[i]/closes[i-1]-1 for i in range(max(1,len(closes)-20),len(closes)) if closes[i-1]]
    volat=(sum(x*x for x in rets)/len(rets))**0.5*100 if rets else 0
    avgv=sum(vols[-20:])/min(20,len(vols)) if vols else 0
    score=50
    score += 12 if c>=ma20 else -10
    score += 10 if c>=ma60 else -8
    score += 10 if dd>=-10 else (3 if dd>=-20 else -8)
    score += 8 if volat<=3 else (2 if volat<=5 else -6)
    score += 10 if avgv>0 else -10
    score=max(0,min(100,round(score)))
    level='🟢' if score>=75 else ('🟡' if score>=55 else '🔴')
    items=[{"name":"20日趨勢","ok":c>=ma20,"detail":f"收盤 {c:.2f}／20日均 {ma20:.2f}"},
           {"name":"中期趨勢","ok":c>=ma60,"detail":f"60日均 {ma60:.2f}"},
           {"name":"回撤風險","ok":dd>=-20,"detail":f"距60日高點 {dd:.1f}%"},
           {"name":"波動","ok":volat<=5,"detail":f"近20日波動約 {volat:.1f}%"}]
    extras=extras or {}
    finance=[f'{label} {num(extras[key]):.2f}' for key,label in
             (('eps','單季 EPS'),('gross_margin','毛利率'),('operating_margin','營益率')) if num(extras.get(key)) is not None]
    chip=[f'{label} {num(extras[key]):,.0f}' for key,label in
          (('foreign_net','外資淨買賣'),('trust_net','投信淨買賣'),('dealer_net','自營商淨買賣')) if num(extras.get(key)) is not None]
    growth=num(extras.get('revenue_yoy'))
    categories={"財務":('期間 '+extras.get('financials_date','日期未記錄')+'｜'+'／'.join(finance)) if finance else "尚無資料",
                "成長":f"最新月營收年增 {growth:.2f}%" if growth is not None else "尚無資料",
                "籌碼":('交易日 '+extras.get('institutional_trading_date','日期未記錄')+'｜'+'／'.join(chip)) if chip else "尚無資料",
                "價量":f"已檢核（{score}）","風險":"已檢核價量波動與回撤"}
    return {"score":None,"price_score":score,"level":"⚪","asof":str(hist[-1].get('date') or ''),"bars":len(closes),"coverage":"價量分數是本工作台的檢核規則；完整健康度須等財報、月營收與籌碼資料入庫，缺項不計分。","items":items,
            "categories":categories}

def twse_etf_dividends():
    """Official TWSE ETF e添富 dividend list. Best-effort parser; never invents missing amounts."""
    k="twse_etf_dividends"; now=time.time()
    if k in CACHE and now-CACHE[k][0]<21600:return CACHE[k][1]
    url="https://www.twse.com.tw/zh/ETFortune/dividendList"
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"text/html"})
    raw=None;last=None
    for tmo in (15,25):
        try:
            with urllib.request.urlopen(req,timeout=tmo) as r: raw=r.read().decode("utf-8","ignore")
            break
        except Exception as exc:last=exc
    if raw is None:raise last
    trs=re.findall(r"<tr[^>]*>(.*?)</tr>",raw,re.I|re.S)
    out={}
    def clean(x):
        x=re.sub(r"<script.*?</script>|<style.*?</style>","",x,flags=re.I|re.S)
        return html.unescape(re.sub(r"<[^>]+>","",x)).replace("\xa0"," ").strip()
    def rocdate(x):
        m=re.search(r"(\d{2,3})年(\d{1,2})月(\d{1,2})日",x)
        if m:return f"{int(m.group(1))+1911:04d}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"
        m=re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})",x)
        return f"{int(m.group(1)):04d}/{int(m.group(2)):02d}/{int(m.group(3)):02d}" if m else ""
    for tr in trs:
        cells=[clean(x) for x in re.findall(r"<td[^>]*>(.*?)</td>",tr,re.I|re.S)]
        if len(cells)<6:continue
        code=cells[0].strip()
        if not re.fullmatch(r"[0-9A-Z]{4,8}",code):continue
        exd,base,pay=rocdate(cells[2]),rocdate(cells[3]),rocdate(cells[4])
        amount=num(cells[5].replace(",",""))
        if not exd:continue
        out.setdefault(code,[]).append({"ex_date":exd,"base_date":base,"pay_date":pay,"amount":amount})
    for code in out:out[code].sort(key=lambda x:x["ex_date"],reverse=True)
    CACHE[k]=(now,out)
    if out:
        try:persist_etf_dividends(out)
        except Exception:pass
    return out

def persist_etf_dividends(divs):
    if not USE_POSTGRES:return
    with db_conn() as c:
      with c.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS public.etf_catalog_snapshots(snapshot_key text PRIMARY KEY,payload jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())")
        cur.execute("INSERT INTO public.etf_catalog_snapshots(snapshot_key,payload) VALUES ('dividends',%s::jsonb) ON CONFLICT(snapshot_key) DO UPDATE SET payload=excluded.payload,updated_at=now()",(json.dumps(divs,ensure_ascii=False),))
      c.commit()

def saved_etf_dividends():
    """Last successfully fetched official dividend list (survives Render restarts)."""
    if not USE_POSTGRES:return {}
    try:
        with db_conn() as c:
          with c.cursor() as cur:
            cur.execute("SELECT to_regclass('public.etf_catalog_snapshots')")
            if not cur.fetchone()[0]:return {}
            cur.execute("SELECT payload FROM public.etf_catalog_snapshots WHERE snapshot_key='dividends'")
            row=cur.fetchone()
            return row[0] if row and isinstance(row[0],dict) else {}
    except Exception:return {}

def saved_etf_catalog():
    if not USE_POSTGRES:return None
    with db_conn() as c:
      with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT to_regclass('public.etf_catalog_snapshots') AS table_name")
        if not cur.fetchone()['table_name']:return None
        cur.execute("SELECT payload FROM public.etf_catalog_snapshots WHERE snapshot_key='official'")
        row=cur.fetchone()
        return row['payload'] if row else None

def persist_etf_catalog(rows,audit):
    if not USE_POSTGRES:return
    with db_conn() as c:
      with c.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS public.etf_catalog_snapshots(snapshot_key text PRIMARY KEY,payload jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())")
        cur.execute("INSERT INTO public.etf_catalog_snapshots(snapshot_key,payload) VALUES ('official',%s::jsonb) ON CONFLICT(snapshot_key) DO UPDATE SET payload=excluded.payload,updated_at=now()",(json.dumps({'rows':rows,'audit':audit},ensure_ascii=False),))
      c.commit()

HOLD_CACHE={}
def _hold_db_get(code):
    if not USE_POSTGRES:return None
    try:
        with db_conn() as c:
          with c.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS public.etf_holdings_cache(etf_id text PRIMARY KEY,payload jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())")
            cur.execute("SELECT payload,extract(epoch from now()-updated_at) FROM public.etf_holdings_cache WHERE etf_id=%s",(code,))
            r=cur.fetchone()
          c.commit()
        return (r[0],float(r[1])) if r else None
    except Exception:return None

def _hold_db_set(code,payload):
    if not USE_POSTGRES:return
    try:
        with db_conn() as c:
          with c.cursor() as cur:
            cur.execute("INSERT INTO public.etf_holdings_cache(etf_id,payload) VALUES (%s,%s::jsonb) ON CONFLICT(etf_id) DO UPDATE SET payload=excluded.payload,updated_at=now()",(code,json.dumps(payload,ensure_ascii=False)))
          c.commit()
    except Exception:pass

def _fetch_moneydj_holdings(code):
    url="https://www.moneydj.com/etf/x/basic/basic0007b.xdjhtm?etfid="+urllib.parse.quote(code.lower())+".tw"
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36","Accept-Language":"zh-TW,zh;q=0.9"})
    with urllib.request.urlopen(req,timeout=20) as r:raw=r.read()
    txt=None
    for enc in ("utf-8","big5","cp950"):
        try:txt=raw.decode(enc);break
        except Exception:pass
    txt=txt or raw.decode("utf-8","replace")
    rows=[]
    for m in re.finditer(r'<td class="col05">(.*?)</td>\s*<td class="col06">\s*([\d.,]+)\s*</td>\s*<td class="col07">\s*([\d.,]*)\s*</td>',txt,re.S):
        label=html.unescape(re.sub(r"<[^>]+>","",m.group(1))).strip()
        mm=re.match(r"(.*?)\s*\(([0-9A-Za-z]+)\.TW\)\s*$",label)
        name,sid=(mm.group(1).strip(),mm.group(2)) if mm else (label,"")
        try:w=float(m.group(2).replace(",",""))
        except Exception:continue
        sh=num(m.group(3).replace(",","")) if m.group(3) else None
        rows.append({"code":sid,"name":name,"weight":w,"shares":sh})
    dm=re.search(r"資料日期[：:]\s*(20\d{2}[/-]\d{1,2}[/-]\d{1,2})",txt)
    return {"ok":True,"code":code,"source":"MoneyDJ 理財網（ETF 持股明細）","source_url":url,"asof":dm.group(1) if dm else None,"holdings":rows,"total_weight":round(sum(x["weight"] for x in rows),2)}

def etf_holdings(code):
    code=re.sub(r"[^0-9A-Za-z]","",code or "").upper()
    if not code:return {"ok":False,"error":"請提供 ETF 代號"}
    now=time.time();c=HOLD_CACHE.get(code)
    if c and now-c[0]<43200:return c[1]
    stale=_hold_db_get(code)
    if stale and stale[1]<43200:
        HOLD_CACHE[code]=(now,dict(stale[0],cached=True));return HOLD_CACHE[code][1]
    try:
        d=_fetch_moneydj_holdings(code)
        if not d["holdings"]:raise RuntimeError("來源頁面沒有持股表（可能是新上市、債券型或主動式 ETF）")
        d["fetched_at"]=datetime.now().strftime("%Y-%m-%d %H:%M")
        HOLD_CACHE[code]=(now,d);_hold_db_set(code,d);return d
    except Exception as exc:
        if stale:return dict(stale[0],cached=True,stale=True,note="最新資料抓取失敗，顯示上次資料："+str(exc)[:80])
        return {"ok":False,"code":code,"error":"取得成分股失敗："+type(exc).__name__+" "+str(exc)[:120]}

ETF_LOAD_LOCK=threading.Lock()
ETF_LOAD_STATE={'running':False,'payload':None,'error':None}

def etf_catalog_view():
    """Searchable master list: never wait for optional remote metrics."""
    rows=[]
    if USE_POSTGRES:
      with db_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
          cur.execute("SELECT stock_id,stock_name,market FROM public.stocks WHERE stock_id LIKE '00%%' ORDER BY stock_id")
          rows=[dict(r) for r in cur.fetchall()]
    # Official verified identity; not a fabricated quote or completeness claim.
    if not any(r['stock_id']=='00981A' for r in rows):
      rows.append({'stock_id':'00981A','stock_name':'主動統一台股增長','market':'上市','catalog_source':'https://www.twse.com.tw/zh/ETFortune/etfInfo/00981A'})
    saved=saved_etf_catalog()
    if saved:
      rows=saved['rows']
      audit=dict(saved['audit'],refresh_error=ETF_LOAD_STATE.get('error'))
    else:audit={'verified':False,'total_count':None,'error':'尚未完成官方全名錄核對'}
    payload=ETF_LOAD_STATE.get('payload')
    if payload:return dict(payload,loading=ETF_LOAD_STATE['running'],load_error=ETF_LOAD_STATE['error'])
    for r in rows:r['group']=etf_research_tools.strategy_group(r['stock_name'],r['stock_id'])
    return {'ok':True,'rows':rows,'groups':{},'catalog_error':None if saved else '完整官方名錄尚未載入；目前為資料庫候選及已核對商品，不能視為全部 ETF。','selection_basis':'先提供代號與名稱搜尋；行情、人氣與配息背景載入。','note':'缺少資料的欄位不估算。','manager_win_rate_definition':'尚無完整建倉紀錄，不計算經理人勝率。','loading':ETF_LOAD_STATE['running'],'load_error':ETF_LOAD_STATE['error'],'catalog_audit':audit}

def etf_research_background():
    with ETF_LOAD_LOCK:
      if ETF_LOAD_STATE['running']:return
      ETF_LOAD_STATE['running']=True
      ETF_LOAD_STATE['error']=None
    def worker():
      try:
        quick=etf_research(fast=True)
        if quick.get('ok'):ETF_LOAD_STATE['payload']=quick   # 先出行情＋人氣，不等官方網站
        payload=etf_research()
        if not payload.get('ok'):raise ValueError(payload.get('error','ETF 載入失敗'))
        ETF_LOAD_STATE['payload']=payload
      except Exception as exc:ETF_LOAD_STATE['error']=str(exc)
      finally:ETF_LOAD_STATE['running']=False
    threading.Thread(target=worker,daemon=True).start()

def etf_research(fast=False):
    """fast=True：只用資料庫＋上次核對名錄（不連官方網站），讓頁面先有行情與人氣；完整版再補官方名錄、榜單與配息。"""
    if not USE_POSTGRES:return {"ok":False,"error":"需要 PostgreSQL"}
    with db_conn() as c:
      with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT to_regclass('public.etf_daily_prices') AS t")
        has_etf_daily=bool(cur.fetchone()['t'])
        # 價格來源：股票主檔裡的 ETF（daily_prices）＋ etf_daily_prices（全部 ETF 官方日K，不依賴主檔）；同日以 daily_prices 為準
        extra="UNION ALL SELECT stock_id,trade_date,close_price,turnover,2 AS pri FROM public.etf_daily_prices" if has_etf_daily else ""
        cur.execute("""WITH src AS (
          SELECT p.stock_id,p.trade_date,p.close_price,p.turnover,1 AS pri
          FROM public.daily_prices p JOIN public.stocks s ON s.stock_id=p.stock_id
          WHERE (s.stock_id LIKE '00%%' OR s.stock_name ILIKE '%%ETF%%')
          """+extra+"""
        ), u AS (SELECT DISTINCT ON (stock_id,trade_date) * FROM src ORDER BY stock_id,trade_date,pri),
        h AS (
          SELECT u.stock_id,COALESCE(s.stock_name,u.stock_id) AS stock_name,COALESCE(s.market,'') AS market,
                 u.trade_date,u.close_price,u.turnover,
                 row_number() over(partition by u.stock_id order by u.trade_date desc) rn
          FROM u LEFT JOIN public.stocks s ON s.stock_id=u.stock_id
        ), a AS (SELECT stock_id,stock_name,market,
          max(close_price) filter(where rn=1) close,
          max(trade_date) filter(where rn=1) price_date,
          max(close_price) filter(where rn=21) p20,
          max(close_price) filter(where rn=61) p60,
          max(turnover) filter(where rn=1) latest_turnover,
          avg(turnover) filter(where rn<=20) avg20_turnover,
          count(turnover) filter(where rn<=20) turnover_samples
          FROM h WHERE rn<=61 GROUP BY stock_id,stock_name,market)
        SELECT * FROM a WHERE close IS NOT NULL ORDER BY stock_id""")
        rows=[dict(r) for r in cur.fetchall()]
    catalog_error=None
    catalog_audit={'verified':False,'error':'尚未完成官方名錄核對'}
    try:
      if fast:raise RuntimeError('FAST')
      catalog=etf_research_tools.fetch_catalog()
      known={str(r['stock_id']):r for r in rows}
      with db_conn() as c:
        with c.cursor() as cur:
          cur.execute('SELECT stock_id FROM public.stocks')
          master=[r[0] for r in cur.fetchall()]
      checked=datetime.utcnow().isoformat()+'Z'
      catalog_audit=etf_research_tools.catalog_audit(catalog,master,checked)
      catalog_audit['missing_price_codes']=[r['stock_id'] for r in catalog if r['stock_id'] not in known]
      persist_etf_catalog(catalog,catalog_audit)
      rows=[dict(known.get(x['stock_id'],{}),**x) for x in catalog]
    except Exception as exc:
      catalog_error=None if str(exc)=='FAST' else str(exc)
      saved=saved_etf_catalog()
      if saved:
        known={r['stock_id']:r for r in rows}
        rows=[dict(known.get(x['stock_id'],{}),**x) for x in saved['rows']]
        catalog_audit=dict(saved['audit'],refresh_error=catalog_error)
      if not saved and not any(r['stock_id']=='00981A' for r in rows):rows.append({'stock_id':'00981A','stock_name':'主動統一台股增長','market':'上市'})
    for r in rows:
      for key in ('close','p20','p60','latest_turnover','avg20_turnover','price_date','turnover_samples'):r.setdefault(key,None)
      for k in ('close','p20','p60','latest_turnover','avg20_turnover'):
        if r.get(k) is not None:r[k]=float(r[k])
      r['r20']=((r['close']/r['p20']-1)*100) if r.get('p20') else None
      r['r60']=((r['close']/r['p60']-1)*100) if r.get('p60') else None
      r['group']=etf_research_tools.strategy_group(r['stock_name'],r['stock_id'])
      r['manager_win_rate_10']=None;r['manager_win_rate_20']=None;r['manager_win_rate_60']=None
    # Page parser requires a unique official date and explicit table headings.
    # Its failure must leave exchange-derived fields unknown, not fabricated.
    pop_error=None
    key='official_etf_popularity';cached=CACHE.get(key)
    try:
      if cached and time.time()-cached[0]<21600:popular=cached[1]
      elif fast:popular={}
      else:
        popular=etf_research_tools.fetch_popularity();CACHE[key]=(time.time(),popular)
    except Exception as exc:popular={};pop_error=type(exc).__name__+': '+str(exc)[:140]
    for r in rows:
      official=popular.get(str(r['stock_id']),{})
      for key in ('beneficiaries','assets','turnover','asof'):r['official_'+key]=official.get(key)
    div_source="臺灣證券交易所 ETF e添富"
    try: divs=saved_etf_dividends() if fast else twse_etf_dividends()
    except Exception:
      divs=saved_etf_dividends();div_source="官方配息資料暫時無法取得"+("（先顯示上次核對的資料）" if divs else "")
    if fast:div_source="（先顯示上次核對的配息；最新配息背景更新中）" if divs else "（配息背景載入中）"
    today_s=date.today().strftime("%Y/%m/%d"); cutoff=(date.today()-timedelta(days=365)).strftime("%Y/%m/%d")
    for r in rows:
      ds=divs.get(str(r['stock_id']),[])
      past=[x for x in ds if x.get('ex_date','')<=today_s]
      latest=past[0] if past else None
      r['last_dividend']=latest.get('amount') if latest else None
      r['ex_date']=latest.get('ex_date') if latest else None
      r['pay_date']=latest.get('pay_date') if latest else None
      vals=[x.get('amount') for x in past if x.get('ex_date','')>=cutoff and x.get('amount') is not None]
      r['dividend_12m']=sum(vals) if vals else None
      announced=[x for x in ds if x.get('ex_date','')>today_s]
      r['next_announced']=announced[-1] if announced else None
      r['dividend_history']=ds[:20]
      r['dividend_yield_12m']=r['dividend_12m']/r['close']*100 if r.get('dividend_12m') is not None and r.get('close') else None
      r['price_date']=str(r['price_date']) if r.get('price_date') else None
    # 人氣＝集保「戶股權分散表」持有人數（ETF 的受益人數），每週存檔 public.holder_weekly；涵蓋全部 ETF，不靠官方榜單
    hold_info={'latest':None,'previous':None,'covered':0,'error':None}
    try:
      with db_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
          cur.execute("SELECT to_regclass('public.holder_weekly') AS t")
          if cur.fetchone()['t']:
            cur.execute("SELECT DISTINCT data_date FROM public.holder_weekly ORDER BY data_date DESC LIMIT 2")
            ds=[r['data_date'] for r in cur.fetchall()]
            if ds:
              codes=[str(r['stock_id']) for r in rows]
              cur.execute("SELECT stock_id,data_date,holders_total FROM public.holder_weekly WHERE stock_id=ANY(%s) AND data_date=ANY(%s)",(codes,ds))
              by={}
              for h in cur.fetchall():by.setdefault(str(h['stock_id']),{})[h['data_date']]=h['holders_total']
              L=ds[0];P=ds[1] if len(ds)>1 else None
              hold_info.update(latest=str(L),previous=str(P) if P else None)
              def ranks(d):
                vals=sorted(((v.get(d),k) for k,v in by.items() if v.get(d) is not None),key=lambda t:(-t[0],t[1]))
                return {k:i+1 for i,(_,k) in enumerate(vals)}
              rl=ranks(L);rp=ranks(P) if P else {}
              for r in rows:
                v=by.get(str(r['stock_id']),{});cur_v=v.get(L);prev_v=v.get(P) if P else None
                r['tdcc_holders']=int(cur_v) if cur_v is not None else None
                r['tdcc_prev']=int(prev_v) if prev_v is not None else None
                r['tdcc_change']=(int(cur_v)-int(prev_v)) if cur_v is not None and prev_v is not None else None
                r['tdcc_change_pct']=((cur_v/prev_v-1)*100) if cur_v is not None and prev_v else None
                r['tdcc_date']=str(L) if cur_v is not None else None
                r['pop_rank']=rl.get(str(r['stock_id']));r['pop_rank_prev']=rp.get(str(r['stock_id']))
              hold_info['covered']=len(rl)
    except Exception as exc:hold_info['error']=type(exc).__name__+': '+str(exc)[:120]
    for r in rows:
      for k in ('tdcc_holders','tdcc_prev','tdcc_change','tdcc_change_pct','tdcc_date','pop_rank','pop_rank_prev'):r.setdefault(k,None)
    rows.sort(key=lambda r:(r['tdcc_holders'] is None,-(r['tdcc_holders'] or 0),r['official_beneficiaries'] is None,-(r['official_beneficiaries'] or 0),r['avg20_turnover'] is None,-(r['avg20_turnover'] or 0),r['stock_id']))
    groups={}
    for r in rows:groups.setdefault(r['group'],[]).append({'code':r['stock_id'],'name':r['stock_name']})
    return {"ok":True,"rows":rows,"groups":groups,
            "catalog_error":catalog_error,"catalog_audit":catalog_audit,"selection_basis":"官方 ISIN 上市＋上櫃 ETF 名錄（含尚無日 K 者）；名錄失敗會明示並退回資料庫候選，可依官方受益人次及本站近20筆成交金額查人氣。族群僅依 ETF 名稱歸類，不代表實際持股產業或績效推薦。",
            "popularity_error":pop_error,"holders_info":hold_info,
            "manager_win_rate_definition":"建倉後10／20／60交易日獲利筆數÷可觀測建倉筆數；須有基金經理人實際建倉、持股異動、適用股利與完整持有期。現有資料無法計算，顯示尚無資料。",
            "note":f"價格趨勢與交易金額：PostgreSQL；官方排行：證交所 ETF e添富（頁面日期逐筆顯示）；配息：{div_source}。20／60 日收盤價報酬未計配息；未公告配息不推估。"}

PROFILE_JOB={'running':False,'rows_updated':0,'message':'尚未更新官方公司資料','updated_at':None}
PROFILE_LOCK=threading.Lock()

def company_source_links(code):
    code=urllib.parse.quote(str(code),safe='')
    return [{'label':'產業價值鏈資訊平台：公司所屬產業鏈','url':'https://ic.tpex.org.tw/company_chain.php?stk_code='+code},
      *[{'label':'Goodinfo：'+label,'url':'https://goodinfo.tw/tw/'+page+'?STOCK_ID='+code} for label,page in
        [('基本資料／主要業務／重要子公司','BasicInfo.asp'),('月營收','ShowSaleMonChart.asp'),('財務報表','StockFinDetail.asp'),('法人買賣','ShowBuySaleChart.asp'),('股東持股分級','EquityDistributionClassHis.asp'),('股利政策','StockDividendPolicy.asp')]],
      {'label':'玩股網：流動／速動比率（外部核對）','url':'https://www.wantgoo.com/stock/'+code+'/financial-security/current-quick-ratio'},
      {'label':'MoneyDJ：財務比率累計季表（外部核對）','url':'https://concords.moneydj.com/z/zc/zcr/zcr0.djhtm?a='+code+'&b=A'},
      {'label':'財報狗：公司分析（外部核對）','url':'https://statementdog.com/analysis/'+code},
      {'label':'公開資訊觀測站：公司公告／財報／年報','url':'https://mops.twse.com.tw/mops/#/web/home'}]

def db_company_profiles_all():
    if not USE_POSTGRES or not _pg_table_columns('company_research_profiles'):return {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT stock_id,payload FROM public.company_research_profiles')
            return {str(code):(json.loads(value) if isinstance(value,str) else value) for code,value in cur.fetchall()}

def db_company_research(code):
    if not USE_POSTGRES or not _pg_table_columns('company_research_profiles'):return {}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT payload FROM public.company_research_profiles WHERE stock_id=%s',(str(code),))
            row=cur.fetchone()
    return (json.loads(row[0]) if isinstance(row[0],str) else row[0]) if row else {}

def db_company_revenue(code):
    if not USE_POSTGRES:return []
    schema=revenue_schema();period=revenue_date_sql(dict(schema,date=None)) if schema.get('year') and schema.get('month') else revenue_date_sql(schema)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT '+period+',"'+schema['revenue']+'" FROM public.monthly_revenue WHERE "'+schema['code']+'"=%s ORDER BY 1 DESC NULLS LAST LIMIT 24',(str(code),))
            rows=cur.fetchall()
    return [{'period':str(day),'revenue':num(value)} for day,value in rows if day is not None]

def _company_research_worker():
    PROFILE_JOB.update(running=True,rows_updated=0,message='讀取官方上市、上櫃公司基本資料…')
    errors=[]
    try:
        with DB_LOCK,db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS public.company_research_profiles (stock_id text PRIMARY KEY,payload jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())')
        for market,url in [('上市','https://openapi.twse.com.tw/v1/opendata/t187ap03_L'),('上櫃','https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O')]:
            try:
                raw=fetch_json(url);rows=market_feeds.normalize_profiles(raw,market,url)
                if not rows:raise ValueError('沒有可驗證公司代號或出表日期')
                with DB_LOCK,db_conn() as conn:
                    with conn.cursor() as cur:
                        psycopg2.extras.execute_values(cur,'INSERT INTO public.company_research_profiles(stock_id,payload) VALUES %s ON CONFLICT(stock_id) DO UPDATE SET payload=EXCLUDED.payload,updated_at=now()',[(r['code'],json.dumps(r,ensure_ascii=False)) for r in rows],template='(%s,%s::jsonb)',page_size=500)
                PROFILE_JOB['rows_updated']+=len(rows)
            except Exception as e:errors.append(market+'：'+str(e).split('\n')[0][:250])
        PROFILE_JOB['message']='已更新官方公司基本資料；產品、供應鏈與子公司詳情請核對來源入口' if not errors else '部分更新：'+'；'.join(errors)
    except Exception as e:PROFILE_JOB['message']='更新失敗：'+str(e).split('\n')[0][:300]
    finally:
        PROFILE_JOB.update(running=False,updated_at=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'))

def company_research_start():
    if not USE_POSTGRES:return {'ok':False,'error':'公司研究資料入庫需要 PostgreSQL'}
    with PROFILE_LOCK:
        if PROFILE_JOB['running']:return {'ok':True,**PROFILE_JOB}
        PROFILE_JOB['running']=True
        threading.Thread(target=_company_research_worker,daemon=True).start()
    return {'ok':True,**PROFILE_JOB}


def industry_catalog(query=''):
    """Official exchange classification is a category, never an inferred supply-chain edge."""
    base=db_universe('both')
    q=query.strip().lower()
    rows=[r for r in base if q and (q in str(r['code']).lower() or q in (r.get('name') or '').lower() or q in (r.get('industry') or '').lower())]
    industries=sorted({r.get('industry') for r in base if r.get('industry')})
    return {'ok':True,'categories':industries,'rows':rows[:100],
            'source':'資料庫股票清單的交易所產業分類','asof':db_meta_get('last_update','尚未記錄'),
            'chain_status':'此端點只有交易所產業分類；主題地圖另由 /api/industry-map 提供。'}

def industry_map_view(query='', theme=''):
    try: official=db_universe('both')
    except Exception: official=[]
    return industry_map.catalog(query, theme, official)

def company_profile(code):
    stock=next((r for r in db_universe('both') if r['code']==code),None)
    if stock is None:return {'ok':False,'error':'找不到股票代號'}
    hist=db_history(code,90)
    profile=db_company_research(code)
    finance=db_chip_fundamental(code) if USE_POSTGRES else {}
    if USE_POSTGRES and _pg_table_columns('official_financial_reports'):
        with db_conn() as c:
            with c.cursor() as cur:
                cur.execute('SELECT kind,report_date,payload FROM public.official_financial_reports WHERE stock_id=%s ORDER BY report_date',(code,))
                records=[{'kind':kind,'date':str(day),'payload':json.loads(payload) if isinstance(payload,str) else payload} for kind,day,payload in cur.fetchall()]
        if records:finance.update(health_engine.financial_facts(financial_feeds.merge_official({},records)))
    return {'ok':True,'stock':stock,'profile':profile,'external_sources':company_source_links(code),'price_history':hist,'price':({**hist[-1]} if hist else None),
            'revenue_history':db_company_revenue(code),'revenue_yoy':db_revenue_yoy_series(code,13)[-1:] if USE_POSTGRES else [],
            'finance':finance,
            'chain_links':industry_map.company_themes(code),
            'chain_status':'公開主題地圖標記來自 Timeverse/My-TW-Coverage；標記同一主題不代表兩家公司有直接買賣關係。',
            'source':'資料庫股票清單與歷史日 K'}

def warehouse_seed_universe():
    rows=[]
    try:rows+=twse()
    except:pass
    try:rows+=tpex()
    except:pass
    im={}
    try:im=industries()
    except:pass
    d={}
    for x in rows:
        if x.get("code"):
            x=dict(x);x["industry"]=im.get(x["code"],{}).get("industry","");d[x["code"]]=x
    rows=list(d.values());db_upsert_stocks(rows);return rows
def warehouse_update(max_requests=25):
    warehouse_seed_universe();g=finmind_guard_status()
    if g["cooling"]:return {"ok":False,"code":"FINMIND_COOLDOWN","guard":g,**db_status()}
    today=date.today()
    if USE_POSTGRES:
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT s.stock_id AS code,MAX(p.trade_date) last_date FROM public.stocks s LEFT JOIN public.daily_prices p ON p.stock_id=s.stock_id GROUP BY s.stock_id ORDER BY CASE WHEN MAX(p.trade_date) IS NULL THEN 0 ELSE 1 END,MAX(p.trade_date),s.stock_id");todo=cur.fetchall()
    else:
        with db_conn() as c:todo=c.execute("SELECT s.code,MAX(p.date) last_date FROM stocks s LEFT JOIN daily_prices p ON p.code=s.code GROUP BY s.code ORDER BY CASE WHEN MAX(p.date) IS NULL THEN 0 ELSE 1 END,MAX(p.date),s.code").fetchall()
    req=written=errors=0
    for r in todo:
        if req>=max_requests:break
        sd=(((r["last_date"] if hasattr(r["last_date"],"year") else datetime.strptime(r["last_date"],"%Y-%m-%d").date())+timedelta(days=1)) if r["last_date"] else (today-timedelta(days=550))).isoformat()
        if sd>today.isoformat():continue
        try:data=finmind("TaiwanStockPrice",r["code"],sd,today.isoformat(),cache_hours=0.01);req+=1;written+=db_upsert_history(r["code"],data)
        except FinMindCooldown:break
        except Exception:req+=1;errors+=1
    db_meta_set("last_update",datetime.now().isoformat(timespec="seconds"))
    return {"ok":not finmind_guard_status()["cooling"],"requests":req,"rows_updated":written,"errors":errors,"guard":finmind_guard_status(),**db_status()}

def warehouse_progress():
    try:
        st=db_status()
        total=st.get("stocks",0) or 0
        if USE_POSTGRES:
            with db_conn() as c:
                with c.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM public.stocks s WHERE EXISTS (SELECT 1 FROM public.daily_prices p WHERE p.stock_id=s.stock_id)")
                    covered=cur.fetchone()[0]
        else:
            with db_conn() as c: covered=c.execute("SELECT COUNT(DISTINCT code) FROM daily_prices").fetchone()[0]
        failures=0
        if USE_POSTGRES:
            with db_conn() as c:
                with c.cursor() as cur:cur.execute("SELECT COUNT(*) FROM public.warehouse_failures");failures=cur.fetchone()[0]
        return {**st,"covered_stocks":covered,"total_stocks":total,"missing_stocks":max(0,total-covered),"failed_stocks":failures,"pct":round((covered/total*100),1) if total else 0,**WAREHOUSE_JOB}
    except Exception as e:
        return {"running":WAREHOUSE_JOB.get("running",False),"error":str(e)[:180]}

def _warehouse_worker():
    WAREHOUSE_JOB.update({"running":True,"stop":False,"requests":0,"rows_updated":0,"errors":0,"started_at":datetime.now().isoformat(timespec="seconds"),"message":"建立歷史日K中…"})
    try:
        warehouse_seed_universe()
        while not WAREHOUSE_JOB["stop"]:
            g=finmind_guard_status()
            if g["cooling"]:
                WAREHOUSE_JOB["message"]="FinMind 冷卻中，資料已保存，可稍後續跑"; break
            if USE_POSTGRES:
                with db_conn() as c:
                    with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute("SELECT s.stock_id AS code,MAX(p.trade_date) last_date FROM public.stocks s LEFT JOIN public.daily_prices p ON p.stock_id=s.stock_id GROUP BY s.stock_id HAVING NOT EXISTS (SELECT 1 FROM public.warehouse_failures f WHERE f.stock_id=s.stock_id AND f.retry_after>now()) ORDER BY CASE WHEN MAX(p.trade_date) IS NULL THEN 0 ELSE 1 END,MAX(p.trade_date),s.stock_id LIMIT 1")
                        r=cur.fetchone()
            else:
                with db_conn() as c:r=c.execute("SELECT s.code,MAX(p.date) last_date FROM stocks s LEFT JOIN daily_prices p ON p.code=s.code GROUP BY s.code ORDER BY CASE WHEN MAX(p.date) IS NULL THEN 0 ELSE 1 END,MAX(p.date),s.code LIMIT 1").fetchone()
            if not r: WAREHOUSE_JOB["message"]="沒有待更新股票"; break
            today=date.today(); ld=r["last_date"]
            if ld:
                ld=ld if hasattr(ld,"year") else datetime.strptime(str(ld),"%Y-%m-%d").date()
                sd=(ld+timedelta(days=1)).isoformat()
            else: sd=(today-timedelta(days=550)).isoformat()
            if sd>today.isoformat():
                # This stock is current. Find the next stale/unseeded stock instead of looping forever.
                if USE_POSTGRES:
                    with db_conn() as c:
                        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                            cur.execute("SELECT s.stock_id AS code,MAX(p.trade_date) last_date FROM public.stocks s LEFT JOIN public.daily_prices p ON p.stock_id=s.stock_id GROUP BY s.stock_id HAVING MAX(p.trade_date) IS NULL OR MAX(p.trade_date)<%s ORDER BY CASE WHEN MAX(p.trade_date) IS NULL THEN 0 ELSE 1 END,MAX(p.trade_date),s.stock_id LIMIT 1",(today,)); r=cur.fetchone()
                else:
                    with db_conn() as c:r=c.execute("SELECT s.code,MAX(p.date) last_date FROM stocks s LEFT JOIN daily_prices p ON p.code=s.code GROUP BY s.code HAVING MAX(p.date) IS NULL OR MAX(p.date)<? ORDER BY CASE WHEN MAX(p.date) IS NULL THEN 0 ELSE 1 END,MAX(p.date),s.code LIMIT 1",(today.isoformat(),)).fetchone()
                if not r: WAREHOUSE_JOB["message"]="歷史日K已更新到最新可取得日期"; break
                ld=r["last_date"]; sd=((ld if hasattr(ld,"year") else datetime.strptime(str(ld),"%Y-%m-%d").date())+timedelta(days=1)).isoformat() if ld else (today-timedelta(days=550)).isoformat()
            code=str(r["code"]);WAREHOUSE_JOB["last_code"]=code
            try:
                data=finmind("TaiwanStockPrice",code,sd,today.isoformat(),cache_hours=0.01)
                WAREHOUSE_JOB["requests"]+=1
                if not data:
                    warehouse_mark_failure(code,'來源在本次日期區間尚無資料');WAREHOUSE_JOB['errors']+=1
                else:
                    WAREHOUSE_JOB["rows_updated"]+=db_upsert_history(code,data);warehouse_clear_failure(code)
            except FinMindCooldown:
                WAREHOUSE_JOB["message"]="FinMind 額度保護中，已保存進度";break
            except Exception as e:
                WAREHOUSE_JOB["requests"]+=1;WAREHOUSE_JOB["errors"]+=1;warehouse_mark_failure(code,e);WAREHOUSE_JOB["message"]="個股 "+code+" 更新失敗，已跳過並記錄，繼續下一檔"
            db_meta_set("last_update",datetime.now().isoformat(timespec="seconds"))
            time.sleep(7.5)  # 約 480 次/小時，保留額度給其他功能
    finally:
        WAREHOUSE_JOB["running"]=False

def warehouse_start():
    global WAREHOUSE_THREAD
    warehouse_seed_universe()
    if WAREHOUSE_JOB.get("running"):
        return {"ok":True,"already_running":True,**warehouse_progress()}
    WAREHOUSE_THREAD=threading.Thread(target=_warehouse_worker,daemon=True);WAREHOUSE_THREAD.start()
    return {"ok":True,"started":True,**warehouse_progress()}

def _revenue_worker():
    REVENUE_JOB.update(running=True,rows_updated=0,stocks_covered=0,latest_date=None,requests=0,failed=0,last_code='',pending=None,
                       message='逐檔建立月營收中…',started_at=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'),finished_at=None)
    try:
        if not USE_POSTGRES:raise RuntimeError('需要 PostgreSQL 才能批次續跑')
        # FinMind free-tier month revenue requires data_id. Never make the paid all-stock request.
        candidates,pending=db_revenue_pending_codes(120)
        REVENUE_JOB['pending']=pending
        if not candidates:
            REVENUE_JOB['message']='目前無待補個股；請核對各公司最新營收月份';return
        start=(datetime.now(ZoneInfo('Asia/Taipei')).date()-timedelta(days=900)).isoformat()
        for code in candidates:
            if finmind_guard_status()['cooling']:
                REVENUE_JOB['message']='FinMind 額度保護中；已入庫資料保留，稍後再按按鈕續跑';return
            REVENUE_JOB['last_code']=code
            try:
                records=finmind('TaiwanStockMonthRevenue',data_id=code,start_date=start,cache_hours=0.25)
                REVENUE_JOB['requests']+=1
                if not records:
                    REVENUE_JOB['failed']+=1;db_revenue_mark_failure(code,'來源未回傳月營收');continue
                written=db_upsert_monthly_revenue(records)
                if not written:
                    REVENUE_JOB['failed']+=1;db_revenue_mark_failure(code,'來源欄位缺漏');continue
                db_revenue_clear_failure(code)
                REVENUE_JOB['rows_updated']+=written;REVENUE_JOB['stocks_covered']+=1
                latest=max((str(x.get('date')) for x in records if x.get('date')),default=None)
                if latest and (not REVENUE_JOB['latest_date'] or latest>REVENUE_JOB['latest_date']):REVENUE_JOB['latest_date']=latest
            except FinMindCooldown:
                REVENUE_JOB['message']='FinMind 額度保護中；已入庫資料保留，稍後再按按鈕續跑';return
            except Exception as e:
                REVENUE_JOB['failed']+=1;db_revenue_mark_failure(code,str(e)[:150])
                if isinstance(e,ValueError) or (psycopg2 and isinstance(e,psycopg2.Error)):
                    REVENUE_JOB['message']='資料寫入失敗，已停止本批以避免重複錯誤：'+str(e)[:600];return
                if isinstance(e,urllib.error.HTTPError) and e.code in (400,401,403):
                    REVENUE_JOB['message']=f'{code} 的個股月營收請求被 FinMind 拒絕（HTTP {e.code}）；已停止，請檢查資料集與 Token 權限。';return
            finally:
                REVENUE_JOB['pending']=max(0,pending-REVENUE_JOB['stocks_covered'])
                time.sleep(7.5)
        REVENUE_JOB['message']=f'本批完成，已補 {REVENUE_JOB["stocks_covered"]} 檔；待補約 {REVENUE_JOB["pending"]} 檔。可再按一次續跑。'
        try:db_meta_set('monthly_revenue_last_update',datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'))
        except Exception:pass
    except Exception as e:
        REVENUE_JOB['message']='更新失敗：'+('FinMind 額度／頻率限制' if isinstance(e,FinMindCooldown) else str(e)[:600])
    finally:
        REVENUE_JOB['running']=False;REVENUE_JOB['finished_at']=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')

def db_revenue_pending_codes(limit=120):
    """Stocks lacking roughly two years of current monthly reports; retry failures tomorrow."""
    cutoff=datetime.now(ZoneInfo('Asia/Taipei')).date()-timedelta(days=95)
    schema=revenue_schema()
    with db_conn() as c:
        with c.cursor() as cur:
            cur.execute('''WITH covered AS (
                SELECT "'''+schema['code']+'''" AS stock_id,COUNT(*) n,MAX('''+revenue_date_sql(schema)+''') newest FROM public.monthly_revenue GROUP BY "'''+schema['code']+'''"
            ) SELECT s.stock_id FROM public.stocks s
              LEFT JOIN covered r ON r.stock_id=s.stock_id
              LEFT JOIN public.revenue_failures f ON f.stock_id=s.stock_id AND f.retry_after>now()
              WHERE f.stock_id IS NULL AND (r.n IS NULL OR r.n<24 OR r.newest<%s)
              ORDER BY COALESCE(r.n,0),r.newest NULLS FIRST,s.stock_id''',(cutoff,))
            codes=[str(r[0]) for r in cur.fetchall()]
    return codes[:limit],len(codes)

def db_revenue_mark_failure(code,reason):
    if not USE_POSTGRES:return
    with DB_LOCK,db_conn() as c:
        with c.cursor() as cur:
            cur.execute('''INSERT INTO public.revenue_failures(stock_id,error_text) VALUES(%s,%s)
                ON CONFLICT(stock_id) DO UPDATE SET error_text=EXCLUDED.error_text,
                last_failed_at=now(),retry_after=now()+interval '1 day' ''',(code,reason))

def db_revenue_clear_failure(code):
    if not USE_POSTGRES:return
    with DB_LOCK,db_conn() as c:
        with c.cursor() as cur:cur.execute('DELETE FROM public.revenue_failures WHERE stock_id=%s',(code,))

def revenue_start():
    global REVENUE_THREAD
    if REVENUE_JOB['running']:return {'ok':True,'already_running':True,**REVENUE_JOB}
    REVENUE_JOB['running']=True
    REVENUE_THREAD=threading.Thread(target=_revenue_worker,daemon=True);REVENUE_THREAD.start()
    return {'ok':True,'started':True,**REVENUE_JOB}

def selected_institutional_rows(code, records):
    grouped={}
    names={'Foreign_Investor':'foreign_net','Investment_Trust':'trust_net',
           'Dealer':'dealer_net','Dealer_self':'dealer_net','Dealer_Hedging':'dealer_net'}
    for r in records:
        day=str(r.get('date') or '')[:10];key=names.get(r.get('name'))
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',day) or not key:continue
        buy=num(r.get('buy'));sell=num(r.get('sell'))
        if buy is None or sell is None:continue
        row=grouped.setdefault(day,{'stock_id':code,'trade_date':day,'foreign_net':None,'trust_net':None,'dealer_net':None})
        row[key]=(row[key] or 0)+(buy-sell)
    return list(grouped.values())

def selected_financial_rows(code, records):
    grouped={}
    for r in records:
        day=str(r.get('date') or '')[:10]
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',day):continue
        row=grouped.setdefault(day,{'stock_id':code,'period_date':day,'eps':None,'gross_margin':None,'operating_margin':None})
        if r.get('type')=='EPS':row['eps']=num(r.get('value'))
    return [x for x in grouped.values() if x['eps'] is not None]

def selected_margin_rows(code, records):
    out=[]
    for r in records:
        day=str(r.get('date') or '')[:10]
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}',day):
            out.append({'stock_id':code,'trade_date':day,'margin_balance':num(r.get('MarginPurchaseTodayBalance')),
                        'short_balance':num(r.get('ShortSaleTodayBalance'))})
    return [x for x in out if x['margin_balance'] is not None or x['short_balance'] is not None]

def db_upsert_selected(table, rows):
    if not USE_POSTGRES or not rows:return 0
    names={'institutional_trading':('trade_date','foreign_net','trust_net','dealer_net'),
           'financials':('period_date','eps','gross_margin','operating_margin'),
           'margin_short':('trade_date','margin_balance','short_balance')}
    if table not in names:raise ValueError('資料表不支援')
    date_col,*fields=names[table]
    if table=='institutional_trading':
        with DB_LOCK,db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS public.research_institutional_daily (stock_id text NOT NULL,trade_date date NOT NULL,foreign_net numeric,trust_net numeric,dealer_net numeric,source_name text NOT NULL,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(stock_id,trade_date))')
        table='research_institutional_daily'
    cols=_pg_table_columns(table)
    actual_code=next((c for c in ('stock_id','stock_code','code') if c in cols),None)
    actual_date=next((c for c in (date_col,'date','period_date') if c in cols),None)
    if not actual_code or not actual_date or any(f not in cols for f in fields):raise ValueError(table+' 欄位不相容：'+','.join(sorted(cols)))
    columns=[actual_code,actual_date]+fields
    vals=[tuple([r.get('stock_id'),r.get(date_col)]+[r.get(k) for k in fields]) for r in rows]
    if 'source_name' in cols:
        columns.append('source_name');vals=[v+(r.get('source_name','FinMind'),) for v,r in zip(vals,rows)]
    match='t."'+actual_code+'"=v."'+actual_code+'" AND t."'+actual_date+'"=v."'+actual_date+'"'
    quoted=','.join('"'+c+'"' for c in columns)
    assignments=','.join('"'+k+'"=v."'+k+'"' for k in columns[2:])+(',updated_at=now()' if 'updated_at' in cols else '')
    casts=['text','date']+['numeric']*len(fields)+(['text'] if 'source_name' in cols else [])
    with DB_LOCK,db_conn() as c:
        with c.cursor() as cur:
            sql='WITH v('+quoted+') AS (VALUES %s), changed AS (UPDATE public."'+table+'" t SET '+assignments+' FROM v WHERE '+match+' RETURNING 1) INSERT INTO public."'+table+'" ('+quoted+') SELECT '+quoted+' FROM v WHERE NOT EXISTS(SELECT 1 FROM public."'+table+'" t WHERE '+match+') ON CONFLICT DO NOTHING'
            psycopg2.extras.execute_values(cur,sql,vals,template='('+','.join('%s::'+cast for cast in casts)+')',page_size=500)
    return len(vals)

def db_institutional_rankings():
    table='research_institutional_daily' if _pg_table_columns('research_institutional_daily') else 'institutional_trading'
    cols=_pg_table_columns(table)
    if not USE_POSTGRES or not cols:return {'ok':True,'trade_date':None,'coverage':0,'rows':[],'source':'尚無個股法人資料'}
    cc=next((k for k in ('stock_id','stock_code','code') if k in cols),None)
    dc=next((k for k in ('trade_date','date') if k in cols),None)
    if not cc or not dc or any(k not in cols for k in ('foreign_net','trust_net','dealer_net')):
        return {'ok':True,'trade_date':None,'coverage':0,'rows':[],'source':'既有法人表欄位不相容；請執行官方個股法人更新，寫入獨立的標準資料表。','schema_columns':sorted(cols)}
    with db_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SELECT MAX("'+dc+'") d FROM public."'+table+'"');day=cur.fetchone()['d']
            if day is None:return {'ok':True,'trade_date':None,'coverage':0,'rows':[],'source':'尚無個股法人資料'}
            cur.execute('SELECT i."'+cc+'" AS code,s.stock_name AS name,s.market,i.foreign_net,i.trust_net,i.dealer_net, (i.foreign_net+i.trust_net+i.dealer_net) AS total_net FROM public."'+table+'" i JOIN public.stocks s ON s.stock_id=i."'+cc+'" WHERE i."'+dc+'"=%s AND i.foreign_net IS NOT NULL AND i.trust_net IS NOT NULL AND i.dealer_net IS NOT NULL ORDER BY total_net DESC',(day,))
            rows=[dict(x) for x in cur.fetchall()]
    for r in rows:
        for k in ('foreign_net','trust_net','dealer_net','total_net'):r[k]=float(r[k])
    return {'ok':True,'trade_date':str(day),'coverage':len(rows),'rows':rows,
            'market_coverage':{m:sum(r['market']==m for r in rows) for m in ('上市','上櫃')},
            'source':'官方市場報表／FinMind 個股資料；外資不含外資自營商；單位為股，僅涵蓋已入庫個股'}


def _selected_worker(codes):
    SELECTED_JOB.update(running=True,stocks_done=0,stocks_total=len(codes),rows_updated=0,current_code='',message='分批補入本次選股個股資料…',started_at=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'),finished_at=None)
    today=datetime.now(ZoneInfo('Asia/Taipei')).date();recent=(today-timedelta(days=45)).isoformat()
    datasets=[('TaiwanStockPrice','price',recent),('TaiwanStockInstitutionalInvestorsBuySell','institutional_trading',recent),
              ('TaiwanStockMarginPurchaseShortSale','margin_short',recent),
              ('TaiwanStockFinancialStatements','financials',(today-timedelta(days=1600)).isoformat()),
              ('TaiwanStockBalanceSheet','health_raw',(today-timedelta(days=1600)).isoformat()),
              ('TaiwanStockCashFlowsStatement','health_raw',(today-timedelta(days=1600)).isoformat()),
              ('TaiwanStockPER','health_raw',recent),
              ('TaiwanStockMonthRevenue','monthly_revenue',(today-timedelta(days=800)).isoformat())]
    errors=[]
    try:
        for code in codes:
            SELECTED_JOB['current_code']=code
            for dataset,table,start in datasets:
                if finmind_guard_status()['cooling']:
                    SELECTED_JOB['message']='FinMind 額度保護中；已入庫部分保留，可稍後續跑';return
                try:
                    records=finmind(dataset,code,start,today.isoformat(),cache_hours=0.25)
                    if table=='price':count=db_upsert_history(code,records) if records else 0
                    elif table=='monthly_revenue':count=db_upsert_monthly_revenue(records)
                    elif table=='institutional_trading':count=db_upsert_selected(table,selected_institutional_rows(code,records))
                    elif table=='financials':
                        count=db_health_raw_upsert(code,dataset,records)
                        try:count+=db_upsert_selected(table,selected_financial_rows(code,records))
                        except Exception as e:errors.append(code+' 舊財報表：'+type(e).__name__)
                    elif table=='health_raw':count=db_health_raw_upsert(code,dataset,records)
                    else:count=db_upsert_selected(table,selected_margin_rows(code,records))
                    SELECTED_JOB['rows_updated']+=count
                    if not records:errors.append(code+' '+table+' 尚無來源資料')
                except FinMindCooldown:
                    SELECTED_JOB['message']='FinMind 額度保護中；已入庫部分保留，可稍後續跑';return
                except Exception as e:errors.append(code+' '+table+'：'+str(e)[:70])
                time.sleep(7.5)
            SELECTED_JOB['stocks_done']+=1
        SELECTED_JOB['message']='完成；請核對個股最新資料日期' if not errors else '完成；部分缺漏：'+'；'.join(errors[:3])
    finally:
        SELECTED_JOB['running']=False;SELECTED_JOB['finished_at']=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')

def selected_start(codes):
    global SELECTED_THREAD
    if not USE_POSTGRES:return {'ok':False,'error':'請先連線 PostgreSQL'}
    if SELECTED_JOB['running']:return {'ok':True,'already_running':True,**SELECTED_JOB}
    if not isinstance(codes,list) or not codes or len(codes)>20 or any(not isinstance(x,str) or not re.fullmatch(r'\d{4,6}',x) for x in codes):
        return {'ok':False,'error':'一次請選 1～20 檔有效股票代號'}
    codes=list(dict.fromkeys(codes))
    allowed={str(x['code']) for x in db_universe('both')}
    if not set(codes)<=allowed:return {'ok':False,'error':'包含不在上市櫃資料庫中的代號'}
    SELECTED_JOB['running']=True
    SELECTED_THREAD=threading.Thread(target=_selected_worker,args=(codes,),daemon=True);SELECTED_THREAD.start()
    return {'ok':True,'started':True,**SELECTED_JOB}

CHIP_DATASETS={
    'institutional':'TaiwanStockTotalInstitutionalInvestors',
    'margin':'TaiwanStockTotalMarginPurchaseShortSale'
}

def db_upsert_market_chips(dataset, records):
    if dataset not in CHIP_DATASETS:raise ValueError('不支援的籌碼資料')
    vals=[];stamp=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')
    for r in records:
        day=str(r.get('date') or '')[:10];category=str(r.get('name') or '')
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',day) or not category:continue
        vals.append((dataset,day,category,num(r.get('buy')),num(r.get('sell')),
                     num(r.get('TodayBalance')),num(r.get('YesBalance')),'FinMind',stamp))
    if not vals:return 0
    with DB_LOCK,db_conn() as c:
        if USE_POSTGRES:
            with c.cursor() as cur:
                psycopg2.extras.execute_values(cur,"""INSERT INTO public.market_chips
                    (dataset,trade_date,category,buy,sell,today_balance,yesterday_balance,source_name,updated_at) VALUES %s
                    ON CONFLICT(dataset,trade_date,category) DO UPDATE SET buy=EXCLUDED.buy,sell=EXCLUDED.sell,
                    today_balance=EXCLUDED.today_balance,yesterday_balance=EXCLUDED.yesterday_balance,
                    source_name=EXCLUDED.source_name,updated_at=EXCLUDED.updated_at""",vals,page_size=500)
        else:c.executemany('''INSERT INTO market_chips(dataset,trade_date,category,buy,sell,today_balance,yesterday_balance,source_name,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(dataset,trade_date,category) DO UPDATE SET
            buy=excluded.buy,sell=excluded.sell,today_balance=excluded.today_balance,
            yesterday_balance=excluded.yesterday_balance,updated_at=excluded.updated_at''',vals)
    return len(vals)

def db_market_chips():
    if USE_POSTGRES:
        with db_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute('''SELECT dataset,trade_date,category,buy,sell,today_balance,yesterday_balance,
                    source_name,updated_at FROM public.market_chips
                    WHERE trade_date >= CURRENT_DATE - INTERVAL '100 days' ORDER BY trade_date DESC,category''')
                rows=[dict(r) for r in cur.fetchall()]
    else:
        with db_conn() as c:rows=[dict(r) for r in c.execute('''SELECT dataset,trade_date,category,buy,sell,today_balance,
            yesterday_balance,source_name,updated_at FROM market_chips WHERE trade_date >= date('now','-100 days')
            ORDER BY trade_date DESC,category''')]
    for r in rows:
        for k in ('trade_date','updated_at'):r[k]=str(r[k]) if r[k] is not None else None
        for k in ('buy','sell','today_balance','yesterday_balance'):r[k]=float(r[k]) if r[k] is not None else None
        r['net']=(r['buy']-r['sell']) if r['buy'] is not None and r['sell'] is not None else None
    latest={d:max((r['trade_date'] for r in rows if r['dataset']==d),default=None) for d in CHIP_DATASETS}
    return {'ok':True,'rows':rows,'latest':latest,'job':dict(CHIP_JOB),
            'source':'FinMind 全市場盤後資料，僅讀已入庫資料；數字單位依原資料定義'}

def _chip_worker():
    CHIP_JOB.update(running=True,rows_updated=0,message='更新市場法人與融資融券中…',started_at=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'),finished_at=None)
    errors=[];start=(datetime.now(ZoneInfo('Asia/Taipei')).date()-timedelta(days=100)).isoformat()
    try:
        # Official whole-market stock reports are independent of FinMind quota.
        today=datetime.now(ZoneInfo('Asia/Taipei')).date()
        for market in ('上市','上櫃'):
            try:
                found=False
                for back in range(5):
                    day=today-timedelta(days=back)
                    if day.weekday()>4:continue
                    if market=='上市':url='https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL&date='+day.strftime('%Y%m%d')
                    else:url='https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade?type=Daily&response=json&date='+day.strftime('%Y/%m/%d')
                    records=market_feeds.normalize_institutional(market_feeds.fetch(url),market)
                    if records:
                        CHIP_JOB['rows_updated']+=db_upsert_selected('institutional_trading',records)
                        found=True;break
                if not found:errors.append(market+'近五日尚無官方個股法人報表')
            except Exception as e:errors.append(market+'個股法人：'+str(e)[:100])
        for key,dataset in CHIP_DATASETS.items():
            try:
                records=finmind(dataset,start_date=start,cache_hours=0.25)
                if not records:raise RuntimeError('來源未回傳資料')
                CHIP_JOB['rows_updated']+=db_upsert_market_chips(key,records)
            except Exception as e:errors.append(key+'：'+('FinMind 額度限制' if isinstance(e,FinMindCooldown) else str(e)[:90]))
        CHIP_JOB['message']='已入庫；請核對交易日期與涵蓋天數' if not errors else '；'.join(errors)
    except Exception as e:
        CHIP_JOB['message']='更新失敗：'+str(e)[:100]
    finally:
        CHIP_JOB['running']=False;CHIP_JOB['finished_at']=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')

def chip_start():
    global CHIP_THREAD
    if CHIP_JOB['running']:return {'ok':True,'already_running':True,**CHIP_JOB}
    CHIP_JOB['running']=True
    CHIP_THREAD=threading.Thread(target=_chip_worker,daemon=True);CHIP_THREAD.start()
    return {'ok':True,'started':True,**CHIP_JOB}

def save_intraday_snapshot(payload):
    key=payload['trade_date']+' '+payload['quote_to']
    with DB_LOCK,db_conn() as c:
        if USE_POSTGRES:
            with c.cursor() as cur:
                cur.execute('CREATE TABLE IF NOT EXISTS public.intraday_flow_snapshots(snapshot_key text PRIMARY KEY,trade_date date NOT NULL,payload jsonb NOT NULL,captured_at timestamptz NOT NULL DEFAULT now())')
                cur.execute('INSERT INTO public.intraday_flow_snapshots(snapshot_key,trade_date,payload) VALUES(%s,%s,%s::jsonb) ON CONFLICT(snapshot_key) DO UPDATE SET payload=EXCLUDED.payload,captured_at=now()', (key,payload['trade_date'],json.dumps(payload,ensure_ascii=False)))
        else:
            c.execute('CREATE TABLE IF NOT EXISTS intraday_flow_snapshots(snapshot_key TEXT PRIMARY KEY,trade_date TEXT,payload TEXT)')
            c.execute('INSERT OR REPLACE INTO intraday_flow_snapshots VALUES(?,?,?)',(key,payload['trade_date'],json.dumps(payload,ensure_ascii=False)))

def intraday_history():
    with db_conn() as c:
        if USE_POSTGRES:
            with c.cursor() as cur:
                cur.execute("SELECT to_regclass('public.intraday_flow_snapshots')")
                if not cur.fetchone()[0]:return []
                cur.execute('SELECT payload FROM public.intraday_flow_snapshots ORDER BY trade_date DESC,snapshot_key DESC LIMIT 90')
                return [r[0] for r in cur.fetchall()]
        exists=c.execute("SELECT 1 FROM sqlite_master WHERE name='intraday_flow_snapshots'").fetchone()
        return [json.loads(r[0]) for r in c.execute('SELECT payload FROM intraday_flow_snapshots ORDER BY snapshot_key DESC LIMIT 90')] if exists else []

def intraday_data():
    history=intraday_history()
    if not history:return {'ok':True,'rows':[],'job':dict(INTRADAY_JOB),'message':INTRADAY_JOB['message']+'；按更新建立第一份快照。'}
    out=json.loads(json.dumps(history[0]));same=[p for p in reversed(history) if p['trade_date']==out['trade_date']]
    comparable_scope=market_feeds.comparable_snapshots(same)
    out.pop('quotes',None)
    if comparable_scope:
        out['comparable_coverage_pct']=round(comparable_scope['cohort_count']/comparable_scope['cohort_total']*100,1)
        out['comparable_count']=comparable_scope['cohort_count']
    for row in out['rows']:
        if comparable_scope:
            row['timeline']=comparable_scope['timeline'].get(row['industry'],[])
        else:
            comparable=[p for p in same if p.get('coverage_codes')==out.get('coverage_codes')]
            row['timeline']=[{'time':p['quote_to'][:5],'share_pct':g['share_pct']} for p in comparable for g in p['rows'] if g['industry']==row['industry']]
        points=row['timeline'][-3:]
        row['signal']='需至少三份相同股票範圍的快照'
        if len(points)==3:
            first=points[1]['share_pct']-points[0]['share_pct'];second=points[2]['share_pct']-points[1]['share_pct']
            row['signal']='🔥 估算占比連續增加、增幅擴大' if 0<first<second else '占比增加' if second>0 else '占比下降' if second<0 else '占比持平'
    out['job']=dict(INTRADAY_JOB)
    out['message']=f"{out['trade_date']} {out['quote_from']}～{out['quote_to']}｜延遲快照｜成交資金估算｜涵蓋 {out['coverage']}/{out['market_total']} 檔；"+(f"時間比較固定共同 {out['comparable_count']} 檔為分母。" if comparable_scope else '時間比較需至少三份可比較快照。')+('｜'+INTRADAY_JOB['message'] if INTRADAY_JOB['running'] else '')
    return out

_MIS={"opener":None,"at":0.0}
_MIS_UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
def _mis_opener(force=False):
    """TWSE MIS 需要先拿到網站的 session cookie，且會擋沒有瀏覽器標頭的請求。"""
    import http.cookiejar
    if force or _MIS["opener"] is None or time.time()-_MIS["at"]>900:
        op=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        op.addheaders=[("User-Agent",_MIS_UA),("Accept-Language","zh-TW,zh;q=0.9"),("Accept","*/*")]
        for warm in ("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw","https://mis.twse.com.tw/stock/index.jsp"):
            try:op.open(warm,timeout=15).read(2000);break
            except Exception:pass   # 暖機頁失敗不影響後面直接呼叫報價 API
        _MIS["opener"]=op;_MIS["at"]=time.time()
    return _MIS["opener"]
def _mis_fetch(ids):
    url="https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&_="+str(int(time.time()*1000))+"&ex_ch="+urllib.parse.quote(ids)
    last=None
    for attempt in range(3):
        try:
            op=_mis_opener(force=attempt>0)
            req=urllib.request.Request(url,headers={"Referer":"https://mis.twse.com.tw/stock/index.jsp","X-Requested-With":"XMLHttpRequest"})
            return json.loads(op.open(req,timeout=15).read().decode("utf-8","replace"))
        except Exception as e:
            last=e;time.sleep(1.2*(attempt+1))
    raise last

def _intraday_snapshot_bars(by_code,max_age_min=20):
    """Render 連不上證交所時：讀 GitHub Actions 存進資料庫的即時報價快照（太舊就不用）。"""
    if not USE_POSTGRES:return {},{}
    with db_conn() as c:
        with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT stock_id,trade_date,open,high,low,close,volume_shares,quote_time,est,EXTRACT(EPOCH FROM (now()-updated_at))/60 AS age FROM public.intraday_quotes WHERE stock_id<>'TAIEX'")
            rows=cur.fetchall()
    if not rows:return {},{}
    age=min(float(r['age']) for r in rows)
    if age>max_age_min:raise ValueError('快照已 %d 分鐘沒更新'%int(age))
    bars={};est_n=0
    for r in rows:
        code=str(r['stock_id'])
        if code not in by_code or float(r['age'])>max_age_min:continue
        shares=float(r['volume_shares'] or 0);cl=float(r['close'])
        bars[code]={'date':str(r['trade_date']),'open':float(r['open']),'high':float(r['high']),'low':float(r['low']),'close':cl,'volume':shares,'turnover':cl*shares}
        if r.get('est'):est_n+=1
    return bars,{'time':max(str(r['quote_time'] or '') for r in rows),'age_min':int(age),'est':est_n}

def intraday_scan_bars(stocks):
    """選股用：TWSE MIS 即時報價 → {代號: 今日暫時日K}（開高低收、累計量）。
    證交所的成交價 z 只在該檔剛成交時才有值；沒有時用最佳買進價估算（價差>1.5%不估），並計數揭露。
    本機抓不到、或覆蓋率太低時，改讀 GitHub Actions 存的快照。"""
    from concurrent.futures import ThreadPoolExecutor
    by_code={str(s['code']):s for s in stocks if s.get('market') in ('上市','上櫃')}
    codes=list(by_code);bars={};errors=[];latest_t='';est_n=0
    def fnum(rec,k):
        try:return float(str(rec.get(k)).replace(',',''))
        except Exception:return None
    chunks=[codes[i:i+50] for i in range(0,len(codes),50)]
    deadline=time.time()+150
    def one(chunk):
        if time.time()>deadline:return None,'逾時略過'
        ids='|'.join(('tse_' if by_code[c]['market']=='上市' else 'otc_')+c+'.tw' for c in chunk)
        try:
            payload=_mis_fetch(ids)
            if payload.get('rtcode')!='0000':raise ValueError(payload.get('rtmessage','行情來源回傳錯誤'))
            return payload,''
        except Exception as e:return None,str(e)[:80]
    with ThreadPoolExecutor(3) as ex:results=list(ex.map(one,chunks))
    for payload,err in results:
        if payload is None:
            if err:errors.append(err)
            continue
        for rec in payload.get('msgArray',[]):
            code=str(rec.get('c'));d=str(rec.get('d') or '')
            z,o,h,l,v=fnum(rec,'z'),fnum(rec,'o'),fnum(rec,'h'),fnum(rec,'l'),fnum(rec,'v')
            if code not in by_code or len(d)!=8 or None in (o,h,l):continue
            est=False
            if z is None:
                try:a1=float(str(rec.get('a','')).split('_')[0]);b1=float(str(rec.get('b','')).split('_')[0])
                except Exception:a1=b1=0.0
                mid=(a1+b1)/2 if a1>0 and b1>0 and a1>=b1 else None
                if mid is None or (a1-b1)/mid>0.015:continue
                z=b1;est=True
            shares=(v or 0)*1000
            bars[code]={'date':f'{d[:4]}-{d[4:6]}-{d[6:]}','open':o,'high':h,'low':l,'close':z,'volume':shares,'turnover':z*shares}
            if est:est_n+=1
            latest_t=max(latest_t,str(rec.get('t') or ''))
    label='TWSE 即時報價（%d 檔有報價，其中 %d 檔無成交價、以最佳買進價估算；其餘股票仍以最近收盤資料計算）'%(len(bars),est_n)
    if len(bars)<len(codes)*0.5:
        try:
            sbars,snap=_intraday_snapshot_bars(by_code)
            if len(sbars)>len(bars):
                bars=sbars;latest_t=snap['time']
                label='Actions 即時快照（%s 分鐘前，共 %d 檔有報價，其中 %d 檔無成交價、以最佳買進價估算；其餘股票仍以最近收盤資料計算）'%(snap['age_min'],len(bars),snap['est'])
        except Exception as e:errors.append('快照:'+str(e)[:60])
    if bars:
        day=max(b['date'] for b in bars.values());bars={c:b for c,b in bars.items() if b['date']==day}
    else:day=None
    return bars,{'date':day,'time':latest_t,'fetched':len(bars),'errors':errors,'label':label,'source':'TWSE MIS 即時行情（成交價或最佳買進價×累計量，量與金額為估算）'}

def _intraday_worker():
    try:
        base=db_universe('both');by_code={str(s['code']):s for s in base};quotes=[];errors=[];est_n=[0]
        INTRADAY_JOB['total']=len(base);INTRADAY_JOB['done']=0
        for pos in range(0,len(base),50):
            batch=base[pos:pos+50]
            ids='|'.join(('tse_' if s['market']=='上市' else 'otc_')+str(s['code'])+'.tw' for s in batch if s['market'] in ('上市','上櫃'))
            try:
                payload=_mis_fetch(ids)
                if payload.get('rtcode')!='0000':raise ValueError(payload.get('rtmessage','行情來源回傳錯誤'))
                for record in payload.get('msgArray',[]):
                    stock=by_code.get(str(record.get('c')))
                    if stock:
                        if str(record.get('z') or '-').strip() in ('-',''):
                            # 沒有成交價：用最佳買進價估算（價差>1.5%不估），並計數
                            try:a1=float(str(record.get('a','')).split('_')[0]);b1=float(str(record.get('b','')).split('_')[0])
                            except Exception:a1=b1=0.0
                            mid=(a1+b1)/2 if a1>0 and b1>0 and a1>=b1 else None
                            if mid is None or (a1-b1)/mid>0.015:continue
                            record=dict(record,z='%.4f'%b1);est_n[0]+=1
                        quote=market_feeds.normalize_quote(record,stock)
                        if quote:quotes.append(quote)
            except Exception as e:
                errors.append(str(e)[:90])
                if len(errors)>=3:break
            INTRADAY_JOB['done']=min(pos+50,len(base));INTRADAY_JOB['message']=f'擷取行情 {INTRADAY_JOB["done"]}/{len(base)} 檔'
            time.sleep(1)
        if not quotes:raise ValueError('未取得可驗證時間與成交價量的行情：'+'；'.join(errors))
        day=max(q['date'] for q in quotes);quotes=[q for q in quotes if q['date']==day]
        out={'ok':True,'trade_date':day,'quote_from':min(q['quote_time'] for q in quotes),'quote_to':max(q['quote_time'] for q in quotes),
             'coverage':len(quotes),'market_total':len(base),'rows':market_feeds.group_quotes(quotes),'source':'TWSE MIS',
             'quotes':[{'code':q['code'],'industry':q['industry'],'estimated_amount':q['estimated_amount']} for q in quotes],
             'coverage_codes':sorted(q['code'] for q in quotes),
             'definition':'成交資金估算＝最新成交價 × 當日累計成交量（張）×1000；不是交易所累計實際成交金額。產業平均漲跌採個股算術平均。本次 %d 檔無成交價，改用買賣一檔中間價估算。'%est_n[0],
             'retrieved_at':datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'),'errors':errors}
        save_intraday_snapshot(out);INTRADAY_JOB['message']='快照已入庫'+('；部分請求失敗' if errors else '')
    except Exception as e:INTRADAY_JOB['message']='盤中快照更新失敗：'+str(e)[:250]
    finally:INTRADAY_JOB['running']=False

def intraday_start():
    with INTRADAY_LOCK:
        if INTRADAY_JOB['running']:return {'ok':True,'already_running':True,'job':dict(INTRADAY_JOB)}
        if time.time()-INTRADAY_JOB['last_started']<300:return {'ok':False,'error':'快照每五分鐘最多擷取一次，請先查看已保存的快照。'}
        INTRADAY_JOB.update(running=True,last_started=time.time(),message='開始擷取公開盤中行情…')
        threading.Thread(target=_intraday_worker,daemon=True).start()
        return {'ok':True,'started':True,'job':dict(INTRADAY_JOB)}

US_TW_STATE={'running':False,'started_at':None,'finished_at':None,'error':None,'result':None,'attempt_ts':0}

def _us_tw_worker():
    import sys
    US_TW_STATE.update(running=True,error=None,started_at=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'),finished_at=None)
    try:
        r=us_tw_link.run_job(sys.modules[__name__],None)
        US_TW_STATE['result']={k:r.get(k) for k in ('data_date','usable_pairs','total_pairs','partial','reason')}
        US_TW_STATE['result']['errors']=(r.get('errors') or [])[:6]
    except Exception as e:
        US_TW_STATE['error']=type(e).__name__+'：'+str(e)[:200]
    finally:
        US_TW_STATE.update(running=False,finished_at=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds'))

def us_tw_refresh_start(min_gap=1800):
    """背景重算美股→台股連動統計；同時只跑一個，最短間隔 min_gap 秒（避免一直打 Yahoo）。"""
    if US_TW_STATE['running'] or not USE_POSTGRES:return False
    if time.time()-US_TW_STATE['attempt_ts']<min_gap:return False
    US_TW_STATE['attempt_ts']=time.time()
    threading.Thread(target=_us_tw_worker,daemon=True).start()
    return True

SCAN_JOBS={};SCAN_LOCK=threading.Lock()
class _ScanProxy:
    """讓選股核心在背景執行緒裡也能用 self.sendj：只把結果存起來，不寫網路回應。"""
    def __init__(self):self.out=None;self.cancelled=False
    def sendj(self,o,status=200):self.out=(status,o)

def scan_job_start(handler_cls,q):
    """選股改成背景執行：瀏覽器每 2 秒查一次進度，避免 Render 對長請求（約 100 秒）直接切斷。"""
    import uuid
    key=json.dumps(q,sort_keys=True,ensure_ascii=False)
    with SCAN_LOCK:
        for jid,j in SCAN_JOBS.items():
            if j['state']=='running' and time.time()-j['t0']<900:
                if j['key']==key:return {'ok':True,'id':jid,'already_running':True}
                # A different scan was requested: stop the old one (it quits at the next stock) and start the new one.
                j['proxy'].cancelled=True;j['state']='cancelled'
        proxy=_ScanProxy()
        jid=uuid.uuid4().hex[:12];SCAN_JOBS[jid]={'state':'running','t0':time.time(),'key':key,'proxy':proxy}
        for old in sorted(SCAN_JOBS,key=lambda x:SCAN_JOBS[x]['t0'])[:-5]:SCAN_JOBS.pop(old,None)
    def work():
        p=proxy
        try:handler_cls._scan_core(p,q)
        except Exception as e:p.out=(500,{'ok':False,'error':'選股執行失敗：'+type(e).__name__})
        with SCAN_LOCK:
            if jid in SCAN_JOBS and not p.cancelled:SCAN_JOBS[jid].update(state='done',out=p.out,t1=time.time())
    threading.Thread(target=work,daemon=True).start()
    return {'ok':True,'id':jid}

def scan_job_result(jid):
    with SCAN_LOCK:
        j=SCAN_JOBS.get(jid)
        if not j:return {'ok':False,'error':'找不到這次選股（伺服器可能剛重新啟動），請重新按執行。'}
        if j['state']=='cancelled':return {'ok':False,'error':'這次選股已被新的選股取代。'}
        if j['state']=='running':return {'ok':True,'state':'running','elapsed':round(time.time()-j['t0'])}
        status,payload=j['out'] or (500,{'ok':False,'error':'選股沒有回傳結果'})
        return {'ok':True,'state':'done','status':status,'result':payload,'elapsed':round(j['t1']-j['t0'])}

def _revenue_coverage(cache):
    """How many stocks have usable YoY for each of the latest 4 months (newest last)."""
    try:
        n=len(cache);out=[]
        for back in range(4,0,-1):
            out.append(sum(1 for v in cache.values() if v and len(v)>=back and v[-back] is not None))
        return {"stocks":n,"latest4_nonnull":out}
    except Exception:return None

# ───────── LINE 機器人（邏輯在 line_bot.py）：聊天室輸入「飆成精」→ 回覆個股分數前三名 ─────────
def line_verify_signature(body,signature):
    secret=os.environ.get("LINE_CHANNEL_SECRET","").strip()
    if not secret or not signature:return False
    mac=base64.b64encode(hmac.new(secret.encode(),body,hashlib.sha256).digest()).decode()
    return hmac.compare_digest(mac,signature)

def line_handle_events(payload):
    import sys,line_bot
    line_bot.handle_events(sys.modules[__name__],payload)


class Handler(SimpleHTTPRequestHandler):
    def translate_path(self,path):
        p=urllib.parse.unquote(urllib.parse.urlparse(path).path).lstrip('/') or 'index.html'
        resolved=(ROOT/p).resolve()
        return str(resolved) if resolved.is_relative_to(ROOT.resolve()) else str(ROOT/'not-found')
    def sendj(self,o,status=200):
        b=json.dumps(o,ensure_ascii=False).encode();self.send_response(status);self.send_header("Content-Type","application/json; charset=utf-8");self.send_header("Content-Length",str(len(b)));self.end_headers();self.wfile.write(b)
    def do_GET(self):
        if self.path.startswith('/api/line-status'):
            sec=os.environ.get('LINE_CHANNEL_SECRET','');tok=os.environ.get('LINE_CHANNEL_TOKEN','')
            try:
                import line_bot;mod=True
            except Exception as e:mod=False
            return self.sendj({'ok':True,'line_bot_module':mod,'secret_set':bool(sec.strip()),'secret_length':len(sec.strip()),'secret_has_space_or_quote':bool(sec!=sec.strip() or '"' in sec or "'" in sec),'token_set':bool(tok.strip()),'token_length':len(tok.strip()),'recent':(line_bot.LOG[-25:] if mod else None),'live_running':(dict(line_bot._LIVE,lock=None,targets=len(line_bot._LIVE['targets'])) if mod else None)})
        if self.path.startswith('/api/intraday-test'):
            qs=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            codes=[x for x in (qs.get('codes',['2330,2436'])[0]).split(',') if x.strip()][:20]
            try:
                st=[{'code':c.strip(),'market':'上市'} for c in codes]
                bars,info=intraday_scan_bars(st)
                return self.sendj({'ok':True,'info':info,'bars':bars})
            except Exception as e:return self.sendj({'ok':False,'error':type(e).__name__+': '+str(e)[:200]})
        if self.path.startswith('/api/scan-result'):
            qs=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query);return self.sendj(scan_job_result(qs.get('id',[''])[0]))
        if self.path.startswith('/api/version'):return self.sendj({'ok':True,'version':'V2.0','build':'2026-10-02-industry-50-v1'})
        if self.path.startswith('/api/financial-status'):return self.sendj({'ok':True,**FINANCIAL_JOB})
        if self.path.startswith('/api/financial-history-status'):return self.sendj({'ok':True,**FINANCIAL_HISTORY_JOB})
        if self.path.startswith('/api/taiex-status'):return self.sendj({'ok':True,**TAIEX_JOB})
        if self.path.startswith('/api/research-test'):
            key=os.environ.get('GEMINI_API_KEY','').strip();model=os.environ.get('GEMINI_MODEL','gemini-3.8-flash').strip();t0=time.time()
            if not key:return self.sendj({'ok':False,'error':'沒有設定 GEMINI_API_KEY'})
            out={'ok':False,'model':model,'key_length':len(key)}
            for use_search in (False,True):
                tag='search' if use_search else 'plain'
                try:
                    body={'contents':[{'parts':[{'text':'只回答兩個字：正常'}]}]}
                    if use_search:body['tools']=[{'google_search':{}}]
                    req=urllib.request.Request('https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent'%urllib.parse.quote(model),data=json.dumps(body).encode(),method='POST',headers={'Content-Type':'application/json','x-goog-api-key':key})
                    with urllib.request.urlopen(req,timeout=45) as r:d=json.loads(r.read().decode())
                    cand=(d.get('candidates') or [{}])[0]
                    out[tag]={'ok':True,'text':''.join(p.get('text','') for p in (cand.get('content') or {}).get('parts',[]))[:40],'sec':round(time.time()-t0,1)}
                except urllib.error.HTTPError as e:out[tag]={'ok':False,'http':e.code,'detail':e.read().decode()[:300],'sec':round(time.time()-t0,1)}
                except Exception as e:out[tag]={'ok':False,'error':type(e).__name__+': '+str(e)[:150],'sec':round(time.time()-t0,1)}
            out['ok']=bool(out.get('plain',{}).get('ok') and out.get('search',{}).get('ok'))
            return self.sendj(out)
        if self.path.startswith('/api/research-models'):
            key=os.environ.get('GEMINI_API_KEY','').strip()
            if not key:return self.sendj({'ok':False,'error':'沒有設定 GEMINI_API_KEY'})
            return self.sendj({'ok':True,'primary':os.environ.get('GEMINI_MODEL','gemini-3.8-flash').strip(),'fallbacks':_alt_models(key,{os.environ.get('GEMINI_MODEL','gemini-3.8-flash').strip()})})
        if self.path.startswith('/api/research-quota'):return self.sendj(dict(ok=True,configured=bool(os.environ.get('GEMINI_API_KEY','').strip()),**research_quota()))
        if self.path.startswith('/api/heat-watch') or self.path.startswith('/api/heat-history') or self.path.startswith('/api/heat-snapshot'):
            q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query);M=__import__('sys').modules[__name__]
            try:
                if self.path.startswith('/api/heat-history'):return self.sendj(heat_monitor.watch_history(M,q.get('code',[''])[0]))
                if self.path.startswith('/api/heat-snapshot'):return self.sendj(heat_monitor.snapshot_all(M))
                return self.sendj(heat_monitor.watch_list(M))
            except Exception as exc:return self.sendj({'ok':False,'error':type(exc).__name__+' '+str(exc)[:160]})
        if self.path.startswith('/api/heat-monitor'):
            q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:return self.sendj(heat_monitor.api(__import__('sys').modules[__name__],q.get('code',[''])[0]))
            except Exception as exc:return self.sendj({'ok':False,'error':type(exc).__name__+' '+str(exc)[:160]})
        if self.path.startswith('/api/etf-holdings'):
            q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:return self.sendj(etf_holdings(q.get('code',[''])[0]))
            except Exception as exc:return self.sendj({'ok':False,'error':type(exc).__name__+' '+str(exc)[:120]})
        if self.path.startswith('/api/stock-research'):
            q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            try:return self.sendj(stock_research(q.get('code',[''])[0],start=q.get('start',['0'])[0]=='1'))
            except Exception as exc:return self.sendj({'ok':False,'error':'AI 研究失敗：'+type(exc).__name__+' '+str(exc)[:120]},500)
        if self.path.startswith('/api/stock-search'):
            try:
                q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                return self.sendj(search_stock_research(q.get('q',[''])[0]))
            except Exception as exc:return self.sendj({'ok':False,'error':'個股搜尋失敗：'+type(exc).__name__},500)
        if self.path.startswith('/api/revenue-status'):return self.sendj({'ok':True,**REVENUE_JOB})
        if self.path.startswith('/api/selected-data-status'):return self.sendj({'ok':True,**SELECTED_JOB})
        if self.path.startswith('/api/institutional-rankings'):
            try:return self.sendj(db_institutional_rankings())
            except Exception as e:return self.sendj({'ok':False,'error':'個股法人排行讀取失敗：'+type(e).__name__,'detail':str(e).split('\n')[0][:600]},500)
        if self.path.startswith('/api/market-chips'):
            try:return self.sendj(db_market_chips())
            except Exception:return self.sendj({'ok':False,'error':'盤後籌碼資料表尚未建立或無法讀取'},500)
        if self.path.startswith("/api/health"):return self.sendj({"ok":True,"version":"V2.0","database":db_health()})
        if self.path.startswith("/api/db-health"):return self.sendj(db_health(),200 if db_health().get("ok") else 500)
        if self.path.startswith("/api/strategies"):return self.sendj({"ok":True,"strategies":strategies_get()})
        if self.path.startswith("/api/warehouse-status"):return self.sendj({"ok":True,**warehouse_progress()})
        if self.path.startswith("/api/core-data-status"):return self.sendj(core_data_status())
        if self.path.startswith("/api/finmind-guard"):return self.sendj({"ok":True,**finmind_guard_status()})
        if self.path.startswith("/api/finmind-status"):
            return self.sendj({"ok":True,**token_status()})
        if self.path.startswith("/api/global"):
            try:return self.sendj(global_markets())
            except Exception as e:return self.sendj({"ok":False,"error":str(e)},500)
        if self.path.startswith('/api/flow-radar'):
            try:return self.sendj(flow_radar.api(__import__('sys').modules[__name__]))
            except Exception as e:return self.sendj({'ok':False,'error':'資金流向雷達讀取失敗：'+type(e).__name__},500)
        if self.path.startswith('/api/risk-gauges'):
            try:return self.sendj(risk_gauges.api())
            except Exception as e:return self.sendj({'ok':False,'error':'風險指標讀取失敗：'+type(e).__name__},500)
        if self.path.startswith('/api/taifex-night'):
            try:return self.sendj(taifex_night.api())
            except Exception as e:return self.sendj({'ok':False,'error':'台指期日報讀取失敗：'+type(e).__name__},500)
        if self.path.startswith('/api/us-tw-link/refresh'):
            started=us_tw_refresh_start(min_gap=300)
            return self.sendj({'ok':True,'started':started,'refresh':dict(US_TW_STATE)})
        if self.path.startswith('/api/us-tw-link'):
            try:
                res=us_tw_link.api(db_conn,USE_POSTGRES)
                today=datetime.now(ZoneInfo('Asia/Taipei')).date().isoformat()
                if (not res.get('ok')) or str(res.get('snapshot_date',''))<today:us_tw_refresh_start()
                res['refresh']=dict(US_TW_STATE)
                return self.sendj(res)
            except Exception as e:return self.sendj({'ok':False,'error':'連動統計讀取失敗：'+type(e).__name__},500)
        if self.path.startswith('/api/taiwan-market-close'):
            try:return self.sendj(taiwan_market_close())
            except Exception:return self.sendj({'ok':False,'error':'證交所盤後指數暫時無法取得'},503)
        if self.path.startswith('/api/intraday-flow'):
            try:return self.sendj(intraday_data())
            except Exception:return self.sendj({'ok':False,'error':'盤中快照資料庫讀取失敗'},500)
        if self.path.startswith("/api/capital-flow"):
            try:return self.sendj(capital_flow())
            except Exception as e:return self.sendj({"ok":False,"error":str(e)},500)
        if self.path.startswith("/api/industry-momentum"):
            try:return self.sendj(industry_momentum())
            except Exception as e:return self.sendj({"ok":False,"error":str(e)},500)
        if self.path.startswith("/api/etf-diag"):
            out={'ok':True,'build':'etf-v3-2026-10-05'}
            if 'run=1' in self.path:
                t0=time.time()
                try:
                    r=etf_research(fast=True)
                    out['fast']={'ok':r.get('ok'),'rows':len(r.get('rows',[])),'error':r.get('error'),'sec':round(time.time()-t0,1)}
                    with_px=[x for x in r.get('rows',[]) if x.get('close') or x.get('close_price')]
                    out['fast']['rows_with_price']=len(with_px)
                    out['fast']['sample']=(r.get('rows') or [{}])[0]
                except Exception as e:
                    import traceback
                    out['fast']={'error':type(e).__name__+': '+str(e)[:200],'trace':traceback.format_exc()[-600:],'sec':round(time.time()-t0,1)}
            if 'run=full' in self.path:
                t0=time.time()
                try:
                    r=etf_research()
                    rows=r.get('rows',[])
                    out['full']={'ok':r.get('ok'),'error':r.get('error'),'rows':len(rows),'sec':round(time.time()-t0,1),
                        'catalog_error':r.get('catalog_error'),'div_source':r.get('div_source'),
                        'rows_with_dividend':len([x for x in rows if x.get('last_dividend') or x.get('dividend_12m')]),
                        'state_error':ETF_LOAD_STATE.get('error')}
                except Exception as e:
                    import traceback
                    out['full']={'error':type(e).__name__+': '+str(e)[:200],'trace':traceback.format_exc()[-800:],'sec':round(time.time()-t0,1)}
            try:
                with db_conn() as c:
                    with c.cursor() as cur:
                        for name,sql in (
                            ('etf_daily_prices','SELECT count(DISTINCT stock_id),count(DISTINCT trade_date),min(trade_date),max(trade_date) FROM public.etf_daily_prices'),
                            ('holder_weekly_all',"SELECT count(DISTINCT stock_id),count(DISTINCT data_date),min(data_date),max(data_date) FROM public.holder_weekly"),
                            ('holder_weekly_etf',"SELECT count(DISTINCT stock_id),max(data_date) FROM public.holder_weekly WHERE stock_id ~ '^00'"),
                            ('etf_in_stocks_master',"SELECT count(*) FROM public.stocks WHERE stock_id ~ '^00'")):
                            try:
                                cur.execute(sql);out[name]=[str(x) for x in cur.fetchone()]
                            except Exception as e:
                                out[name]='錯誤：'+type(e).__name__+' '+str(e)[:80]
                                c.rollback()
            except Exception as e:out['db_error']=str(e)[:120]
            return self.sendj(out)
        if self.path.startswith("/api/etf-catalog"):
            try:
                if not ETF_LOAD_STATE.get('payload') and not ETF_LOAD_STATE['running']:
                    try:
                        q=etf_research(fast=True)   # 只讀資料庫，數秒內完成；不依賴背景執行緒
                        if q.get('ok'):ETF_LOAD_STATE['payload']=q
                    except Exception as exc:ETF_LOAD_STATE['error']='快速載入失敗：'+type(exc).__name__+' '+str(exc)[:160]
                return self.sendj(etf_catalog_view())
            except Exception as exc:return self.sendj({'ok':False,'error':str(exc)})
        if self.path.startswith("/api/etf-refresh"):
            etf_research_background()
            return self.sendj({'ok':True,'running':ETF_LOAD_STATE['running'],'error':ETF_LOAD_STATE['error']})
        if self.path.startswith("/api/etf-research"):
            try:return self.sendj(etf_research())
            except Exception as e:return self.sendj({"ok":False,"error":str(e)},500)
        if self.path.startswith('/api/industry-catalog'):
            try:
                q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                return self.sendj(industry_catalog(q.get('q',[''])[0]))
            except Exception:return self.sendj({'ok':False,'error':'產業分類資料暫時無法讀取'},500)
        if self.path.startswith('/api/industry-map/members'):
            try:
                q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                return self.sendj(industry_map.members(q.get('verified',[''])[0]=='1'))
            except Exception as e:return self.sendj({'ok':False,'error':'主題成員讀取失敗：'+type(e).__name__},500)
        if self.path.startswith('/api/industry-map'):
            try:
                q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                result=industry_map_view(q.get('q',[''])[0],q.get('theme',[''])[0])
                return self.sendj(result,200 if result['ok'] else 404)
            except Exception as e:return self.sendj({'ok':False,'error':'主題地圖讀取失敗：'+type(e).__name__},500)
        if self.path.startswith('/api/company-research-status'):return self.sendj({'ok':True,**PROFILE_JOB})
        if self.path.startswith('/api/company-profile'):
            try:
                q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                return self.sendj(company_profile(q.get('code',[''])[0]))
            except Exception as e:return self.sendj({'ok':False,'error':'公司資料讀取失敗：'+type(e).__name__,'detail':str(e).split('\n')[0][:300]},500)
        if self.path.startswith("/api/stock-health"):
            try:
                q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query);code=q.get("code",[""])[0]
                if not code:return self.sendj({"ok":False,"error":"missing code"},400)
                return self.sendj(db_stock_health(code))
            except Exception as e:return self.sendj({"ok":False,"error":str(e)},500)
        if self.path.startswith("/api/industries"):
            q=urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query); name=q.get("name",[""])[0]
            mp=industries()
            if name:
                stocks=[{"code":c,"name":v.get("name",""),"market":v.get("market","")} for c,v in mp.items() if v.get("industry")==name]
                return self.sendj({"ok":True,"name":name,"stocks":sorted(stocks,key=lambda x:x["code"])})
            counts={}
            for v in mp.values(): counts[v["industry"]]=counts.get(v["industry"],0)+1
            return self.sendj({"ok":True,"industries":[{"name":k,"count":v} for k,v in sorted(counts.items())]})
        return super().do_GET()
    def do_POST(self):
        if self.path.startswith('/api/line-webhook'):
            try:
                L=int(self.headers.get('Content-Length','0'));raw=self.rfile.read(L) if L else b''
                if not line_verify_signature(raw,self.headers.get('X-Line-Signature','')):
                    try:
                        import line_bot;line_bot.log('bad_signature',bytes=len(raw))
                    except Exception:pass
                    return self.sendj({'ok':False,'error':'bad signature'},403)
                payload=json.loads(raw or b'{}')
                try:
                    import line_bot;line_bot.log('webhook',events=len(payload.get('events',[])),types=','.join(sorted({str(e.get('type')) for e in payload.get('events',[])}))[:60])
                except Exception:pass
                self.sendj({'ok':True})
                threading.Thread(target=line_handle_events,args=(payload,),daemon=True).start()
                return
            except Exception:return self.sendj({'ok':False},400)
        if self.path in ('/api/heat-watch-add','/api/heat-watch-remove','/api/heat-watch-price'):
            try:
                L=int(self.headers.get('Content-Length','0'));q=json.loads(self.rfile.read(L) or b'{}');M=__import__('sys').modules[__name__]
                if self.path.endswith('price'):
                    try:return self.sendj(heat_monitor.watch_set_price(M,q.get('code',''),q.get('target'),q.get('stop')))
                    except ValueError as ve:return self.sendj({'ok':False,'error':str(ve)})
                if self.path.endswith('add'):return self.sendj(heat_monitor.watch_add(M,q.get('codes',''),str(q.get('note',''))[:200]))
                return self.sendj(heat_monitor.watch_remove(M,q.get('code','')))
            except Exception as exc:return self.sendj({'ok':False,'error':type(exc).__name__+' '+str(exc)[:160]},500)
        if self.path=='/api/company-research-update':return self.sendj(company_research_start())
        if self.path=='/api/intraday-update':
            try:return self.sendj(intraday_start())
            except Exception:return self.sendj({'ok':False,'error':'盤中更新無法啟動'},500)
        if self.path=="/api/strategy-save":
            try:
                L=int(self.headers.get("Content-Length","0"));q=json.loads(self.rfile.read(L) or b"{}")
                if not q.get("name"):return self.sendj({"ok":False,"error":"missing name"},400)
                strategy_save(q["name"],q.get("payload",{}));return self.sendj({"ok":True})
            except Exception as e:return self.sendj({"ok":False,"error":str(e)},500)
        if self.path=="/api/strategy-delete":
            try:
                L=int(self.headers.get("Content-Length","0"));q=json.loads(self.rfile.read(L) or b"{}")
                strategy_delete(q.get("name",""));return self.sendj({"ok":True})
            except Exception as e:return self.sendj({"ok":False,"error":str(e)},500)
        if self.path=="/api/warehouse-update":
            try:
                out=warehouse_start();return self.sendj(out,200 if out.get("ok") else 429)
            except Exception as e:return self.sendj({"ok":False,"error":str(e)},500)
        if self.path=='/api/financial-update':
            try:return self.sendj(financial_start())
            except Exception as exc:return self.sendj({'ok':False,'error':'官方財報更新無法啟動：'+str(exc)[:200]},500)
        if self.path=='/api/financial-history-update':
            try:return self.sendj(financial_history_start())
            except Exception as exc:return self.sendj({'ok':False,'error':'歷史財報回補無法啟動：'+str(exc)[:200]},500)
        if self.path=='/api/taiex-update':
            try:return self.sendj(taiex_start())
            except Exception as exc:return self.sendj({'ok':False,'error':'加權指數回補無法啟動：'+str(exc)[:200]},500)
        if self.path=='/api/revenue-update':
            try:return self.sendj(revenue_start())
            except Exception:return self.sendj({'ok':False,'error':'月營收更新無法啟動'},500)
        if self.path=='/api/chip-update':
            try:return self.sendj(chip_start())
            except Exception:return self.sendj({'ok':False,'error':'籌碼更新無法啟動'},500)
        if self.path=='/api/selected-data-update':
            try:
                L=int(self.headers.get('Content-Length','0'))
                if L>4096:return self.sendj({'ok':False,'error':'清單過長'},413)
                q=json.loads(self.rfile.read(L) or b'{}');out=selected_start(q.get('codes'))
                return self.sendj(out,200 if out.get('ok') else 400)
            except Exception:return self.sendj({'ok':False,'error':'個股資料更新無法啟動'},500)
        if self.path=="/api/scan-start":
            try:
                L=int(self.headers.get("Content-Length","0"));q=json.loads(self.rfile.read(L) or b"{}")
                return self.sendj(scan_job_start(Handler,q))
            except Exception as e:return self.sendj({"ok":False,"error":"選股無法啟動："+str(e)[:100]},500)
        if self.path!="/api/scan":return self.sendj({"error":"not found"},404)
        try:
            L=int(self.headers.get("Content-Length","0"));q=json.loads(self.rfile.read(L) or b"{}")
        except Exception:return self.sendj({"ok":False,"error":"請求內容不正確"},400)
        return self._scan_core(q)

    def _scan_core(self,q):
        try:
            universe=q.get("universe","both");base=db_universe(universe)
            if not base:return self.sendj({"ok":False,"code":"EMPTY_WAREHOUSE","error":"資料庫尚無上市櫃股票清單；請先執行資料更新。"},422)
            industry=q.get("industry","")
            # Industry research never narrows the independent full-market screener.
            industry=""
            conds=q.get("conditions",[])
            if not isinstance(conds,list) or len(conds)>40:return self.sendj({"ok":False,"error":"公式數量不正確"},400)
            if not conds:return self.sendj({"ok":False,"error":"請先加入至少一條自由公式"},400)
            reasons=scan_plan(conds)
            active=[(i,c) for i,c in enumerate(conds) if not reasons[i] and c.get('op')]
            computed=[(i,c) for i,c in enumerate(conds) if not reasons[i] and not c.get('op')]
            if not active and not computed:
                return self.sendj({"ok":False,"code":"NO_RUNNABLE_FORMULA","error":"目前沒有可計算的公式。請看每一行缺少的資料。","condition_status":[{"row":i+1,"reason":reason,"kind":"numeric" if not c.get('op') else "comparison"} for i,(c,reason) in enumerate(zip(conds,reasons))]},422)
            if any(c.get('not') or c.get('group_open') or c.get('group_close') for _,c in computed):
                return self.sendj({"ok":False,"error":"純算式是數值，不能套用 NOT 或邏輯括號；請修改該行的進階邏輯設定。"},422)
            partial=bool(any(reasons))
            # Parentheses spanning paused rows cannot be removed without changing the user's logic.
            if any(reasons[i] and c.get('op') for i,c in enumerate(conds)) and any(c.get('op') and (c.get('group_open') or c.get('group_close') or c.get('logic')=='any' or c.get('not')) for c in conds):
                return self.sendj({"ok":False,"code":"PARTIAL_LOGIC","error":"有暫停條件且使用 OR／NOT／括號，不能安全地略過條件運算。請補齊資料或先另存簡化策略。","condition_status":[{"row":i+1,"reason":reason} for i,reason in enumerate(reasons)]},422)
            candidates=base
            runnable=active+computed
            referenced_fields={t.get('field') for _,c in runnable for t in condition_terms(c)}
            revenue_cache=db_all_revenue_yoy() if 'revenue_yoy' in referenced_fields else {}
            revenue_mom_cache=db_all_revenue_yoy(kind='mom') if 'revenue_mom' in referenced_fields else {}
            ratio_hist=db_margin_ratio_history() if 'short_margin_ratio' in referenced_fields else {}
            profile_cache=db_company_profiles_all() if 'capital_100m' in referenced_fields else {}
            extra_fields_all={"foreign_net","trust_net","dealer_net","margin_balance","short_balance","margin_change","short_change","revenue_yoy","revenue_mom","eps","foreign_holding","foreign_holding_pct","daytrade_volume","large_holder_pct","gross_margin","operating_margin","capital_100m","pe","dividend_yield","book_value_per_share","pb","pretax_margin","net_margin_pct","net_margin","net_volume","margin_utilization","short_utilization","open_interest_proxy","margin_volume_ratio","short_volume_ratio","short_margin_ratio","daytrade_ratio"}
            _light=bool(profile_cache) and (referenced_fields & extra_fields_all) <= {'capital_100m','revenue_yoy','revenue_mom'}
            chip_cache=db_chip_fundamental_all() if (not _light and (referenced_fields-{'revenue_yoy','revenue_mom'}) & extra_fields_all) else {}
            hits=[]
            trace_enabled=all(c.get('logic')!='any' and not c.get('not') and not c.get('group_open') and not c.get('group_close') for _,c in active)
            cumulative=[0]*len(active);near_matches=[]
            cond_pass=[0]*len(conds);cond_missing=[0]*len(conds);cond_tested=[0]*len(conds)
            actually_scanned=0
            by_code={r['code']:r for r in candidates}
            # All-AND strategies stop at the first failing row (in the user's own row order), so the
            # cumulative filter trace is exact. full_diag evaluates every row on every stock instead.
            full=bool(q.get('full_diag')) or not trace_enabled
            def condition_fields(c):
                out=[]
                if "left" in c:
                    for side in (c.get("left",{}),c.get("right",{})):
                        for t in (side.get("a",{}),side.get("b",{})):
                            if t.get("field"):out.append(t.get("field"))
                else:
                    if c.get("lhs",{}).get("field"):out.append(c["lhs"]["field"])
                    if c.get("rhs_type")=="expr" and c.get("rhs",{}).get("field"):out.append(c["rhs"]["field"])
                return out
            cf=[f for _,c in runnable for f in condition_fields(c)]
            extra_fields=extra_fields_all
            needed=any(f in extra_fields for f in cf)
            only_light=bool(profile_cache) and referenced_fields & extra_fields <= {'capital_100m','revenue_yoy','revenue_mom'}
            probe_set={str(x).strip() for x in (q.get('probe') or [])[:30] if str(x).strip()};probe_out=[]
            live_bars={};intraday_info=None;live_used=0
            if q.get('intraday'):
                try:live_bars,intraday_info=intraday_scan_bars(candidates)
                except Exception as e:intraday_info={'fetched':0,'errors':[str(e)[:80]]}
            _MEMO.d={}
            try:
              for code,hist in db_histories_for_universe(candidates):
                if getattr(self,'cancelled',False):return
                _MEMO.d={}
                r=by_code[code]
                if live_bars:
                    qb=live_bars.get(code)
                    if qb and (not hist or str(hist[-1].get('date'))<qb['date']):
                        hist=list(hist)+[qb];live_used+=1
                actually_scanned+=1
                r['price_date']=str(hist[-1].get('date') or '')
                r["close"]=hist[-1].get("close");r["volume"]=hist[-1].get("volume")
                if len(hist)>1 and hist[-2].get("close"):r["change_pct"]=(hist[-1].get("close",0)/hist[-2]["close"]-1)*100
                extras=({'capital_100m':profile_cache.get(r['code'],{}).get('capital_100m')} if profile_cache else {}) if only_light else (dict(chip_cache.get(r['code'],{})) if needed and (referenced_fields-{'revenue_yoy','revenue_mom'}) & extra_fields else {})
                if "revenue_yoy" in cf:
                    extras['revenue_yoy']=revenue_cache.get(r['code'])
                if "revenue_mom" in cf:
                    extras['revenue_mom']=revenue_mom_cache.get(r['code'])
                if needed and hist:
                    cv=num(hist[-1].get("volume")) or 0
                    mb=num(extras.get("margin_balance")); sb=num(extras.get("short_balance")); dv=num(extras.get("daytrade_volume"))
                    extras["net_margin"]=(mb-sb) if mb is not None and sb is not None else None
                    extras["net_volume"]=cv
                    extras["margin_volume_ratio"]=(mb/cv*100) if mb is not None and cv else None
                    extras["short_volume_ratio"]=(sb/cv*100) if sb is not None and cv else None
                    extras["short_margin_ratio"]=(sb/mb*100) if sb is not None and mb else None
                    extras["daytrade_ratio"]=(dv/cv*100) if dv is not None and cv else None
                    # Fields whose official denominator/source is not available in this request stay None rather than fabricated.
                    extras.setdefault("margin_utilization",None);extras.setdefault("short_utilization",None);extras.setdefault("open_interest_proxy",None)
                if "short_margin_ratio" in cf:  # must come after the scalar block above, which would overwrite it
                    extras['short_margin_ratio']=ratio_hist.get(r['code'])  # history list so “10日內最大值” works; None if no data
                if code in probe_set:
                    rows_=[]
                    for ci,c in active:
                        try:ok,av,bv=check(hist,c,extras,r["code"])
                        except Exception:ok,av,bv=None,None,None
                        rows_.append({'row':ci+1,'ok':ok,'a':av,'b':bv,'op':c.get('op'),'label':c.get('label','')})
                    probe_out.append({'code':code,'name':r['name'],'price_date':r['price_date'],'rows':rows_})
                checks=[None]*len(active);details=[{"ok":None,"a":None,"b":None} for _ in active]
                prefix_ok=True;passed_prefix=0;n_eval=0
                for j,(ci,c) in enumerate(active):
                    # Stop at the first failing row, except for stocks that already passed almost every
                    # row: those are finished so the "closest matches" list stays meaningful.
                    if not full and not prefix_ok and passed_prefix<len(active)-3:break
                    ok,av,bv=check(hist,c,extras,r["code"]);checks[j]=ok;details[j]={"ok":ok,"a":av,"b":bv};n_eval=j+1
                    cond_tested[ci]+=1
                    if ok: cond_pass[ci]+=1
                    if av is None or bv is None: cond_missing[ci]+=1
                    if prefix_ok and ok is True:passed_prefix+=1
                    else:prefix_ok=False
                good=evaluate_condition_groups([c for _,c in active],checks) if active else True
                if trace_enabled:
                    prefix=True
                    for j,value in enumerate(checks):
                        prefix=prefix and value is True
                        if prefix:cumulative[j]+=1
                    if good is not True and n_eval==len(active):
                        near_matches.append({'code':code,'name':r['name'],'price_date':r['price_date'],
                            'checks':[{'row':ci+1,'a':details[j]['a'],'op':c['op'],'b':details[j]['b'],'ok':checks[j]} for j,(ci,c) in enumerate(active)],
                            'matched':sum(value is True for value in checks),
                            'failed':[{'row':ci+1,'a':details[j]['a'],'op':c['op'],'b':details[j]['b'],'missing':checks[j] is None} for j,(ci,c) in enumerate(active) if checks[j] is not True]})
                        near_matches.sort(key=lambda x:(-x['matched'],x['code']))
                        del near_matches[10:]
                calculated=[]
                if good is True:
                    for ci,c in computed:
                        value=side_value(hist,c.get('left',{}),extras,r['code'])
                        calculated.append({'row':ci+1,'value':value})
                        cond_tested[ci]+=1
                        if value is None:cond_missing[ci]+=1
                        else:cond_pass[ci]+=1
                # A pure arithmetic row supplies a value and sorting key, not a
                # boolean gate. Missing values remain null; they never become zero.
                if good is True and (active or any(x['value'] is not None for x in calculated)):
                    z=dict(r);z["matched"]=sum(v is True for v in checks);z["total"]=len(active);z["details"]=details
                    recent=[num(x.get('close')) for x in hist[-20:]]
                    z['ma20']=sum(recent)/20 if len(recent)==20 and all(v is not None for v in recent) else None
                    z['vs_ma20_pct']=((num(z.get('close'))/z['ma20']-1)*100) if z['ma20'] and num(z.get('close')) is not None else None
                    z['computed']=calculated;z["health"]=health_from_hist(hist);hits.append(z)
            finally:
                _MEMO.d=None
            # A populated table may still cover only some companies; never call that full screening.
            partial=partial or any(cond_missing[i]>0 for i,_ in runnable)
            if computed:hits.sort(key=lambda x:(x['computed'][0]['value'] is not None,x['computed'][0]['value'] if x['computed'][0]['value'] is not None else float('-inf'),x.get('change_pct') or -999),reverse=True)
            else:hits.sort(key=lambda x:(x["matched"],x.get("change_pct") or -999),reverse=True)
            try:
                _tags=flow_radar.annotate(__import__('sys').modules[__name__],[z['code'] for z in hits[:100]]) if hits else {}
                for z in hits[:100]:z['flow']=_tags.get(str(z['code']))
            except Exception:pass
            self.sendj({"ok":True,"market_total":len(base),"scanned":actually_scanned,"count":len(hits),"rows":hits[:100],"partial":partial,"active_conditions":len(active),"total_conditions":len(conds),
                        "numeric_conditions":len(computed),"sort_basis":('第 '+str(computed[0][0]+1)+' 行數值，由大到小') if computed else '符合條件與漲跌幅',
                        "condition_status":[{"row":i+1,"reason":reason,"kind":"numeric" if not c.get('op') else 'comparison'} for i,(c,reason) in enumerate(zip(conds,reasons))],
                        "condition_stats":[{"pass":cond_pass[i],"missing":cond_missing[i],"tested":cond_tested[i]} for i in range(len(conds))],
                        "short_circuit":not full,"intraday":(dict(intraday_info,used=live_used) if intraday_info is not None else None),"probe":probe_out,"margin_history":({"stocks":len(ratio_hist),"days":max((len(v) for v in ratio_hist.values()),default=0),"stocks_10d":sum(1 for v in ratio_hist.values() if len(v)>=10)} if ratio_hist else None),
                        "revenue_coverage":_revenue_coverage(revenue_cache) if revenue_cache else None,
                        "filter_trace":[{'row':ci+1,'remaining':cumulative[j],'removed':(actually_scanned if j==0 else cumulative[j-1])-cumulative[j]} for j,(ci,c) in enumerate(active)] if trace_enabled else [],
                        "near_matches":near_matches if not hits and trace_enabled else [],
                        "note":"純算式逐檔計算並作為結果數值及排序依據；純算式缺值顯示缺資料，不排除已通過比較的股票；比較式缺值不能視為通過。結果最多顯示前100檔。","warehouse":db_status(),
                        "finmind_guard":finmind_guard_status()})
        except ValueError as e:self.sendj({"ok":False,"error":str(e)},400)
        except Exception:
            import traceback
            traceback.print_exc()
            self.sendj({"ok":False,"error":"選股執行失敗，請檢查資料庫連線與伺服器紀錄。"},500)

if __name__=="__main__":
    os.chdir(ROOT);print("Private Taiwan Research Desk V2.0",PORT);ThreadingHTTPServer(("0.0.0.0",PORT),Handler).serve_forever()

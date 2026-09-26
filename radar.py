#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""异动雷达 —— 全市场 USDT 永续合约实时异常检测。

设计原则：
  * 只报事实，不判断方向，不给出入场/止损/目标。
  * 每个币用「自己的历史基准」比较，而不是全市场统一阈值，
    这样小币的正常波动不会淹没大币的真实异动。
  * 所有阈值都是相对倍数 + 绝对门槛双重把关，避免冷门币刷屏。

数据源（单条 WS 连接覆盖全市场）：
  !ticker@arr      每秒全市场 24h ticker → 差分得到每分钟成交额与价格变化
  !markPrice@arr   每秒标记价格与资金费率
  REST openInterest 轮询持仓量（5 分钟周期）

检测项：
  1 成交额异动   1分钟成交额 / 自身60分钟中位数 >= VOL_MULT
  2 价格急变     1分钟涨跌幅 / 自身波动基准 >= MOVE_MULT
  3 资金费率异常 |费率| >= FUND_ABS 或 5分钟内跳变 >= FUND_JUMP
  4 持仓量突变   5分钟 OI 变化 >= OI_CHG
  5 突破24h高低  价格创 24 小时新高/新低（需同时有量能配合）
"""
import asyncio
import collections
import datetime
import json
import os
import time
import urllib.parse
import urllib.request

# ───────────────────────── 配置 ─────────────────────────
TZ = 8 * 3600                      # UTC+8

# 覆盖范围
MIN_24H_TURNOVER = 3_000_000       # 只看 24h 成交额 >= 300万 USDT 的币
EXCLUDE = set()                    # 想屏蔽的币写这里，如 {"BTCDOMUSDT"}
INCLUDE_TRADFI = True              # 是否包含美股/港股/商品等 TradFi 永续（contractType=TRADIFI_PERPETUAL）

# 1 成交额异动
VOL_MULT = 15.0                    # 1分钟成交额 >= 自身中位数的 15 倍
VOL_MIN_ABS = 3_000_000            # 且该分钟绝对成交额 >= 300万 USDT
VOL_NEED_MOVE = 1.0                # 且该分钟价格至少变动 1.0%，滤掉「放量但不动」的撮合脉冲

# 2 价格急变
MOVE_MULT = 12.0                   # 1分钟涨跌 >= 自身基准波动的 12 倍
MOVE_MIN_ABS = 3.0                 # 且绝对涨跌 >= 3.0%

# 3 资金费率
FUND_ABS = 0.20                    # |资金费率| >= 0.20%（年化约 220%）
FUND_JUMP = 0.05                   # 或 5 分钟内跳变 >= 0.05%
FUND_REALERT_MULT = 1.5            # 已报过的币，费率绝对值再扩大到上次的 1.5 倍才再报
FUND_REALERT_SEC = 4 * 3600        # 或距上次报警超过 4 小时

# 4 持仓量
OI_CHG = 8.0                       # 5分钟 OI 变化 >= 8%
OI_MIN_NOTIONAL = 10_000_000       # 且 OI 名义价值 >= 1000万 USDT（滤掉小盘噪音）

# 5 突破 24h 高低
BREAK_ENABLE = True
BREAK_VOL_MULT = 3.0               # 突破时需伴随 3 倍量能
BREAK_MIN_ABS = 1_000_000          # 突破时该分钟成交额门槛（独立于成交额异动，避免被其 300万 门槛淹没）

# 推送节流
COOLDOWN = 1800                    # 同币同类型 30 分钟内不重复推送
MAX_PER_MIN = 6                    # 全局每分钟最多推送条数
QUIET_START, QUIET_END = None, None  # 免打扰时段，例如 (2, 8) 表示凌晨2-8点静默

BASE = "https://fapi.binance.com"
# 币安 2026 起行情流迁到 /market 路径；旧的 /stream 仍能握手但不再推送任何数据
WS_URL = "wss://fstream.binance.com/market/stream?streams=!ticker@arr/!markPrice@arr@1s"
WS_IDLE_TIMEOUT = 30               # 超过该秒数没收到任何消息就判定为假死并重连


# ───────────────────────── 工具 ─────────────────────────
_TZINFO = datetime.timezone(datetime.timedelta(seconds=TZ))


def now_str():
    return datetime.datetime.now(_TZINFO).strftime("%H:%M:%S")


def cur_hour():
    return datetime.datetime.now(_TZINFO).hour


def fmt_px(p):
    if p >= 1000:
        return f"{p:,.1f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.5f}"
    return f"{p:.7f}".rstrip("0")


def fmt_money(x):
    if x >= 1e8:
        return f"{x/1e8:.2f}亿"
    if x >= 1e4:
        return f"{x/1e4:.0f}万"
    return f"{x:.0f}"


def rest(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read().decode())


def median(seq):
    s = sorted(seq)
    n = len(s)
    if n == 0:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


# ───────────────────────── 推送 ─────────────────────────
_push_times = collections.deque(maxlen=60)


def push(text):
    print(f"[{now_str()}] PUSH | " + text.replace("\n", " | "), flush=True)
    if QUIET_START is not None:
        h = cur_hour()
        in_quiet = (QUIET_START <= h < QUIET_END if QUIET_START < QUIET_END
                    else h >= QUIET_START or h < QUIET_END)
        if in_quiet:
            return
    t = time.time()
    while _push_times and t - _push_times[0] > 60:
        _push_times.popleft()
    if len(_push_times) >= MAX_PER_MIN:
        print(f"[{now_str()}] (节流：本分钟推送已达上限，跳过)", flush=True)
        return
    _push_times.append(t)
    # 通过独立 Telegram bot 直接推送，凭据来自环境变量
    token = os.environ.get("TG_BOT_TOKEN", "")
    chat_id = os.environ.get("TG_CHAT_ID", "")
    if not token or not chat_id:
        print(f"[{now_str()}] 推送失败: 未配置 TG_BOT_TOKEN / TG_CHAT_ID", flush=True)
        return
    try:
        body = urllib.parse.urlencode({"chat_id": chat_id, "text": text,
                                       "disable_web_page_preview": "true"}).encode()
        req = urllib.request.Request(f"https://api.telegram.org/bot{token}/sendMessage",
                                     data=body, headers={"User-Agent": "anomaly-radar"})
        resp = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
        if not resp.get("ok"):
            print(f"[{now_str()}] 推送失败: {resp}", flush=True)
    except Exception as e:
        print(f"[{now_str()}] 推送失败: {e}", flush=True)


# ───────────────────────── 状态 ─────────────────────────
class Sym:
    __slots__ = ("sym", "px", "qv", "last_qv", "last_min", "vols", "moves",
                 "last_px_min", "hi24", "lo24", "funding", "fund_hist",
                 "oi", "oi_hist", "turnover", "alerts", "fund_alerted")

    def __init__(self, sym):
        self.sym = sym
        self.px = 0.0
        self.qv = 0.0              # 24h 累计成交额
        self.last_qv = None
        self.last_min = 0
        self.vols = collections.deque(maxlen=60)    # 每分钟成交额
        self.moves = collections.deque(maxlen=60)   # 每分钟绝对涨跌%
        self.last_px_min = 0.0
        self.hi24 = 0.0
        self.lo24 = 0.0
        self.funding = 0.0
        self.fund_hist = collections.deque(maxlen=10)
        self.oi = 0.0
        self.oi_hist = collections.deque(maxlen=12)
        self.turnover = 0.0
        self.alerts = {}           # 类型 -> 上次推送时间
        self.fund_alerted = 0.0    # 上次资金费率异常报警时的费率（%），0 表示未处于异常

    def can_alert(self, kind):
        return time.time() - self.alerts.get(kind, 0) > COOLDOWN

    def mark(self, kind):
        self.alerts[kind] = time.time()


SYMS = {}
WS_MSGS = 0                        # 累计收到的 WS 消息数，用于心跳自检


# ───────────────────────── 检测 ─────────────────────────
def on_minute_close(s, minute_qv, minute_move):
    """每分钟结算时的检测。minute_qv=该分钟成交额，minute_move=该分钟涨跌%"""
    name = s.sym[:-4]

    base_vol = median(s.vols) if len(s.vols) >= 20 else 0
    base_move = median(s.moves) if len(s.moves) >= 20 else 0

    # 先各自判定，再决定怎么发：同一分钟同时触发量价两项时合并成一条，不重复打扰
    vol_hit = move_hit = None

    # ── 1 成交额异动 ──
    if base_vol > 0 and minute_qv >= VOL_MIN_ABS and abs(minute_move) >= VOL_NEED_MOVE:
        ratio = minute_qv / base_vol
        if ratio >= VOL_MULT and s.can_alert("vol"):
            vol_hit = ratio

    # ── 2 价格急变 ──
    if base_move > 0 and abs(minute_move) >= MOVE_MIN_ABS:
        ratio = abs(minute_move) / base_move
        if ratio >= MOVE_MULT and s.can_alert("move"):
            move_hit = ratio

    if vol_hit and move_hit:
        s.mark("vol")
        s.mark("move")
        arrow = "急涨" if minute_move > 0 else "急跌"
        push(f"⚡ 放量{arrow} {name}\n"
             f"1分钟 {minute_move:+.2f}%，为常态波动 {move_hit:.0f} 倍\n"
             f"成交额 {fmt_money(minute_qv)} USDT，为常态 {vol_hit:.0f} 倍\n"
             f"现价 {fmt_px(s.px)}\n"
             f"时间 {now_str()}")
    elif vol_hit:
        s.mark("vol")
        push(f"📊 成交额异动 {name}\n"
             f"1分钟 {fmt_money(minute_qv)} USDT，为常态 {vol_hit:.0f} 倍\n"
             f"现价 {fmt_px(s.px)}（该分钟 {minute_move:+.2f}%）\n"
             f"时间 {now_str()}")
    elif move_hit:
        s.mark("move")
        arrow = "急涨" if minute_move > 0 else "急跌"
        vtxt = f"，同时成交额 {fmt_money(minute_qv)}" if minute_qv else ""
        push(f"⚡ 价格{arrow} {name}\n"
             f"1分钟 {minute_move:+.2f}%，为常态波动 {move_hit:.0f} 倍\n"
             f"现价 {fmt_px(s.px)}{vtxt}\n"
             f"时间 {now_str()}")

    # ── 5 突破 24h 高低 ──
    if BREAK_ENABLE and s.hi24 > 0 and base_vol > 0:
        vr = minute_qv / base_vol if base_vol else 0
        if vr >= BREAK_VOL_MULT and minute_qv >= BREAK_MIN_ABS:
            if s.px >= s.hi24 and s.can_alert("hi"):
                s.mark("hi")
                push(f"🔺 创24小时新高 {name}\n"
                     f"现价 {fmt_px(s.px)}，前高 {fmt_px(s.hi24)}\n"
                     f"伴随量能 {vr:.0f} 倍（{fmt_money(minute_qv)}）\n"
                     f"时间 {now_str()}")
            elif s.px <= s.lo24 and s.can_alert("lo"):
                s.mark("lo")
                push(f"🔻 创24小时新低 {name}\n"
                     f"现价 {fmt_px(s.px)}，前低 {fmt_px(s.lo24)}\n"
                     f"伴随量能 {vr:.0f} 倍（{fmt_money(minute_qv)}）\n"
                     f"时间 {now_str()}")


def check_funding(s, new_rate):
    """资金费率异常。new_rate 单位 %"""
    name = s.sym[:-4]
    old = s.funding
    s.funding = new_rate
    s.fund_hist.append((time.time(), new_rate))

    if abs(new_rate) < FUND_ABS * 0.8:
        s.fund_alerted = 0.0       # 回落到阈值以下（留 20% 回差），下次再超阈值时重新报警
    elif abs(new_rate) >= FUND_ABS and (
            not s.fund_alerted
            or new_rate * s.fund_alerted < 0
            or abs(new_rate) >= abs(s.fund_alerted) * FUND_REALERT_MULT
            or time.time() - s.alerts.get("fund_abs", 0) > FUND_REALERT_SEC):
        s.mark("fund_abs")
        s.fund_alerted = new_rate
        side = "多头付空头" if new_rate > 0 else "空头付多头"
        push(f"💰 资金费率异常 {name}\n"
             f"当前 {new_rate:+.4f}%（{side}），年化约 {new_rate*3*365:+.0f}%\n"
             f"现价 {fmt_px(s.px)}\n"
             f"时间 {now_str()}")
        return

    t = time.time()
    old_pts = [r for ts, r in s.fund_hist if t - ts >= 300]
    if old_pts:
        jump = new_rate - old_pts[-1]
        if abs(jump) >= FUND_JUMP and s.can_alert("fund_jump"):
            s.mark("fund_jump")
            push(f"💰 资金费率跳变 {name}\n"
                 f"5分钟内 {old_pts[-1]:+.4f}% → {new_rate:+.4f}%（{jump:+.4f}%）\n"
                 f"现价 {fmt_px(s.px)}\n"
                 f"时间 {now_str()}")


def check_oi(s, new_oi):
    name = s.sym[:-4]
    t = time.time()
    s.oi_hist.append((t, new_oi))
    s.oi = new_oi
    notional = new_oi * s.px
    if notional < OI_MIN_NOTIONAL:
        return
    old = [(ts, v) for ts, v in s.oi_hist if 240 <= t - ts <= 420]
    if not old:
        return
    prev = old[0][1]
    if prev <= 0:
        return
    chg = (new_oi / prev - 1) * 100
    if abs(chg) >= OI_CHG and s.can_alert("oi"):
        s.mark("oi")
        word = "激增" if chg > 0 else "骤降"
        push(f"🏦 持仓量{word} {name}\n"
             f"5分钟 {chg:+.1f}%，当前名义 {fmt_money(notional)} USDT\n"
             f"现价 {fmt_px(s.px)}\n"
             f"时间 {now_str()}")


# ───────────────────────── 数据循环 ─────────────────────────
def eligible_symbols():
    """启动与每小时刷新共用同一套筛选规则。"""
    types = {"PERPETUAL"} | ({"TRADIFI_PERPETUAL"} if INCLUDE_TRADFI else set())
    info = rest(f"{BASE}/fapi/v1/exchangeInfo")
    return {x["symbol"] for x in info["symbols"]
            if x["contractType"] in types
            and x["status"] == "TRADING"
            and x["quoteAsset"] == "USDT"
            and x["symbol"] not in EXCLUDE}


def add_new_symbols():
    """把符合条件且尚未监控的币加入 SYMS，返回新增的币名列表。"""
    ok = eligible_symbols()
    added = []
    for t in rest(f"{BASE}/fapi/v1/ticker/24hr"):
        sym = t["symbol"]
        if sym in SYMS or sym not in ok:
            continue
        if float(t["quoteVolume"]) < MIN_24H_TURNOVER:
            continue
        s = Sym(sym)
        s.px = float(t["lastPrice"])
        s.qv = float(t["quoteVolume"])
        s.hi24 = float(t["highPrice"])
        s.lo24 = float(t["lowPrice"])
        s.turnover = s.qv
        s.last_px_min = s.px
        SYMS[sym] = s
        added.append(sym)
    return added


async def load_universe():
    n = len(add_new_symbols())
    print(f"[{now_str()}] 监控范围：{n} 个 USDT 永续（24h成交额 >= {fmt_money(MIN_24H_TURNOVER)}）",
          flush=True)


async def seed_baseline(syms=None):
    """用历史 1m K 线预热每个币的成交额/波动基准，避免重启或新增后 20 分钟空窗。"""
    syms = list(SYMS) if syms is None else list(syms)
    ok = 0
    sem = asyncio.Semaphore(8)
    loop = asyncio.get_event_loop()

    async def one(sym):
        nonlocal ok
        s = SYMS.get(sym)
        if not s:
            return
        async with sem:
            try:
                k = await loop.run_in_executor(
                    None, rest,
                    f"{BASE}/fapi/v1/klines?symbol={sym}&interval=1m&limit=61")
                for x in k[:-1]:
                    qv = float(x[7])                     # quote asset volume
                    o, c = float(x[1]), float(x[4])
                    s.vols.append(qv)
                    s.moves.append(abs((c / o - 1) * 100) if o else 0.0)
                ok += 1
            except Exception:
                pass

    await asyncio.gather(*(one(x) for x in syms))
    print(f"[{now_str()}] 基准预热完成：{ok}/{len(syms)} 个币已就绪，立即可检测", flush=True)


async def ws_ticker():
    import websockets
    global WS_MSGS
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20,
                                          max_size=8 * 1024 * 1024) as ws:
                print(f"[{now_str()}] WS 已连接", flush=True)
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), WS_IDLE_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise RuntimeError(f"{WS_IDLE_TIMEOUT} 秒未收到任何行情消息（连接假死）")
                    WS_MSGS += 1
                    m = json.loads(raw)
                    stream = m.get("stream", "")
                    data = m.get("data", [])
                    if stream.startswith("!ticker"):
                        handle_ticker(data)
                    elif stream.startswith("!markPrice"):
                        handle_markprice(data)
        except Exception as e:
            print(f"[{now_str()}] WS 断开: {e}，5 秒后重连", flush=True)
            await asyncio.sleep(5)


def handle_ticker(arr):
    minute = int(time.time() // 60)
    for t in arr:
        sym = t.get("s")
        s = SYMS.get(sym)
        if not s:
            continue
        px = float(t["c"])
        qv = float(t["q"])          # 24h 累计成交额
        s.px = px
        s.turnover = qv
        s.hi24 = max(s.hi24, float(t["h"]))
        s.lo24 = min(s.lo24, float(t["l"])) if s.lo24 else float(t["l"])

        if s.last_min == 0:
            s.last_min = minute
            s.last_qv = qv
            s.last_px_min = px
            continue

        if minute > s.last_min:
            # 分钟结算：用 24h 累计额差分得到该分钟成交额
            dq = qv - s.last_qv if s.last_qv is not None else 0
            if dq < 0:
                dq = 0              # 24h 窗口滚动导致的负差分，丢弃
            move = (px / s.last_px_min - 1) * 100 if s.last_px_min else 0
            if len(s.vols) >= 20:
                on_minute_close(s, dq, move)
            s.vols.append(dq)
            s.moves.append(abs(move))
            s.last_min = minute
            s.last_qv = qv
            s.last_px_min = px


def handle_markprice(arr):
    for t in arr:
        sym = t.get("s")
        s = SYMS.get(sym)
        if not s:
            continue
        try:
            rate = float(t["r"]) * 100     # 转成 %
        except (KeyError, ValueError):
            continue
        if not s.fund_hist or abs(rate - s.fund_hist[-1][1]) > 1e-9:
            check_funding(s, rate)


async def poll_oi():
    await asyncio.sleep(30)
    while True:
        syms = list(SYMS)
        for sym in syms:
            s = SYMS.get(sym)
            if not s or s.turnover < MIN_24H_TURNOVER * 3:
                continue                      # OI 只查较活跃的币，控制请求量
            try:
                d = rest(f"{BASE}/fapi/v1/openInterest?symbol={sym}", timeout=10)
                check_oi(s, float(d["openInterest"]))
            except Exception:
                pass
            await asyncio.sleep(0.12)
        await asyncio.sleep(60)


async def refresh_universe():
    while True:
        await asyncio.sleep(3600)
        try:
            added = add_new_symbols()
            if added:
                print(f"[{now_str()}] 新增 {len(added)} 个符合条件的币种", flush=True)
                await seed_baseline(added)
        except Exception as e:
            print(f"[{now_str()}] 刷新universe失败: {e}", flush=True)


async def heartbeat():
    last = 0
    while True:
        await asyncio.sleep(1800)
        ready = sum(1 for s in SYMS.values() if len(s.vols) >= 20)
        got = WS_MSGS - last
        last = WS_MSGS
        print(f"[{now_str()}] 运行中：{len(SYMS)} 币在监控，{ready} 个已建立基准，"
              f"近30分钟 WS 消息 {got} 条", flush=True)


def seed_funding():
    """启动时用 REST 预置资金费率：已处于异常的币汇总进启动消息，不再逐条刷屏。"""
    hot = []
    try:
        for x in rest(f"{BASE}/fapi/v1/premiumIndex"):
            s = SYMS.get(x.get("symbol"))
            if not s:
                continue
            rate = float(x["lastFundingRate"]) * 100
            s.funding = rate
            s.fund_hist.append((time.time(), rate))
            if abs(rate) >= FUND_ABS:
                s.fund_alerted = rate
                s.mark("fund_abs")
                hot.append((rate, s.sym[:-4]))
    except Exception as e:
        print(f"[{now_str()}] 预置资金费率失败: {e}", flush=True)
    hot.sort(key=lambda x: -abs(x[0]))
    return hot


async def main():
    await load_universe()
    await seed_baseline()
    hot = seed_funding()
    hot_txt = ("\n当前费率异常：" + "，".join(f"{n} {r:+.3f}%" for r, n in hot[:10])
               + (f" 等 {len(hot)} 个" if len(hot) > 10 else "")) if hot else ""
    push(f"📡 异动雷达已启动\n"
         f"监控 {len(SYMS)} 个 USDT 永续合约\n"
         f"检测：成交额异动 / 价格急变 / 资金费率异常 / 持仓量突变 / 24h高低突破\n"
         f"仅报告事实，不含方向建议{hot_txt}\n"
         f"时间 {now_str()}")
    await asyncio.gather(ws_ticker(), poll_oi(), refresh_universe(), heartbeat())


if __name__ == "__main__":
    print(f"[{now_str()}] 异动雷达启动中...", flush=True)
    asyncio.run(main())

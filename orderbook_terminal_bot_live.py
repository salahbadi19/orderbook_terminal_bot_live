#!/usr/bin/env python3
"""
OrderBook Terminal Bot — Live (Binance)
- Maintains local L2 orderbook from snapshot + depth websocket updates.
- Consumes aggTrade stream for time&sales.
- Signal engine: imbalance + sweep detection + persistence + basic spoof detection.
- Risk manager and optional execution (signed REST).
- Terminal UI with rich.
- Start in simulation mode; verify thoroughly before enabling live execution.
"""

from __future__ import annotations
import asyncio, aiohttp, websockets, time, hmac, hashlib, json, os, math, logging
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

# Try import rich
try:
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich.console import Console
    from rich import box
    from rich.text import Text
except Exception:
    raise SystemExit("Please install required libs: pip install rich aiohttp websockets requests python-dotenv")

# --- CONFIG ---
CONFIG = {
    "symbols": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT", "LTCUSDT", "ADAUSDT", "XRPUSDT", "MATICUSDT", "ATOMUSDT"],
    "default_symbol": "BTCUSDT",
    "depth_limit": 500,
    "top_n_levels": 20,
    "imbalance_threshold": 3.0,
    "sweep_percent_threshold": 0.20,
    "sweep_window_s": 1.5,
    "persistence_ms": 600,
    "large_order_buckets": [10000, 5000, 1000, 500, 100, 10, 0],
    "max_position_risk_percent": 1.0,
    "account_balance_usd": 10000.0,
    "min_confidence_for_exec": 0.75,
    "simulate_execution": True,
    "binance_rest_base": "https://api.binance.com",
    "binance_ws_base": "wss://stream.binance.com:9443/ws",
    "log_level": "INFO",
}

logging.basicConfig(level=getattr(logging, CONFIG["log_level"].upper(), logging.INFO))
log = logging.getLogger("OBot")

# load API keys from env if provided (do NOT paste here)
API_KEY = os.getenv("C24jE1GzXuk5fnBOrSGRhqkfUcynEEjomFIH3TSyFSwgc5n1zg2ezL1CC8LeanUo", "")
API_SECRET = os.getenv("ZbpCUlYg1zfjqH8BDD0gj5MBQnCdObmrNsgzOYhRP9WAZ2OHDR5SBv2rgk86lfgC", "")

# --- Data Structures ---
@dataclass
class Level:
    price: float
    size: float

@dataclass
class OrderBook:
    symbol: str
    bids: OrderedDict = field(default_factory=OrderedDict)  # price -> size
    asks: OrderedDict = field(default_factory=OrderedDict)
    last_update_id: Optional[int] = None
    # For depth diff buffer before snapshot applied:
    pending_updates: List[dict] = field(default_factory=list)
    snapshot_applied: bool = False

    def apply_snapshot(self, snapshot: dict):
        """snapshot: {"lastUpdateId": int, "bids": [[price,qty]], "asks": [[price,qty]]}"""
        self.last_update_id = snapshot.get("lastUpdateId")
        self.bids.clear(); self.asks.clear()
        for p,q in snapshot.get("bids", [])[:CONFIG["depth_limit"]]:
            self.bids[float(p)] = float(q)
        for p,q in snapshot.get("asks", [])[:CONFIG["depth_limit"]]:
            self.asks[float(p)] = float(q)
        self._sort_books()
        self.snapshot_applied = True
        # apply pending updates that are newer than snapshot.lastUpdateId
        pending = sorted(self.pending_updates, key=lambda u: u.get("u",0))
        for up in pending:
            if up.get("u") and up["u"] > self.last_update_id:
                self.apply_diff(up)
        self.pending_updates.clear()
        log.info(f"[{self.symbol}] Snapshot applied (lastUpdateId={self.last_update_id})")

    def apply_diff(self, update: dict):
        """
        update is Binance format: {'e':'depthUpdate','E':..., 's':symbol, 'U': firstUpdateId, 'u': finalUpdateId, 'b':[[p,q]], 'a':[[p,q]]}
        Must ensure update.U <= last_update_id+1 <= update.u (per Binance docs) when integrating.
        """
        if not self.snapshot_applied:
            self.pending_updates.append(update)
            return
        # Simple ordering check
        u = update.get("u")
        U = update.get("U")
        if self.last_update_id is None:
            # unexpected; keep update buffered
            self.pending_updates.append(update)
            return
        if U <= self.last_update_id + 1 <= u:
            # apply
            for p_str,q_str in update.get("b", []):
                price = float(p_str); qty = float(q_str)
                if qty == 0:
                    self.bids.pop(price, None)
                else:
                    self.bids[price] = qty
            for p_str,q_str in update.get("a", []):
                price = float(p_str); qty = float(q_str)
                if qty == 0:
                    self.asks.pop(price, None)
                else:
                    self.asks[price] = qty
            self.last_update_id = u
            self._trim_depth()
            self._sort_books()
        else:
            # missed update gap -> need to re-sync snapshot
            log.warning(f"[{self.symbol}] Update gap detected (U={U} last_update_id={self.last_update_id}). Re-sync required.")
            self.snapshot_applied = False
            # consumer should trigger snapshot re-fetch

    def _trim_depth(self):
        # keep at most depth_limit on each side
        if len(self.bids) > CONFIG["depth_limit"]:
            # remove worst bids (lowest prices)
            keys = sorted(self.bids.keys(), reverse=True)
            for p in keys[CONFIG["depth_limit"]:]:
                self.bids.pop(p, None)
        if len(self.asks) > CONFIG["depth_limit"]:
            keys = sorted(self.asks.keys())
            for p in keys[CONFIG["depth_limit"]:]:
                self.asks.pop(p, None)

    def _sort_books(self):
        self.bids = OrderedDict(sorted(self.bids.items(), key=lambda x: x[0], reverse=True))
        self.asks = OrderedDict(sorted(self.asks.items(), key=lambda x: x[0]))

    def top_n(self, n:int=10) -> Tuple[List[Level], List[Level]]:
        bids = [Level(p,s) for p,s in list(self.bids.items())[:n]]
        asks = [Level(p,s) for p,s in list(self.asks.items())[:n]]
        return bids, asks

    def sum_volume_top_n(self, n:int=20, side="bid") -> float:
        if side=="bid":
            return sum([s for p,s in list(self.bids.items())[:n]])
        return sum([s for p,s in list(self.asks.items())[:n]])

# --- Utils ---
def now_ms() -> int:
    return int(time.time()*1000)

def usd_value(symbol: str, price: float, qty: float) -> float:
    # naive: for USDT pairs price is USD; else user should convert externally
    return price * qty

# --- Signal Engine ---
@dataclass
class TradeTick:
    ts: float
    price: float
    qty: float
    is_buyer_maker: bool

class SignalEngine:
    def __init__(self, ob: OrderBook):
        self.ob = ob
        self.trades: deque = deque(maxlen=2000)
        self.eaten_events: deque = deque()  # (ts, usd)
        self.last_signal_ts = 0.0

    def ingest_trade(self, trade_event: dict):
        """
        trade_event (aggTrade) format: { "e": "aggTrade", "E": eventTime, "s": "BTCUSDT",
           "a": aggregateTradeId, "p": "0.001", "q": "1", "f": firstId, "l": lastId, "T": tradeTime, "m": True, "M": True }
        """
        t = TradeTick(ts=trade_event["T"]/1000.0, price=float(trade_event["p"]), qty=float(trade_event["q"]), is_buyer_maker=trade_event["m"])
        self.trades.append(t)
        usd = usd_value(self.ob.symbol, t.price, t.qty)
        self.eaten_events.append((t.ts, usd))
        # Also, optionally reduce book sizes when trades occur (best-effort)
        # We will not mutate book aggressively here; rely on depth updates.

    def compute_imbalance(self, n:int=None) -> float:
        n = n or CONFIG["top_n_levels"]
        bid_vol = self.ob.sum_volume_top_n(n, "bid")
        ask_vol = self.ob.sum_volume_top_n(n, "ask")
        if ask_vol == 0:
            return float("inf") if bid_vol>0 else 1.0
        return bid_vol/ask_vol

    def compute_sweep(self, window_s: float=None) -> Tuple[float,float]:
        window_s = window_s or CONFIG["sweep_window_s"]
        cutoff = time.time() - window_s
        eaten = [usd for ts,usd in self.eaten_events if ts >= cutoff]
        total_eaten = sum(eaten) if eaten else 0.0
        bids, asks = self.ob.top_n(CONFIG["top_n_levels"])
        top_asks_usd = sum([usd_value(self.ob.symbol, lv.price, lv.size) for lv in asks])
        top_bids_usd = sum([usd_value(self.ob.symbol, lv.price, lv.size) for lv in bids])
        top_total = max(1.0, top_asks_usd + top_bids_usd)
        percent = total_eaten / top_total
        return percent, total_eaten

    def detect_spoofing(self) -> bool:
        # heuristic: if we see many large-limit orders appear and vanish without trades in short time
        # For production, keep a history of diffs; here basic placeholder returns False
        return False

    def generate(self) -> Optional[dict]:
        imbalance = self.compute_imbalance()
        sweep_pct, eaten_usd = self.compute_sweep()
        recent = list(self.trades)[-20:]
        if not recent:
            return None
        buy_aggr = sum(1 for t in recent if not t.is_buyer_maker)
        sell_aggr = sum(1 for t in recent if t.is_buyer_maker)
        total = max(1, buy_aggr + sell_aggr)
        buy_ratio = buy_aggr/total; sell_ratio = sell_aggr/total

        score = 0.0; comps = []
        if imbalance >= CONFIG["imbalance_threshold"]:
            score += 0.35 * min(1.0, imbalance/(CONFIG["imbalance_threshold"]*2))
            comps.append(f"imbalance={imbalance:.2f}")
        if sweep_pct >= CONFIG["sweep_percent_threshold"] and buy_ratio > 0.55:
            score += 0.45 * min(1.0, sweep_pct/(CONFIG["sweep_percent_threshold"]*2))
            comps.append(f"sweep={sweep_pct*100:.1f}%")
        if buy_ratio > 0.6:
            score += 0.15 * ((buy_ratio-0.6)/0.4 + 1)
            comps.append(f"buy_ratio={buy_ratio:.2f}")
        if sell_ratio > 0.6:
            score += 0.15 * ((sell_ratio-0.6)/0.4 + 1)
            comps.append(f"sell_ratio={sell_ratio:.2f}")

        if score <= 0.12:
            return None
        side = "BUY" if buy_ratio > sell_ratio else "SELL"
        confidence = max(0.0, min(1.0, score))
        # cooldown
        if time.time() - self.last_signal_ts < 0.5:
            return None
        if self.detect_spoofing():
            log.debug("spoofing detected -> skip")
            return None
        self.last_signal_ts = time.time()
        return {
            "symbol": self.ob.symbol,
            "side": side,
            "confidence": confidence,
            "components": comps,
            "imbalance": imbalance,
            "sweep_pct": sweep_pct,
            "eaten_usd": eaten_usd,
            "ts": time.time()
        }

# --- Risk Manager ---
class RiskManager:
    def __init__(self, balance_usd: float):
        self.balance = balance_usd
        self.positions: Dict[str, dict] = {}

    def position_size_usd(self, stop_distance_usd: float, confidence: float) -> float:
        # risk = balance * max_risk_percent% * confidence
        allowed_risk = self.balance * (CONFIG["max_position_risk_percent"]/100.0) * max(0.2, confidence)
        if stop_distance_usd <= 0:
            return 0.0
        return allowed_risk / stop_distance_usd

    def can_open(self, symbol: str, size_usd: float) -> bool:
        total = sum(p["usd"] for p in self.positions.values()) if self.positions else 0.0
        return (total + size_usd) <= 0.1 * self.balance

    def open_position(self, symbol: str, side: str, size_usd: float, entry_price: float, stop_price: float):
        self.positions[symbol] = {"side": side, "usd": size_usd, "entry": entry_price, "stop": stop_price, "opened_at": time.time()}
        log.info(f"Opened {symbol} {side} ${size_usd:.2f} entry={entry_price} stop={stop_price}")

    def close_position(self, symbol: str):
        if symbol in self.positions:
            p = self.positions.pop(symbol)
            log.info(f"Closed position {symbol}: {p}")

# --- Execution Adapter (REST signed) ---
class ExecutionAdapter:
    def __init__(self, api_key: str, api_secret: str, simulate: bool=True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.simulate = simulate
        self.session = aiohttp.ClientSession()

    async def place_market_order(self, symbol: str, side: str, qty: float) -> dict:
        if self.simulate or not self.api_key:
            await asyncio.sleep(0.05)
            log.info(f"[SIM] market {side} {symbol} qty={qty:.6g}")
            return {"status":"FILLED", "filledQty": qty, "avgPrice": None}
        # else sign and send POST /api/v3/order with type=MARKET
        ts = int(time.time()*1000)
        params = f"symbol={symbol}&side={side}&type=MARKET&quantity={qty}&timestamp={ts}"
        sig = hmac.new(self.api_secret.encode(), params.encode(), hashlib.sha256).hexdigest()
        url = f"{CONFIG['binance_rest_base']}/api/v3/order?{params}&signature={sig}"
        headers = {"X-MBX-APIKEY": self.api_key}
        async with self.session.post(url, headers=headers) as resp:
            return await resp.json()

    async def close(self):
        await self.session.close()

# --- Terminal UI ---
class TerminalUI:
    def __init__(self, ob: OrderBook, engine: SignalEngine, risk: RiskManager):
        self.console = Console()
        self.ob = ob; self.engine = engine; self.risk = risk
        self.table_height = 16
        self.last_signal = None

    def orderbook_table(self):
        bids, asks = self.ob.top_n(self.table_height)
        table = Table(title=f"Order Book: {self.ob.symbol}", box=box.MINIMAL)
        table.add_column("BID Price", justify="right"); table.add_column("Size", justify="right")
        table.add_column("", justify="center")
        table.add_column("ASK Price", justify="left"); table.add_column("Size", justify="left")
        rows = max(len(bids), len(asks))
        for i in range(rows):
            bid = bids[i] if i < len(bids) else None
            ask = asks[i] if i < len(asks) else None
            table.add_row(
                f"{bid.price:.2f}" if bid else "",
                f"{bid.size:.6g}" if bid else "",
                "",
                f"{ask.price:.2f}" if ask else "",
                f"{ask.size:.6g}" if ask else ""
            )
        return table

    def signal_panel(self):
        txt = Text()
        if self.last_signal:
            s = self.last_signal
            txt.append(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(s['ts']))}\n", style="bold")
            color = "green" if s["side"]=="BUY" else "red"
            txt.append(f"{s['side']} | confidence={s['confidence']:.2f}\n", style=f"bold {color}")
            txt.append(", ".join(s.get("components", [])) + "\n")
            txt.append(f"imbalance={s['imbalance']:.2f} sweep={s['sweep_pct']*100:.2f}% eaten_usd=${s['eaten_usd']:.2f}\n")
        else:
            txt.append("No signal", style="dim")
        return Panel(txt, title="Last Signal")

    def risk_panel(self):
        txt = Text()
        txt.append(f"Balance: ${self.risk.balance:.2f}\n")
        txt.append(f"Open positions: {len(self.risk.positions)}\n")
        for k,v in self.risk.positions.items():
            txt.append(f"{k}: {v['side']} ${v['usd']:.2f} entry={v['entry']}\n")
        return Panel(txt, title="Risk")

    async def live(self, signal_queue: asyncio.Queue):
        with Live(refresh_per_second=4, console=self.console) as live:
            while True:
                try:
                    # non-blocking get
                    if not signal_queue.empty():
                        self.last_signal = await signal_queue.get()
                    grid = Table.grid(expand=True)
                    grid.add_row(self.orderbook_table())
                    grid.add_row(self.signal_panel(), self.risk_panel())
                    live.update(grid)
                    await asyncio.sleep(0.2)
                except asyncio.CancelledError:
                    break

# --- Binance connectivity utilities ---
async def fetch_snapshot(session: aiohttp.ClientSession, symbol: str, limit: int=CONFIG["depth_limit"]) -> dict:
    url = f"{CONFIG['binance_rest_base']}/api/v3/depth?symbol={symbol}&limit={limit}"
    async with session.get(url) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Snapshot error {resp.status}")
        return await resp.json()

# Websocket handler per symbol: subscribes to depth and aggTrade combined stream using multiplex (stream path)
async def run_symbol(symbol: str, engine_map: Dict[str, SignalEngine], ob_map: Dict[str, OrderBook],
                     ui_queue_map: Dict[str, asyncio.Queue], exec_adapter: ExecutionAdapter, risk: RiskManager):
    # we'll use multiplex stream endpoint to get both depth and aggTrade: /stream?streams=btcusdt@depth@100ms/btcusdt@aggTrade
    stream_name_depth = f"{symbol.lower()}@depth@100ms"
    stream_name_agg = f"{symbol.lower()}@aggTrade"
    stream_path = f"/stream?streams={stream_name_depth}/{stream_name_agg}"
    ws_url = CONFIG["binance_ws_base"].replace("/ws","") + stream_path  # full URL to /stream?streams=...
    ob = ob_map[symbol]
    engine = engine_map[symbol]
    queue = ui_queue_map[symbol]

    async with aiohttp.ClientSession() as session:
        # first fetch snapshot
        try:
            snapshot = await fetch_snapshot(session, symbol, limit=CONFIG["depth_limit"])
            ob.apply_snapshot(snapshot)
        except Exception as e:
            log.error(f"[{symbol}] Snapshot fetch failed: {e}")
            return

        # now open websocket and process messages
        async with session.ws_connect(ws_url) as ws:
            log.info(f"[{symbol}] Connected websocket to {ws_url}")
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    payload = json.loads(msg.data)
                    # multiplex wrapper: payload = {"stream":"btcusdt@depth","data":{...}}
                    stream = payload.get("stream","")
                    data = payload.get("data", payload)
                    if stream.endswith("@depth@100ms") or data.get("e") == "depthUpdate":
                        # depth update structure: U, u, b, a
                        if not ob.snapshot_applied:
                            # buffer inside ob
                            ob.apply_diff(data)
                        else:
                            ob.apply_diff(data)
                    elif stream.endswith("@aggTrade") or data.get("e") == "aggTrade":
                        # pass trade to engine
                        engine.ingest_trade(data)
                    # after each message we can check signal
                    sig = engine.generate()
                    if sig:
                        await queue.put(sig)
                        log.info(f"[{symbol}] SIG: {sig}")
                        # risk sizing and possible execution
                        # choose stop distance as e.g., 0.3% of mid-price
                        bids, asks = ob.top_n(1)
                        if bids and asks:
                            mid = (bids[0].price + asks[0].price)/2.0
                        else:
                            mid = (bids[0].price if bids else asks[0].price) if (bids or asks) else 0.0
                        stop_distance = 0.003 * mid if mid>0 else 0.0
                        size_usd = risk.position_size_usd(stop_distance, sig["confidence"])
                        if size_usd > 0 and risk.can_open(symbol, size_usd) and sig["confidence"] >= CONFIG["min_confidence_for_exec"]:
                            # compute qty for market order (USD->qty)
                            qty = size_usd / mid if mid>0 else 0.0
                            # basic min qty rounding could be added per asset filters
                            if qty > 0:
                                res = await exec_adapter.place_market_order(symbol, sig["side"], qty)
                                if res.get("status") == "FILLED" or res.get("filledQty"):
                                    stop_price = mid - stop_distance if sig["side"]=="BUY" else mid + stop_distance
                                    risk.open_position(symbol, sig["side"], size_usd, mid, stop_price)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    log.error(f"[{symbol}] Websocket error: {msg}")
                    break

# --- Orchestrator ---
async def main(symbol: str = CONFIG["default_symbol"]):
    # prepare objects
    ob = OrderBook(symbol)
    engine = SignalEngine(ob)
    risk = RiskManager(CONFIG["account_balance_usd"])
    exec_adapter = ExecutionAdapter(API_KEY, API_SECRET, simulate=CONFIG["simulate_execution"])

    ui_queue = asyncio.Queue()
    ui = TerminalUI(ob, engine, risk)

    # run websocket loop for single symbol (for multiple symbols spawn multiple tasks)
    ws_task = asyncio.create_task(run_symbol(symbol, {symbol: engine}, {symbol: ob}, {symbol: ui_queue}, exec_adapter, risk))
    ui_task = asyncio.create_task(ui.live(ui_queue))

    try:
        await asyncio.gather(ws_task, ui_task)
    except asyncio.CancelledError:
        pass
    finally:
        await exec_adapter.close()

if __name__ == "__main__":
    # usage: ensure environment variables BINANCE_API_KEY and BINANCE_API_SECRET set if you want real execution
    # recommended: start with CONFIG["simulate_execution"]=True
    try:
        sym = CONFIG["default_symbol"]
        asyncio.run(main(sym))
    except KeyboardInterrupt:
        print("Stopped by user")

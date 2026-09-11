#!/usr/bin/env python3
"""
SweepScan - liquidity sweep + displacement + FVG scanner.

Single file, no dependencies. Double-click to open the app, or run:
    python sweepscan_app.py            # GUI
    python sweepscan_app.py --cli      # console mode
    python sweepscan_app.py --netcheck # test which data sources you can reach

Watches the pairs you choose and alerts when this forms:
  1. price runs a prior swing high/low (takes liquidity)
  2. a big candle closes back through that level (displacement)
  3. the move leaves a fair value gap

It only watches and tells you. It never places a trade.
"""
import argparse
import csv
import json
import os
import queue
import re
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple

APP_NAME = "SweepScan"
CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".sweepscan.json")

# ==========================================================================
# 1. DETECTION ENGINE
# ==========================================================================

@dataclass
class Signal:
    symbol: str
    timeframe: str
    direction: str
    time: Any
    swept_level: float
    sweep_index: int
    displacement_index: int
    displacement_atr_mult: float
    broke_structure_at: Optional[float]
    fvg_top: float
    fvg_bottom: float
    fvg_midpoint: float
    fvg_size: float
    close: float
    atr_value: float = 0.0
    pattern: str = "reversal"        # "reversal" (sweep + structure shift) or "breakout" (sweep + continuation)
    extreme: float = 0.0             # furthest price reached during the displacement leg
    entry_level: Optional[float] = None  # retracement price to watch for the actual entry (reversal only)
    entry_pct: float = 0.5           # how far back toward the swept structure entry_level sits

    def as_dict(self):
        return asdict(self)

    def confidence(self):
        return confidence_score(self.as_dict())

    def summary(self) -> str:
        label, score = self.confidence()
        lines = [
            f"{self.symbol} {self.timeframe} | {self.direction.upper()} sweep + displacement"
            f"{' (breakout)' if self.pattern == 'breakout' else ''}",
            f"  confidence  : {label} ({score}/100)",
            f"  swept level : {self.swept_level:.6g}",
            f"  displacement: {self.displacement_atr_mult:.1f}x ATR",
            f"  FVG zone    : {self.fvg_bottom:.6g} - {self.fvg_top:.6g}",
            f"  FVG 50%     : {self.fvg_midpoint:.6g}",
        ]
        if self.pattern == "breakout":
            # the recent range's opposite boundary the displacement candle
            # broke through (a "ceiling" for bullish, a "floor" for bearish)
            # - this was already computed but never shown in the message.
            label = "ceiling" if self.direction == "bullish" else "floor"
            lines.append(f"  {label:<12}: {self.broke_structure_at:.6g}")
        if self.entry_level is not None:
            lines.append(f"  watching for: entry near {self.entry_level:.6g} "
                          f"({self.entry_pct*100:.0f}% back toward {self.broke_structure_at:.6g})")
        lines.append(f"  last close  : {self.close:.6g}")
        return "\n".join(lines)


def entry_summary(p: dict) -> str:
    """Text for the second-stage alert: price has traded back to the
    watched retracement level and the entry is live."""
    label, score = confidence_score(p)
    pct = p.get("entry_pct") or 0.5
    is_breakout = p.get("pattern") == "breakout"
    tag = "BREAKOUT ENTRY" if is_breakout else "ENTRY"
    lines = [
        f"{p['symbol']} {p['timeframe']} | {p['direction'].upper()} {tag} "
        f"({pct*100:.0f}% retest)",
        f"  confidence     : {label} ({score}/100)",
        f"  entry level    : {p['entry_level']:.6g}",
        f"  swept level    : {p['swept_level']:.6g}",
    ]
    if p.get("broke_structure_at") is not None:
        struct_label = ("ceiling" if p.get("direction") == "bullish" else "floor") \
            if is_breakout else "broke structure"
        lines.append(f"  {struct_label:<15}: {p['broke_structure_at']:.6g}")
    lines += [
        f"  FVG zone       : {p['fvg_bottom']:.6g} - {p['fvg_top']:.6g}",
        f"  displacement   : {p['displacement_atr_mult']:.1f}x ATR",
        f"  price now      : {p['close']:.6g}",
    ]
    return "\n".join(lines)


def confidence_score(p: dict) -> Tuple[str, int]:
    """Heuristic 0-100 confidence score + label (STRONG/MEDIUM/WEAK) built
    purely from a signal's own numbers - no lookahead, no history needed.

    Three ingredients, each measured in units of that signal's own ATR so
    they're comparable across symbols with wildly different price scales
    (EUR/USD at ~1.16 vs BTC/USDT at ~78000):
      - how big the displacement candle was relative to normal volatility
      - how large the FVG (the imbalance left behind) is relative to ATR
      - how decisively price closed through the broken structure level
    """
    a = p.get("atr_value") or 0.0
    disp = min((p.get("displacement_atr_mult") or 0.0) / 3.0, 1.0)
    if a > 0:
        fvg = min((p.get("fvg_size") or 0.0) / a / 1.2, 1.0)
        broke = p.get("broke_structure_at")
        close = p.get("close")
        if broke is not None and close is not None:
            struct = min(abs(close - broke) / a, 1.0)
        else:
            struct = 0.5
    else:
        fvg = 0.5
        struct = 0.5
    score = round(100 * (0.45 * disp + 0.30 * fvg + 0.25 * struct))
    score = max(0, min(100, score))
    label = "STRONG" if score >= 70 else "MEDIUM" if score >= 40 else "WEAK"
    return label, score


def _epoch(t):
    """Normalize a candle's `time` (usually a datetime, sometimes an int/str
    timestamp depending on the feed) to a plain float so it can be compared
    even after a round-trip through JSON (see save_state/load_state) - a
    raw datetime object doesn't survive that round-trip as something you
    can still compare with `>`."""
    if hasattr(t, "timestamp"):
        try:
            return t.timestamp()
        except Exception:
            pass
    try:
        return float(t)
    except Exception:
        return 0.0


def atr(candles, period=14, end=None):
    end = len(candles) if end is None else end
    start = max(1, end - period)
    trs = []
    for i in range(start, end):
        pc = candles[i - 1]["close"]
        trs.append(max(candles[i]["high"] - candles[i]["low"],
                       abs(candles[i]["high"] - pc),
                       abs(candles[i]["low"] - pc)))
    return sum(trs) / len(trs) if trs else 0.0


def swing_highs(candles, left=2, right=2):
    out = []
    for i in range(left, len(candles) - right):
        h = candles[i]["high"]
        if all(h > candles[i - k]["high"] for k in range(1, left + 1)) and \
           all(h > candles[i + k]["high"] for k in range(1, right + 1)):
            out.append(i)
    return out


def swing_lows(candles, left=2, right=2):
    out = []
    for i in range(left, len(candles) - right):
        l = candles[i]["low"]
        if all(l < candles[i - k]["low"] for k in range(1, left + 1)) and \
           all(l < candles[i + k]["low"] for k in range(1, right + 1)):
            out.append(i)
    return out


def find_bearish_fvg(candles, i):
    if i - 1 < 0 or i + 1 >= len(candles):
        return None
    top, bottom = candles[i - 1]["low"], candles[i + 1]["high"]
    return (bottom, top) if top > bottom else None


def find_bullish_fvg(candles, i):
    if i - 1 < 0 or i + 1 >= len(candles):
        return None
    bottom, top = candles[i - 1]["high"], candles[i + 1]["low"]
    return (bottom, top) if top > bottom else None


def _fvg_near_level(level, fvg_bottom, fvg_top, atr_value, mult):
    """Is `level` (a swing high/low) inside the FVG, or at least close to it?
    Used to invalidate a breakout setup whose FVG formed nowhere near the
    level price is actually going to retrace back to - a technically-valid
    sweep+displacement+FVG combo where the FVG and the broken structure
    aren't part of the same move isn't the pattern this is meant to catch.
    Distance is measured in units of the signal's own ATR so the tolerance
    scales sensibly across symbols with very different price levels."""
    if fvg_bottom <= level <= fvg_top:
        return True
    if atr_value <= 0:
        return False
    dist = min(abs(level - fvg_bottom), abs(level - fvg_top))
    return dist <= mult * atr_value


def detect(candles, symbol="", timeframe="", lookback=60, swing_left=2,
           swing_right=2, sweep_window=3, min_displacement_atr=1.5,
           min_body_ratio=0.55, require_structure_break=True, atr_period=14,
           entry_retracement_pct=0.5):
    signals = []
    n = len(candles)
    if n < atr_period + swing_left + swing_right + 3:
        return signals

    highs = swing_highs(candles, swing_left, swing_right)
    lows = swing_lows(candles, swing_left, swing_right)
    start = max(swing_left + 1, n - lookback)

    for i in range(start, n - 1):
        a = atr(candles, atr_period, end=i)
        if a <= 0:
            continue
        c = candles[i]
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        body = abs(c["close"] - c["open"])
        if body / rng < min_body_ratio:
            continue
        mult = body / a
        if mult < min_displacement_atr:
            continue

        if c["close"] < c["open"]:   # bearish
            # swept = the swing HIGH whose buyside liquidity got taken (the
            # "swing low -> swing high" leg's top). high_idx is WHICH swing
            # high that was, so the structure-break check below is tied to
            # THIS SAME leg's swing low - not just any low anywhere in the
            # lookback window. Without that link, the sweep and the "break"
            # can be two unrelated pivots from different parts of the chart
            # (e.g. a noisy 5m forex chart), which technically passes both
            # checks but isn't the single low->high->back-below-the-low
            # zigzag the model actually describes.
            swept, sidx, high_idx = None, None, None
            for j in range(max(0, i - sweep_window), i + 1):
                for h in [x for x in highs if x < j]:
                    lvl = candles[h]["high"]
                    if candles[j]["high"] > lvl and c["close"] < lvl:
                        if swept is None or lvl > swept:
                            swept, sidx, high_idx = lvl, j, h
            if swept is None:
                continue
            broke = None
            if require_structure_break:
                prior_lows = [x for x in lows if x < high_idx]
                if prior_lows:
                    low_idx = prior_lows[-1]  # the swing low that formed right
                                               # before this specific swept high
                    lvl = candles[low_idx]["low"]
                    if c["close"] < lvl and lvl < swept:
                        broke = lvl
                if broke is None:
                    continue
            fvg = find_bearish_fvg(candles, i)
            if fvg:
                b, t = fvg
                extreme = min(x["low"] for x in candles[sidx:i + 1])
                entry_level = extreme + entry_retracement_pct * (broke - extreme)
                signals.append(Signal(symbol, timeframe, "bearish", c["time"],
                                      swept, sidx, i, mult, broke, t, b,
                                      (t + b) / 2, t - b, candles[-1]["close"],
                                      atr_value=a, pattern="reversal", extreme=extreme,
                                      entry_level=entry_level, entry_pct=entry_retracement_pct))
        else:                        # bullish (mirror of the bearish case above -
            # swept = the swing LOW whose sellside liquidity got taken; the
            # structure break must be back above the SAME leg's swing high)
            swept, sidx, low_idx = None, None, None
            for j in range(max(0, i - sweep_window), i + 1):
                for l in [x for x in lows if x < j]:
                    lvl = candles[l]["low"]
                    if candles[j]["low"] < lvl and c["close"] > lvl:
                        if swept is None or lvl < swept:
                            swept, sidx, low_idx = lvl, j, l
            if swept is None:
                continue
            broke = None
            if require_structure_break:
                prior_highs = [x for x in highs if x < low_idx]
                if prior_highs:
                    high_idx = prior_highs[-1]  # the swing high that formed
                                                 # right before this swept low
                    lvl = candles[high_idx]["high"]
                    if c["close"] > lvl and lvl > swept:
                        broke = lvl
                if broke is None:
                    continue
            fvg = find_bullish_fvg(candles, i)
            if fvg:
                b, t = fvg
                extreme = max(x["high"] for x in candles[sidx:i + 1])
                entry_level = extreme + entry_retracement_pct * (broke - extreme)
                signals.append(Signal(symbol, timeframe, "bullish", c["time"],
                                      swept, sidx, i, mult, broke, t, b,
                                      (t + b) / 2, t - b, candles[-1]["close"],
                                      atr_value=a, pattern="reversal", extreme=extreme,
                                      entry_level=entry_level, entry_pct=entry_retracement_pct))
    return signals


def detect_breakout(candles, symbol="", timeframe="", lookback=60, swing_left=2,
                     swing_right=2, sweep_window=3, min_displacement_atr=1.5,
                     min_body_ratio=0.55, atr_period=14, range_window=20,
                     entry_retracement_pct=0.5, fvg_near_structure_atr=2.0):
    """Second pattern: sweep the floor/ceiling of a recent range, then an
    aggressive candle CONTINUES out of the range (no reversal / no opposite
    structure break required) - the stop-hunt-then-breakout setup, as
    opposed to detect()'s sweep-then-reversal setup.

    Same two-stage retest behavior as detect()'s reversal setups: this
    doesn't fire the moment the breakout happens, it computes an
    entry_level - the same halfway-back-to-the-broken-level math used for
    reversals - and the caller (scan_once) arms it and waits for price to
    actually retrace there before alerting.

    A setup is only valid if the FVG the displacement candle left behind
    actually sits near the level being broken (see _fvg_near_level) - a
    breakout whose FVG formed somewhere unrelated to the level price will
    eventually retest isn't the same setup, even if the raw sweep+
    displacement+FVG checks all technically pass."""
    signals = []
    n = len(candles)
    if n < atr_period + swing_left + swing_right + 3:
        return signals

    highs = swing_highs(candles, swing_left, swing_right)
    lows = swing_lows(candles, swing_left, swing_right)
    start = max(swing_left + 1, n - lookback)

    for i in range(start, n - 1):
        a = atr(candles, atr_period, end=i)
        if a <= 0:
            continue
        c = candles[i]
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        body = abs(c["close"] - c["open"])
        if body / rng < min_body_ratio:
            continue
        mult = body / a
        if mult < min_displacement_atr:
            continue

        recent = max(0, i - range_window)

        if c["close"] > c["open"]:   # bullish: sweep the range floor, break the range ceiling
            swept, sidx = None, None
            for j in range(max(recent, i - sweep_window), i + 1):
                for l in [x for x in lows if recent <= x < j]:
                    lvl = candles[l]["low"]
                    if candles[j]["low"] < lvl and c["close"] > lvl:
                        if swept is None or lvl < swept:
                            swept, sidx = lvl, j
            if swept is None:
                continue
            ceiling = None
            for h in reversed([x for x in highs if recent <= x < i]):
                if c["close"] > candles[h]["high"]:
                    ceiling = candles[h]["high"]
                    break
            if ceiling is None:
                continue
            fvg = find_bullish_fvg(candles, i)
            if fvg:
                b, t = fvg
                if not _fvg_near_level(ceiling, b, t, a, fvg_near_structure_atr):
                    continue
                extreme = max(x["high"] for x in candles[sidx:i + 1])
                entry_level = extreme + entry_retracement_pct * (ceiling - extreme)
                signals.append(Signal(symbol, timeframe, "bullish", c["time"],
                                      swept, sidx, i, mult, ceiling, t, b,
                                      (t + b) / 2, t - b, candles[-1]["close"],
                                      atr_value=a, pattern="breakout", extreme=extreme,
                                      entry_level=entry_level, entry_pct=entry_retracement_pct))
        else:                         # bearish: sweep the range ceiling, break the range floor
            swept, sidx = None, None
            for j in range(max(recent, i - sweep_window), i + 1):
                for h in [x for x in highs if recent <= x < j]:
                    lvl = candles[h]["high"]
                    if candles[j]["high"] > lvl and c["close"] < lvl:
                        if swept is None or lvl > swept:
                            swept, sidx = lvl, j
            if swept is None:
                continue
            floor = None
            for l in reversed([x for x in lows if recent <= x < i]):
                if c["close"] < candles[l]["low"]:
                    floor = candles[l]["low"]
                    break
            if floor is None:
                continue
            fvg = find_bearish_fvg(candles, i)
            if fvg:
                b, t = fvg
                if not _fvg_near_level(floor, b, t, a, fvg_near_structure_atr):
                    continue
                extreme = min(x["low"] for x in candles[sidx:i + 1])
                entry_level = extreme + entry_retracement_pct * (floor - extreme)
                signals.append(Signal(symbol, timeframe, "bearish", c["time"],
                                      swept, sidx, i, mult, floor, t, b,
                                      (t + b) / 2, t - b, candles[-1]["close"],
                                      atr_value=a, pattern="breakout", extreme=extreme,
                                      entry_level=entry_level, entry_pct=entry_retracement_pct))
    return signals


# ==========================================================================
# 2. DATA FEEDS
# ==========================================================================

class FeedError(Exception):
    pass


class PublicFeed:
    """Public candle data, standard library only. Aggregators are listed
    first because they're usually reachable where exchange domains are
    blocked."""

    ORDER = ["yahoo", "cryptocompare", "bybit", "okx", "kraken", "coinbase", "binance"]

    SPECS = {
        "yahoo": {"url": "https://query1.finance.yahoo.com/v8/finance/chart/{sym}",
                   "tf": {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
                           "1h": "1h", "4h": "1h", "1d": "1d"}},
        "cryptocompare": {"url": "https://min-api.cryptocompare.com/data/v2/{endpoint}",
                           "tf": {"1m": ("histominute", 1), "5m": ("histominute", 5),
                                   "15m": ("histominute", 15), "30m": ("histominute", 30),
                                   "1h": ("histohour", 1), "4h": ("histohour", 4),
                                   "1d": ("histoday", 1)}},
        "binance": {"url": "https://api.binance.com/api/v3/klines",
                     "tf": {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
                             "1h": "1h", "4h": "4h", "1d": "1d"}},
        "bybit": {"url": "https://api.bybit.com/v5/market/kline",
                   "tf": {"1m": "1", "5m": "5", "15m": "15", "30m": "30",
                           "1h": "60", "4h": "240", "1d": "D"}},
        "okx": {"url": "https://www.okx.com/api/v5/market/candles",
                 "tf": {"1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
                         "1h": "1H", "4h": "4H", "1d": "1D"}},
        "kraken": {"url": "https://api.kraken.com/0/public/OHLC",
                    "tf": {"1m": "1", "5m": "5", "15m": "15", "30m": "30",
                            "1h": "60", "4h": "240", "1d": "1440"}},
        "coinbase": {"url": "https://api.exchange.coinbase.com/products/{sym}/candles",
                      "tf": {"1m": "60", "5m": "300", "15m": "900",
                              "1h": "3600", "4h": "21600", "1d": "86400"}},
    }

    TF_ALIAS = {"m1": "1m", "1m": "1m", "m5": "5m", "5m": "5m",
                "m15": "15m", "15m": "15m", "m30": "30m", "30m": "30m",
                "h1": "1h", "1h": "1h", "h4": "4h", "4h": "4h",
                "d1": "1d", "1d": "1d"}

    def __init__(self, exchange="auto", timeout=20, log=None):
        self.requested = (exchange or "auto").lower()
        self.timeout = max(5, int(timeout))
        self.log = log or (lambda m: None)
        self.active = None

    @staticmethod
    def _parts(symbol):
        s = symbol.replace("-", "/").replace("_", "/").upper().strip()
        if "/" in s:
            return s.split("/", 1)
        for q in ("USDT", "USDC", "USD", "EUR", "BTC", "ETH"):
            if s.endswith(q):
                return s[: -len(q)], q
        return s, "USDT"

    def _symbol_for(self, ex, symbol):
        base, quote = self._parts(symbol)
        if ex == "yahoo":
            raw = symbol.strip().upper()
            if "=" in raw:
                return raw
            q = "USD" if quote in ("USDT", "USDC") else quote
            if base in ("XAU", "GOLD"):
                return "GC=F"
            if base in ("XAG", "SILVER"):
                return "SI=F"
            fx = {"EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"}
            # Yahoo's FX tickers are directional (EURUSD=X, but also
            # USDJPY=X - USD as the BASE for JPY/CAD/CHF, since those are
            # conventionally quoted the other way round from EUR/GBP/AUD/NZD).
            # Originally this only handled base-in-fx and missed USD/JPY,
            # USD/CAD, USD/CHF style pairs entirely.
            if (base in fx and (q in fx or q == "USD")) or (base == "USD" and q in fx):
                return f"{base}{q}=X"
            return f"{base}-{q}"
        if ex == "cryptocompare":
            return f"{base}|{'USD' if quote in ('USDT','USDC') else quote}"
        if ex in ("binance", "bybit"):
            return f"{base}{quote}"
        if ex == "okx":
            return f"{base}-{quote}"
        if ex == "coinbase":
            return f"{base}-{'USD' if quote in ('USDT','USDC') else quote}"
        if ex == "kraken":
            return f"{base}{'USD' if quote in ('USDT','USDC') else quote}"
        return f"{base}{quote}"

    def _get(self, url):
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; SweepScan/1.0)",
            "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            d = ""
            try:
                d = e.read().decode("utf-8")[:150]
            except Exception:
                pass
            raise FeedError(f"HTTP {e.code} {d}")
        except urllib.error.URLError as e:
            raise FeedError(f"unreachable ({e.reason})")
        except Exception as e:
            raise FeedError(str(e))

    @staticmethod
    def _resample(candles, factor):
        out = []
        for i in range(0, len(candles) - factor + 1, factor):
            ch = candles[i:i + factor]
            out.append({"time": ch[0]["time"], "open": ch[0]["open"],
                        "high": max(x["high"] for x in ch),
                        "low": min(x["low"] for x in ch),
                        "close": ch[-1]["close"]})
        return out

    def _fetch_one(self, ex, symbol, tf, count):
        spec = self.SPECS[ex]
        sym = self._symbol_for(ex, symbol)
        interval = spec["tf"][tf]
        limit = min(max(int(count), 10), 720)

        if ex == "yahoo":
            need = limit * (4 if tf == "4h" else 1)
            rng = "7d" if interval in ("1m", "5m") else \
                  ("60d" if interval in ("15m", "30m", "1h") else "2y")
            q = urllib.parse.urlencode({"interval": interval, "range": rng})
            data = self._get(f"{spec['url'].format(sym=sym)}?{q}")
            chart = data.get("chart") or {}
            if chart.get("error"):
                raise FeedError(f"yahoo: {chart['error']}")
            res = (chart.get("result") or [None])[0]
            if not res:
                raise FeedError(f"yahoo: no data for {sym}")
            st = res.get("timestamp") or []
            qd = ((res.get("indicators") or {}).get("quote") or [{}])[0]
            o, h, l, c = (qd.get("open") or [], qd.get("high") or [],
                          qd.get("low") or [], qd.get("close") or [])
            rows = [(st[i] * 1000, o[i], h[i], l[i], c[i]) for i in range(len(st))
                    if i < len(c) and c[i] is not None and o[i] is not None]
            rows = rows[-need:]
        elif ex == "cryptocompare":
            endpoint, agg = interval
            base, q_ccy = sym.split("|")
            q = urllib.parse.urlencode({"fsym": base, "tsym": q_ccy,
                                         "limit": min(limit, 2000), "aggregate": agg})
            data = self._get(f"{spec['url'].format(endpoint=endpoint)}?{q}")
            if data.get("Response") == "Error":
                raise FeedError(f"cryptocompare: {data.get('Message')}")
            rows = [(r["time"] * 1000, r["open"], r["high"], r["low"], r["close"])
                    for r in ((data.get("Data") or {}).get("Data") or []) if r.get("close")]
        elif ex == "binance":
            q = urllib.parse.urlencode({"symbol": sym, "interval": interval, "limit": limit})
            rows = [(r[0], r[1], r[2], r[3], r[4]) for r in self._get(f"{spec['url']}?{q}")]
        elif ex == "bybit":
            q = urllib.parse.urlencode({"category": "spot", "symbol": sym,
                                         "interval": interval, "limit": min(limit, 1000)})
            data = self._get(f"{spec['url']}?{q}")
            if str(data.get("retCode")) != "0":
                raise FeedError(f"bybit: {data.get('retMsg')}")
            rows = [(int(r[0]), r[1], r[2], r[3], r[4])
                    for r in data.get("result", {}).get("list", [])][::-1]
        elif ex == "okx":
            q = urllib.parse.urlencode({"instId": sym, "bar": interval, "limit": min(limit, 300)})
            data = self._get(f"{spec['url']}?{q}")
            if str(data.get("code")) != "0":
                raise FeedError(f"okx: {data.get('msg')}")
            rows = [(int(r[0]), r[1], r[2], r[3], r[4]) for r in data.get("data", [])][::-1]
        elif ex == "kraken":
            q = urllib.parse.urlencode({"pair": sym, "interval": interval})
            data = self._get(f"{spec['url']}?{q}")
            if data.get("error"):
                raise FeedError(f"kraken: {data['error']}")
            res = data.get("result", {})
            key = next((k for k in res if k != "last"), None)
            if not key:
                raise FeedError("kraken: no data")
            rows = [(int(r[0]) * 1000, r[1], r[2], r[3], r[4]) for r in res[key]][-limit:]
        elif ex == "coinbase":
            q = urllib.parse.urlencode({"granularity": interval})
            data = self._get(f"{spec['url'].format(sym=sym)}?{q}")
            if isinstance(data, dict):
                raise FeedError(f"coinbase: {data.get('message')}")
            rows = [(int(r[0]) * 1000, r[3], r[2], r[1], r[4]) for r in data][::-1][-limit:]
        else:
            raise FeedError(f"unknown source {ex}")

        if not rows:
            raise FeedError("empty response")
        out = [{"time": datetime.fromtimestamp(ts / 1000), "open": float(o),
                "high": float(h), "low": float(l), "close": float(c)}
               for ts, o, h, l, c in rows]
        if ex == "yahoo" and tf == "4h":
            out = self._resample(out, 4)
        return out

    def fetch(self, symbol, timeframe, count=300):
        tf = self.TF_ALIAS.get(timeframe.lower())
        if tf is None:
            raise FeedError(f"Unsupported timeframe '{timeframe}'. "
                             f"Use 1m/5m/15m/30m/1h/4h/1d or M1/M5/M15/M30/H1/H4/D1.")
        if self.requested != "auto":
            if self.requested not in self.SPECS:
                raise FeedError(f"Unknown source '{self.requested}'.")
            return self._fetch_one(self.requested, symbol, tf, count)
        order = ([self.active] if self.active else []) + \
                [e for e in self.ORDER if e != self.active]
        errors = []
        for ex in order:
            try:
                data = self._fetch_one(ex, symbol, tf, count)
                if self.active != ex:
                    self.log(f"  using {ex} for market data")
                    self.active = ex
                return data
            except FeedError as e:
                errors.append(f"{ex}: {e}")
        raise FeedError("no source reachable.\n      " + "\n      ".join(errors))

    def check(self):
        res = []
        for ex in self.ORDER:
            try:
                self._fetch_one(ex, "ETH/USDT", "1h", 10)
                res.append((ex, True, "reachable"))
            except Exception as e:
                res.append((ex, False, str(e)[:80]))
        return res

    def close(self):
        pass


class MT5Feed:
    """MetaTrader 5 - your broker's own pairs (ETHUSDm, XAUUSDm...).
    Windows only, needs `pip install MetaTrader5` and MT5 running."""

    def __init__(self, login=None, password=None, server=None, path=None, log=None):
        self.log = log or (lambda m: None)
        try:
            import MetaTrader5 as mt5
        except ImportError:
            raise FeedError(
                "MetaTrader5 package not installed.\n"
                "      Run:  pip install MetaTrader5\n"
                "      (Windows only - make sure MT5 is installed and running.)")
        self.mt5 = mt5
        kw = {}
        if path:
            kw["path"] = path
        if login:
            kw.update(login=int(login), password=password, server=server)
        if not mt5.initialize(**kw):
            raise FeedError(f"MT5 initialize failed: {mt5.last_error()}\n"
                             f"      Make sure MetaTrader 5 is open and you're logged in.")
        self.TF = {"1m": mt5.TIMEFRAME_M1, "5m": mt5.TIMEFRAME_M5,
                   "15m": mt5.TIMEFRAME_M15, "30m": mt5.TIMEFRAME_M30,
                   "1h": mt5.TIMEFRAME_H1, "4h": mt5.TIMEFRAME_H4,
                   "1d": mt5.TIMEFRAME_D1}

    def fetch(self, symbol, timeframe, count=300):
        tf = PublicFeed.TF_ALIAS.get(timeframe.lower())
        code = self.TF.get(tf)
        if code is None:
            raise FeedError(f"Unsupported timeframe {timeframe}")
        if not self.mt5.symbol_select(symbol, True):
            raise FeedError(f"Symbol '{symbol}' not found in your MT5 Market Watch")
        rates = self.mt5.copy_rates_from_pos(symbol, code, 0, count)
        if rates is None or len(rates) == 0:
            raise FeedError(f"No data for {symbol}: {self.mt5.last_error()}")
        return [{"time": datetime.fromtimestamp(r["time"]), "open": float(r["open"]),
                 "high": float(r["high"]), "low": float(r["low"]),
                 "close": float(r["close"])} for r in rates]

    def close(self):
        try:
            self.mt5.shutdown()
        except Exception:
            pass


class CSVFeed:
    """Offline. Reads plain CSV or MT5 exports (tab separated, <DATE> headers)."""

    def __init__(self, tpl="{symbol}_{timeframe}.csv", log=None):
        self.tpl = tpl
        self.log = log or (lambda m: None)

    @staticmethod
    def _key(k):
        return (k or "").strip().strip("<>").strip().lower()

    def fetch(self, symbol, timeframe, count=300):
        path = self.tpl.format(symbol=symbol, timeframe=timeframe)
        try:
            with open(path, newline="", encoding="utf-8-sig") as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    delim = csv.Sniffer().sniff(sample, delimiters=",;\t").delimiter
                except Exception:
                    delim = "\t" if "\t" in sample else ","
                rows = [r for r in csv.reader(f, delimiter=delim)
                        if r and any(x.strip() for x in r)]
        except FileNotFoundError:
            raise FeedError(f"CSV not found: {path}")
        if not rows:
            raise FeedError(f"CSV empty: {path}")
        header = [self._key(h) for h in rows[0]]
        need = ("open", "high", "low", "close")
        if not all(n in header for n in need):
            raise FeedError(f"CSV {path} missing open/high/low/close columns. Found: {rows[0]}")
        idx = {n: header.index(n) for n in need}
        di = header.index("date") if "date" in header else None
        ti = header.index("time") if "time" in header else None
        out = []
        for r in rows[1:]:
            try:
                stamp = ""
                if di is not None and ti is not None and len(r) > max(di, ti):
                    stamp = f"{r[di]} {r[ti]}".strip()
                elif ti is not None and len(r) > ti:
                    stamp = r[ti]
                elif di is not None and len(r) > di:
                    stamp = r[di]
                out.append({"time": stamp, "open": float(r[idx["open"]]),
                            "high": float(r[idx["high"]]), "low": float(r[idx["low"]]),
                            "close": float(r[idx["close"]])})
            except (ValueError, IndexError):
                continue
        if not out:
            raise FeedError(f"No usable rows in {path}")
        return out[-count:] if count else out

    def close(self):
        pass


def make_feed(cfg, log=None):
    kind = (cfg.get("feed") or "auto").lower()
    if kind == "mt5":
        return MT5Feed(cfg.get("mt5_login"), cfg.get("mt5_password"),
                        cfg.get("mt5_server"), cfg.get("mt5_path"), log=log)
    if kind == "csv":
        return CSVFeed(cfg.get("csv_template", "{symbol}_{timeframe}.csv"), log=log)
    ex = "auto" if kind in ("auto", "public") else kind
    return PublicFeed(ex, cfg.get("timeout", 20), log=log)


# ==========================================================================
# 3. ALERTS
# ==========================================================================

def telegram_send(token, chat_id, text):
    """Send a Telegram message. Returns (ok, message).

    Sends PLAIN TEXT deliberately - no parse_mode. Markdown parsing is the
    most common cause of HTTP 400 here, because any stray *, _, [ or ` in a
    symbol name or price makes Telegram reject the whole message with
    "can't parse entities".
    """
    token = (token or "").strip()
    chat_id = (chat_id or "").strip()

    # people often paste the token with the "bot" prefix already attached
    if token.lower().startswith("bot") and ":" in token:
        token = token[3:]
    if not token:
        return False, "no bot token set"
    if not chat_id:
        return False, "no chat ID set"
    if ":" not in token:
        return False, ("that token doesn't look right - it should look like "
                        "123456789:AAExxxxxxxxxxxxxxxxxxxxxxxxx")
    if chat_id.startswith("@"):
        return False, ("chat ID must be a number, not a @username. Use the "
                        "'Find my chat ID' button, or message your bot then open "
                        "https://api.telegram.org/bot<TOKEN>/getUpdates to find it.")
    if not re.fullmatch(r"-?\d+", chat_id):
        return False, f"chat ID '{chat_id}' must be a number (negative for groups)"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            r.read()
        return True, "sent"
    except urllib.error.HTTPError as e:
        desc = ""
        try:
            desc = json.loads(e.read().decode("utf-8")).get("description", "")
        except Exception:
            pass
        low = desc.lower()
        if "chat not found" in low:
            hint = ("Telegram says 'chat not found'. Open Telegram, find your bot "
                    "and press START (or send it any message) - a bot can't message "
                    "you until you've messaged it first. Then re-check the chat ID.")
        elif e.code == 401 or "unauthorized" in low:
            hint = ("Telegram rejected the token. Copy it again from @BotFather - "
                    "it should look like 123456789:AAExxxxxxxx")
        elif "parse" in low:
            hint = f"Telegram couldn't parse the message: {desc}"
        elif "blocked" in low:
            hint = "You've blocked this bot in Telegram. Unblock it and retry."
        else:
            hint = f"HTTP {e.code}{': ' + desc if desc else ' (no detail given)'}"
        return False, hint
    except urllib.error.URLError as e:
        return False, (f"couldn't reach Telegram ({e.reason}) - "
                        f"if your VPN is off, turn it on")
    except Exception as e:
        return False, str(e)


class Notifier:
    def __init__(self, cfg, log=print, on_signal=None):
        self.cfg = cfg
        self.log = log
        self.on_signal = on_signal

    def send(self, title, body, payload=None, chart=None):
        if self.cfg.get("console", True):
            self.log(f"\n{'='*58}\n{title}\n{'-'*58}\n{body}\n{'='*58}")
        if self.cfg.get("sound", True):
            self._beep()
        if self.cfg.get("telegram_token") and self.cfg.get("telegram_chat_id"):
            self._telegram(f"{title}\n\n{body}")
        if self.cfg.get("log_file"):
            self._logfile(payload or {"title": title, "body": body})
        if self.on_signal and payload:
            try:
                self.on_signal(payload, chart)
            except Exception:
                pass

    def _beep(self):
        try:
            if sys.platform == "win32":
                import winsound
                winsound.MessageBeep()
            else:
                print("\a", end="", flush=True)
        except Exception:
            pass

    def _telegram(self, text):
        ok, msg = telegram_send(self.cfg.get("telegram_token"),
                                 self.cfg.get("telegram_chat_id"), text)
        if not ok:
            self.log(f"Telegram: {msg}")
        return ok, msg

    def _logfile(self, payload):
        try:
            p = self.cfg["log_file"]
            d = os.path.dirname(p)
            if d:
                os.makedirs(d, exist_ok=True)
            rec = dict(payload)
            rec["alerted_at"] = datetime.now().isoformat()
            with open(p, "a") as f:
                f.write(json.dumps(rec, default=str) + "\n")
        except Exception as e:
            self.log(f"log write failed: {e}")


# ==========================================================================
# 4. SCAN ENGINE
# ==========================================================================

DEFAULTS = {
    "feed": "auto",
    "symbols": ["ETH/USDT", "BTC/USDT"],
    "timeframes": ["1h", "4h"],
    "timeout": 20,
    "candles": 300,
    "poll_seconds": 300,
    "only_closed_candles": True,
    "csv_template": "{symbol}_{timeframe}.csv",
    "mt5_login": "", "mt5_password": "", "mt5_server": "",
    "detector": {"lookback": 5, "swing_left": 2, "swing_right": 2,
                  "sweep_window": 3, "min_displacement_atr": 1.5,
                  "min_body_ratio": 0.55, "require_structure_break": True,
                  "atr_period": 14, "entry_retracement_pct": 0.5,
                  "entry_max_wait_bars": 60},
    "breakout": {"enabled": True, "range_window": 20, "fvg_near_structure_atr": 2.0},
    "alerts": {"console": True, "sound": True, "log_file": "alerts.jsonl",
                "telegram_token": "", "telegram_chat_id": ""},
}


def load_config(path=CONFIG_PATH):
    cfg = json.loads(json.dumps(DEFAULTS))
    try:
        with open(path) as f:
            user = json.load(f)
        for k, v in user.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    except FileNotFoundError:
        pass
    except Exception:
        pass

    # Telegram credentials can come from the environment instead of the
    # config file - so a config.json committed to a repo (e.g. for a
    # scheduled CI run) never has to contain the actual secret.
    env_token = os.environ.get("SWEEPSCAN_TG_TOKEN")
    env_chat = os.environ.get("SWEEPSCAN_TG_CHAT_ID")
    if env_token:
        cfg["alerts"]["telegram_token"] = env_token
    if env_chat:
        cfg["alerts"]["telegram_chat_id"] = env_chat

    return cfg


def save_config(cfg, path=CONFIG_PATH):
    try:
        with open(path, "w") as f:
            json.dump(cfg, f, indent=2)
        return True
    except Exception:
        return False


def load_state(path):
    """Load persisted seen/armed state (see save_state) for a --state run.
    Missing or unreadable state is treated as a first run: empty seen/armed."""
    try:
        with open(path) as f:
            data = json.load(f)
        seen = set(data.get("seen", []))
        armed = data.get("armed", {})
        for rec in armed.values():
            try:
                rec["disp_time"] = float(rec.get("disp_time", 0))
            except (TypeError, ValueError):
                rec["disp_time"] = 0.0
            rec["fired"] = bool(rec.get("fired", False))
            rec["age"] = int(rec.get("age", 0))
        return seen, armed
    except FileNotFoundError:
        return set(), {}
    except Exception:
        return set(), {}


def save_state(path, seen, armed):
    """Persist seen/armed so a stateless runner (a fresh container on every
    scheduled run, e.g. GitHub Actions) still remembers what it already
    alerted on and what it's still watching for a retest."""
    try:
        with open(path, "w") as f:
            json.dump({"seen": sorted(seen), "armed": armed}, f, default=str)
        return True
    except Exception:
        return False


def scan_once(feed, cfg, notifier, seen, armed=None, backtest=False, prime=False,
              log=print, stop=None):
    """Two-stage scan.

    Stage 1 (reversal setups from detect()): sweep + displacement + structure
    break + FVG. Instead of alerting immediately, the setup is parked in
    `armed` and only watched from here on - this is the "notify me when
    price trades back to the entry, not when the setup first forms" behavior.

    Stage 2 (entries): every poll, each armed setup's symbol/timeframe is
    checked against freshly fetched candles for a later bar whose high
    (bearish) / low (bullish) reaches its entry_level (extreme + pct% of the
    way back to the broken structure level). That's when the real alert
    fires. Setups that never retrace within entry_max_wait_bars are dropped.

    Breakout setups (detect_breakout()) are a different pattern - a sweep
    that continues instead of reversing - but go through the exact same
    arm-then-wait-for-retest flow as reversal setups: detect_breakout()
    computes an entry_level (halfway back from the move's extreme toward
    the broken ceiling/floor, same math as reversal) and only rejects a
    setup outright if its FVG isn't near that ceiling/floor level
    (see _fvg_near_level). Everything that survives gets parked in `armed`
    right alongside reversal setups and only alerts once price actually
    retests entry_level.

    prime=True seeds `seen` from whatever is already sitting in the fetched
    history and still fully arms + resolves setups against that history (so
    a setup that's still waiting for its retest is properly armed for the
    NEXT run, and one that already retraced in the past gets silently
    resolved instead of firing a stale alert later) - it just suppresses the
    notifier.send() calls themselves, so nothing actually gets alerted
    during this pass. Used once before the watch loop starts (or on a
    stateless --state first run) so old, already-played-out setups don't
    flood you with alerts on the very first real poll.
    """
    hits = 0
    if armed is None:
        armed = {}
    d = dict(cfg["detector"])
    entry_pct = d.pop("entry_retracement_pct", 0.5)
    max_wait = d.pop("entry_max_wait_bars", 60)
    if backtest:
        d["lookback"] = 10000

    bcfg = dict(cfg.get("breakout", {"enabled": True, "range_window": 20}))
    breakout_enabled = bcfg.pop("enabled", True)
    b_kwargs = {k: v for k, v in d.items() if k != "require_structure_break"}
    b_kwargs["range_window"] = bcfg.get("range_window", 20)
    b_kwargs["entry_retracement_pct"] = entry_pct
    b_kwargs["fvg_near_structure_atr"] = bcfg.get("fvg_near_structure_atr", 2.0)

    for symbol in cfg["symbols"]:
        for tf in cfg["timeframes"]:
            if stop and stop.is_set():
                return hits
            try:
                candles = feed.fetch(symbol, tf, cfg.get("candles", 300))
            except FeedError as e:
                log(f"  {symbol} {tf}: {e}")
                continue
            except Exception as e:
                log(f"  {symbol} {tf}: {e}")
                continue
            if cfg.get("only_closed_candles", True) and not backtest and len(candles) > 1:
                candles = candles[:-1]
            if not candles:
                continue

            new = 0

            # --- stage 1: find reversal setups, arm them (don't alert yet) ---
            try:
                sigs = detect(candles, symbol=symbol, timeframe=tf,
                              entry_retracement_pct=entry_pct, **d)
            except Exception as e:
                log(f"  {symbol} {tf}: detector error: {e}")
                sigs = []
            for s in sigs:
                key = f"{s.symbol}|{s.timeframe}|{s.direction}|{s.time}"
                if key in seen:
                    continue
                seen.add(key)
                new += 1
                if s.entry_level is None:
                    continue
                armed[key] = {"signal": s.as_dict(), "direction": s.direction,
                               "symbol": symbol, "tf": tf, "entry_level": s.entry_level,
                               "disp_time": _epoch(s.time), "age": 0, "fired": False,
                               "pattern": "reversal"}
                if not prime:
                    log(f"  {symbol} {tf}: {s.direction.upper()} setup formed - watching for "
                        f"entry near {s.entry_level:.6g} ({s.entry_pct*100:.0f}% retest)")

            # --- breakout pattern: arm it too, same retest-wait flow as reversal ---
            if breakout_enabled:
                try:
                    bsigs = detect_breakout(candles, symbol=symbol, timeframe=tf, **b_kwargs)
                except Exception as e:
                    log(f"  {symbol} {tf}: breakout detector error: {e}")
                    bsigs = []
                for s in bsigs:
                    key = f"BRK|{s.symbol}|{s.timeframe}|{s.direction}|{s.time}"
                    if key in seen:
                        continue
                    seen.add(key)
                    new += 1
                    if s.entry_level is None:
                        continue
                    armed[key] = {"signal": s.as_dict(), "direction": s.direction,
                                   "symbol": symbol, "tf": tf, "entry_level": s.entry_level,
                                   "disp_time": _epoch(s.time), "age": 0, "fired": False,
                                   "pattern": "breakout"}
                    if not prime:
                        log(f"  {symbol} {tf}: {s.direction.upper()} BREAKOUT setup formed - "
                            f"watching for entry near {s.entry_level:.6g} "
                            f"({s.entry_pct*100:.0f}% retest)")

            # --- stage 2: check armed setups (reversal + breakout) for this symbol/tf ---
            # (always runs, even during prime - a setup that already retraced somewhere back in
            # the fetched history needs to be resolved now, silently, or it'll wrongly look "new"
            # and fire a stale alert the next time this symbol/tf is scanned)
            for key in [k for k, r in armed.items()
                        if r["symbol"] == symbol and r["tf"] == tf and not r["fired"]]:
                rec = armed[key]
                later = [c for c in candles if _epoch(c["time"]) > rec["disp_time"]]
                rec["age"] = len(later)  # bars elapsed since displacement, not polls elapsed
                hit = None
                for c in later:
                    reached = (c["high"] >= rec["entry_level"] if rec["direction"] == "bearish"
                               else c["low"] <= rec["entry_level"])
                    if reached:
                        hit = c
                        break
                if hit is not None:
                    rec["fired"] = True
                    new += 1
                    del armed[key]
                    if prime:
                        continue
                    hits += 1
                    is_breakout = rec.get("pattern") == "breakout"
                    p = dict(rec["signal"])
                    p["stage"] = "breakout_entry" if is_breakout else "entry"
                    p["close"] = hit["close"]
                    idx = candles.index(hit)
                    lo, hi = max(0, idx - 15), min(len(candles), idx + 3)
                    chart = {"candles": [{"time": c["time"], "open": c["open"],
                                          "high": c["high"], "low": c["low"],
                                          "close": c["close"]} for c in candles[lo:hi]],
                              "sweep_pos": None, "disp_pos": None, "entry_pos": idx - lo}
                    tag = "BREAKOUT ENTRY" if is_breakout else "ENTRY"
                    notifier.send(f"{rec['direction'].upper()} {tag} ({p['entry_pct']*100:.0f}% "
                                  f"retest) | {symbol} {tf}", entry_summary(p), p, chart)
                elif rec["age"] > max_wait:
                    if not prime:
                        log(f"  {symbol} {tf}: {rec['direction'].upper()} setup expired "
                            f"without a retest ({max_wait} bars) - dropped.")
                    del armed[key]

            if not new:
                log(f"  {symbol} {tf}: no new setup ({len(candles)} candles)")
    return hits


# ==========================================================================
# 5. NETWORK DIAGNOSTIC
# ==========================================================================

DIAG_HOSTS = [
    ("example.com", "control"),
    ("query1.finance.yahoo.com", "Yahoo Finance (aggregator)"),
    ("min-api.cryptocompare.com", "CryptoCompare (aggregator)"),
    ("api.bybit.com", "exchange"),
    ("api.kraken.com", "exchange"),
    ("api.binance.com", "exchange"),
]


def netcheck(log=print):
    log("=" * 58)
    log(f"{APP_NAME} connection diagnostic")
    log("=" * 58)
    log(f"Python  : {sys.version.split()[0]} ({sys.platform})")
    log(f"OpenSSL : {ssl.OPENSSL_VERSION}")
    proxies = {k: v for k, v in os.environ.items() if "proxy" in k.lower()}
    log(f"Proxy   : {proxies or 'none set'}")
    results = {}
    for host, note in DIAG_HOSTS:
        log(f"\n  {host}  ({note})")
        try:
            ip = socket.gethostbyname(host)
            log(f"    [OK]   DNS -> {ip}")
        except Exception as e:
            log(f"    [FAIL] DNS: {e}")
            results[host] = "dns"
            continue
        try:
            s = socket.create_connection((ip, 443), timeout=10)
            s.close()
            log("    [OK]   TCP :443")
        except Exception as e:
            log(f"    [FAIL] TCP: {e}")
            results[host] = "tcp"
            continue
        try:
            ctx = ssl.create_default_context()
            with socket.create_connection((host, 443), timeout=10) as sk:
                with ctx.wrap_socket(sk, server_hostname=host) as ss:
                    log(f"    [OK]   TLS {ss.version()}")
        except Exception as e:
            log(f"    [FAIL] TLS: {e}")
            results[host] = "tls"
            continue
        results[host] = "ok"
    log("\n" + "=" * 58)
    ok = [h for h, r in results.items() if r == "ok"]
    log(f"Reachable: {', '.join(ok) if ok else 'NOTHING'}")
    if results.get("query1.finance.yahoo.com") == "ok":
        log("Yahoo works - set Data source to 'yahoo' and you're good.")
    elif not ok:
        log("Nothing reachable. Check your connection or turn on your VPN.")
    elif results.get("example.com") == "ok":
        log("General internet works but data sources are blocked.")
        log("-> Use a VPN, or switch to MT5 / CSV mode.")
    return results


# ==========================================================================
# 6. CLI MODE
# ==========================================================================

def run_cli(args):
    cfg = load_config(args.config)
    if args.netcheck:
        netcheck()
        return 0
    print(f"{APP_NAME} | source: {cfg['feed']} | "
          f"{', '.join(cfg['symbols'])} @ {', '.join(cfg['timeframes'])}")
    try:
        feed = make_feed(cfg, log=print)
    except FeedError as e:
        print(f"Data source failed: {e}")
        return 1
    notifier = Notifier(cfg["alerts"], log=print)
    try:
        if args.backtest:
            seen, armed = set(), {}
            n = scan_once(feed, cfg, notifier, seen, armed=armed, backtest=True)
            print(f"\nBacktest done: {n} historical setups/entries.")
        elif args.state:
            # Stateless-runner mode (e.g. a GitHub Actions run): every
            # invocation is a fresh process, so seen/armed are loaded from
            # (and saved back to) a JSON file instead of living in memory
            # for the life of a long-running loop. The very first run - no
            # state file yet - primes silently so the whole existing
            # backlog doesn't fire as alerts.
            first_run = not os.path.exists(args.state)
            seen, armed = load_state(args.state)
            n = scan_once(feed, cfg, notifier, seen, armed=armed, prime=first_run)
            save_state(args.state, seen, armed)
            if first_run:
                print(f"\nFirst run - primed silently with {len(seen)} existing setup(s). "
                      f"State saved to {args.state}. Future runs will alert on new activity.")
            else:
                print(f"\n{n} new setup(s)/entry(ies). State saved to {args.state} "
                      f"({len(seen)} seen, {len(armed)} still watching for a retest).")
        elif args.once:
            seen, armed = set(), {}
            print(f"\n{scan_once(feed, cfg, notifier, seen, armed=armed)} new setup(s)/entry(ies).")
        else:
            seen, armed = set(), {}
            print("Priming (recording existing setups silently)...")
            scan_once(feed, cfg, Notifier({"console": False}, log=lambda m: None), seen,
                      armed=armed, prime=True, log=lambda m: None)
            print(f"Primed with {len(seen)}. Watching. Ctrl+C to stop.\n")
            while True:
                scan_once(feed, cfg, notifier, seen, armed=armed)
                time.sleep(cfg.get("poll_seconds", 300))
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        feed.close()
    return 0


# ==========================================================================
# 7. GUI
# ==========================================================================

BG, PANEL, FIELD = "#0d0d0d", "#161616", "#1e1e1e"
FG, MUTED, ACCENT = "#e8e8e8", "#8a8a8a", "#e2231a"
HOVER, BORDER = "#ff3b30", "#333333"
GREEN, RED = "#26a65b", "#e2231a"
YELLOW = "#e2a23a"

CONF_COLOR = {"STRONG": GREEN, "MEDIUM": YELLOW, "WEAK": MUTED}
CONF_DOTS = {"STRONG": "●●●", "MEDIUM": "●●○",
             "WEAK": "●○○"}


def draw_candle_chart(canvas, payload, chart, width, height):
    """Draw a small candlestick chart on a tk Canvas: the raw candles around
    a setup, with the swept level, FVG zone, and the sweep/displacement
    candles picked out. Pure tkinter primitives - no plotting library."""
    canvas.delete("all")
    candles = (chart or {}).get("candles") or []
    if not candles:
        canvas.create_text(width / 2, height / 2, fill=MUTED, font=("Segoe UI", 9),
                            text="No chart data for this setup.")
        return

    pad_l, pad_r, pad_t, pad_b = 6, 64, 12, 18
    plot_w = max(10, width - pad_l - pad_r)
    plot_h = max(10, height - pad_t - pad_b)

    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    extras = [v for v in (payload.get("fvg_top"), payload.get("fvg_bottom"),
                          payload.get("swept_level"), payload.get("entry_level"))
              if v is not None]
    lo_v = min(lows + extras)
    hi_v = max(highs + extras)
    if hi_v <= lo_v:
        hi_v = lo_v + max(abs(lo_v) * 0.01, 1e-9)
    span = (hi_v - lo_v) * 1.1
    mid = (hi_v + lo_v) / 2
    lo_v, hi_v = mid - span / 2, mid + span / 2

    def y(v):
        return pad_t + plot_h - (v - lo_v) / (hi_v - lo_v) * plot_h

    n = len(candles)
    step = plot_w / n
    body_w = max(2, step * 0.55)

    top, bottom = payload.get("fvg_top"), payload.get("fvg_bottom")
    if top is not None and bottom is not None:
        canvas.create_rectangle(pad_l, y(top), pad_l + plot_w, y(bottom),
                                 fill=FIELD, outline="")
        for v, lbl in ((top, None), (bottom, None)):
            canvas.create_line(pad_l, y(v), pad_l + plot_w, y(v), fill=ACCENT,
                                dash=(3, 2))
        canvas.create_text(pad_l + plot_w + 4, y((top + bottom) / 2), anchor="w",
                            fill=ACCENT, font=("Consolas", 8), text="FVG")

    swl = payload.get("swept_level")
    if swl is not None:
        canvas.create_line(pad_l, y(swl), pad_l + plot_w, y(swl), fill=MUTED,
                            dash=(2, 3))
        canvas.create_text(pad_l + plot_w + 4, y(swl), anchor="w", fill=MUTED,
                            font=("Consolas", 8), text="swept")

    entry_lvl = payload.get("entry_level")
    if entry_lvl is not None:
        canvas.create_line(pad_l, y(entry_lvl), pad_l + plot_w, y(entry_lvl),
                            fill=YELLOW, dash=(4, 2))
        pct = payload.get("entry_pct") or 0.5
        canvas.create_text(pad_l + plot_w + 4, y(entry_lvl), anchor="w", fill=YELLOW,
                            font=("Consolas", 8), text=f"entry {pct*100:.0f}%")

    sweep_pos = (chart or {}).get("sweep_pos")
    disp_pos = (chart or {}).get("disp_pos")
    entry_pos = (chart or {}).get("entry_pos")

    for i, c in enumerate(candles):
        cx = pad_l + i * step + step / 2
        up = c["close"] >= c["open"]
        color = GREEN if up else RED
        canvas.create_line(cx, y(c["high"]), cx, y(c["low"]), fill=color, width=1)
        top_body, bot_body = (c["close"], c["open"]) if up else (c["open"], c["close"])
        if top_body == bot_body:
            top_body += (hi_v - lo_v) * 0.002
        outline, owidth = "", 1
        if i == disp_pos:
            outline, owidth = "white", 2
        canvas.create_rectangle(cx - body_w / 2, y(top_body), cx + body_w / 2, y(bot_body),
                                 fill=color, outline=outline, width=owidth)
        if i == sweep_pos:
            marker_y = y(c["high"]) - 8 if payload.get("direction") == "bearish" else y(c["low"]) + 8
            canvas.create_text(cx, marker_y, text="×", fill=YELLOW,
                                font=("Consolas", 10, "bold"))
        if i == entry_pos:
            marker_y = y(c["low"]) + 10 if payload.get("direction") == "bearish" else y(c["high"]) - 10
            canvas.create_text(cx, marker_y, text="◆", fill=YELLOW,
                                font=("Consolas", 10, "bold"))

    canvas.create_text(width - pad_r + 4, pad_t, anchor="ne", fill=MUTED,
                        font=("Consolas", 8), text=f"{hi_v:.6g}")
    canvas.create_text(width - pad_r + 4, pad_t + plot_h, anchor="se", fill=MUTED,
                        font=("Consolas", 8), text=f"{lo_v:.6g}")


def run_gui():
    try:
        import tkinter as tk
        from tkinter import ttk, messagebox
    except ImportError:
        print("tkinter not available - falling back to console mode.")
        print("Run with --cli instead.")
        return 1

    class App(tk.Tk):
        def __init__(self):
            super().__init__()
            self.title(f"{APP_NAME} - Sweep + Displacement + FVG Scanner")
            self.geometry("1200x800")
            self.minsize(980, 640)
            self.configure(bg=BG)
            self.cfg = load_config()
            self.logq = queue.Queue()
            self.stop_evt = threading.Event()
            self.worker = None
            self.seen = set()
            self.armed = {}
            self.signals = []
            self.signal_by_item = {}
            self.counts = {"STRONG": 0, "MEDIUM": 0, "WEAK": 0}
            self.next_check_at = None
            self.mode = "ready"
            self._style()
            self._build()
            self.protocol("WM_DELETE_WINDOW", self._close)
            self.after(150, self._drain)
            self.after(500, self._tick_status)

        def _style(self):
            s = ttk.Style(self)
            try:
                s.theme_use("clam")
            except tk.TclError:
                pass
            s.configure(".", background=BG, foreground=FG, fieldbackground=FIELD,
                        bordercolor=BORDER, font=("Segoe UI", 9))
            s.configure("TFrame", background=BG)
            s.configure("P.TFrame", background=PANEL)
            s.configure("TLabel", background=BG, foreground=FG)
            s.configure("P.TLabel", background=PANEL, foreground=FG)
            s.configure("M.TLabel", background=PANEL, foreground=MUTED, font=("Segoe UI", 8))
            s.configure("H.TLabel", background=PANEL, foreground=ACCENT,
                        font=("Segoe UI", 10, "bold"))
            s.configure("TLabelframe", background=PANEL, bordercolor=ACCENT,
                        relief="solid", borderwidth=1)
            s.configure("TLabelframe.Label", background=PANEL, foreground=ACCENT,
                        font=("Segoe UI", 9, "bold"))
            s.configure("TButton", background=ACCENT, foreground="white",
                        bordercolor=ACCENT, padding=6)
            s.map("TButton", background=[("active", HOVER), ("disabled", BORDER)])
            s.configure("S.TButton", background=FIELD, foreground=FG, bordercolor=BORDER)
            s.map("S.TButton", background=[("active", PANEL)])
            s.configure("TEntry", fieldbackground=FIELD, foreground=FG, insertcolor=FG)
            s.configure("TSpinbox", fieldbackground=FIELD, foreground=FG,
                        background=PANEL, arrowcolor=ACCENT)
            s.configure("TCombobox", fieldbackground=FIELD, foreground=FG,
                        background=PANEL, arrowcolor=ACCENT)
            s.configure("TCheckbutton", background=PANEL, foreground=FG)
            s.map("TCheckbutton", indicatorcolor=[("selected", ACCENT)])
            s.configure("Treeview", background=FIELD, fieldbackground=FIELD,
                        foreground=FG, rowheight=24, bordercolor=BORDER)
            s.configure("Treeview.Heading", background=ACCENT, foreground="white",
                        font=("Segoe UI", 9, "bold"), relief="flat")
            s.configure("TNotebook", background=BG, bordercolor=ACCENT)
            s.configure("TNotebook.Tab", background=PANEL, foreground=FG, padding=(14, 7))
            s.map("TNotebook.Tab", background=[("selected", ACCENT)],
                  foreground=[("selected", "white")])

        def _build(self):
            hdr = tk.Frame(self, bg=BG, height=64)
            hdr.pack(fill="x")
            hdr.pack_propagate(False)
            box = tk.Frame(hdr, bg=BG)
            box.pack(side="left", padx=16, pady=6)
            tk.Label(box, text=APP_NAME, bg=BG, fg=ACCENT,
                     font=("Segoe UI", 17, "bold")).pack(anchor="w")
            tk.Label(box, text="liquidity sweep → displacement → fair value gap",
                     bg=BG, fg=MUTED, font=("Segoe UI", 9)).pack(anchor="w")

            stat_box = tk.Frame(hdr, bg=BG)
            stat_box.pack(side="right", padx=16, pady=10)
            self.status = tk.StringVar(value="● Ready")
            self.status_lbl = tk.Label(stat_box, textvariable=self.status, bg=BG, fg=MUTED,
                                        font=("Segoe UI", 10, "bold"), anchor="e")
            self.status_lbl.pack(anchor="e")
            self.stats_var = tk.StringVar(value="0 STRONG  ·  0 MEDIUM  ·  0 WEAK")
            tk.Label(stat_box, textvariable=self.stats_var, bg=BG, fg=MUTED,
                     font=("Consolas", 9), anchor="e").pack(anchor="e")
            tk.Frame(self, bg=ACCENT, height=2).pack(fill="x")

            nb = ttk.Notebook(self)
            nb.pack(fill="x", padx=10, pady=(10, 4))

            # --- Watchlist tab ---
            w = ttk.Frame(nb, style="P.TFrame", padding=14)
            nb.add(w, text="  Watchlist  ")
            ttk.Label(w, text="Choose what to watch", style="H.TLabel").grid(
                row=0, column=0, columnspan=4, sticky="w", pady=(0, 10))
            ttk.Label(w, text="Data source", style="P.TLabel").grid(row=1, column=0, sticky="w")
            self.feed_var = tk.StringVar(value=self.cfg.get("feed", "auto"))
            ttk.Combobox(w, textvariable=self.feed_var, width=16, state="readonly",
                         values=["auto", "yahoo", "cryptocompare", "bybit", "okx",
                                  "kraken", "coinbase", "binance", "mt5", "csv"]
                         ).grid(row=1, column=1, sticky="w", padx=6)
            ttk.Button(w, text="Test connection", style="S.TButton",
                       command=self.do_netcheck).grid(row=1, column=2, padx=6)
            ttk.Label(w, text="'auto' tries Yahoo/CryptoCompare first, then exchanges. "
                              "'mt5' uses your broker's pairs (Windows only).",
                      style="M.TLabel").grid(row=2, column=0, columnspan=4, sticky="w", pady=(2, 10))

            ttk.Label(w, text="Symbols (one per line)", style="P.TLabel").grid(row=3, column=0, sticky="nw")
            self.sym_txt = tk.Text(w, height=6, width=28, bg=FIELD, fg=FG,
                                    insertbackground=ACCENT, relief="flat",
                                    font=("Consolas", 10), highlightthickness=1,
                                    highlightbackground=BORDER, highlightcolor=ACCENT)
            self.sym_txt.grid(row=4, column=0, columnspan=2, sticky="w", pady=3)
            self.sym_txt.insert("1.0", "\n".join(self.cfg.get("symbols", [])))

            ttk.Label(w, text="Timeframes", style="P.TLabel").grid(row=3, column=2, sticky="nw", padx=(20, 0))
            self.tf_vars = {}
            tff = ttk.Frame(w, style="P.TFrame")
            tff.grid(row=4, column=2, sticky="nw", padx=(20, 0))
            for i, tf in enumerate(["5m", "15m", "30m", "1h", "4h", "1d"]):
                v = tk.BooleanVar(value=tf in self.cfg.get("timeframes", ["1h"]))
                self.tf_vars[tf] = v
                ttk.Checkbutton(tff, text=tf, variable=v).grid(row=i % 3, column=i // 3,
                                                                sticky="w", padx=6)
            ttk.Label(w, text="Crypto: ETH/USDT. Forex/metals via Yahoo: EUR/USD, XAU/USD. "
                              "MT5: use your exact broker names like ETHUSDm.",
                      style="M.TLabel").grid(row=5, column=0, columnspan=4, sticky="w", pady=(8, 0))

            # --- Sensitivity tab ---
            d = ttk.Frame(nb, style="P.TFrame", padding=14)
            nb.add(d, text="  Sensitivity  ")
            dc = self.cfg["detector"]
            ttk.Label(d, text="How strict should the setup be?", style="H.TLabel").grid(
                row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))

            self.atr_var = tk.DoubleVar(value=dc.get("min_displacement_atr", 1.5))
            ttk.Label(d, text="Min displacement (× ATR)", style="P.TLabel").grid(row=1, column=0, sticky="w", pady=4)
            ttk.Spinbox(d, from_=0.5, to=6.0, increment=0.1, textvariable=self.atr_var,
                        width=8).grid(row=1, column=1, sticky="w", padx=8)
            ttk.Label(d, text="Higher = only big moves. 1.5 loose, 2.5+ strict.",
                      style="M.TLabel").grid(row=1, column=2, sticky="w")

            self.body_var = tk.DoubleVar(value=dc.get("min_body_ratio", 0.55))
            ttk.Label(d, text="Min body / range", style="P.TLabel").grid(row=2, column=0, sticky="w", pady=4)
            ttk.Spinbox(d, from_=0.1, to=1.0, increment=0.05, textvariable=self.body_var,
                        width=8).grid(row=2, column=1, sticky="w", padx=8)
            ttk.Label(d, text="Filters out big-wick indecision candles.",
                      style="M.TLabel").grid(row=2, column=2, sticky="w")

            self.swing_var = tk.IntVar(value=dc.get("swing_right", 2))
            ttk.Label(d, text="Swing confirmation bars", style="P.TLabel").grid(row=3, column=0, sticky="w", pady=4)
            ttk.Spinbox(d, from_=1, to=6, textvariable=self.swing_var, width=8).grid(
                row=3, column=1, sticky="w", padx=8)
            ttk.Label(d, text="Higher = only major swings count as liquidity.",
                      style="M.TLabel").grid(row=3, column=2, sticky="w")

            self.mss_var = tk.BooleanVar(value=dc.get("require_structure_break", True))
            ttk.Checkbutton(d, text="Require market structure break (recommended)",
                            variable=self.mss_var).grid(row=4, column=0, columnspan=3,
                                                         sticky="w", pady=(10, 4))

            self.poll_var = tk.IntVar(value=self.cfg.get("poll_seconds", 300))
            ttk.Label(d, text="Check every (seconds)", style="P.TLabel").grid(row=5, column=0, sticky="w", pady=4)
            ttk.Spinbox(d, from_=30, to=3600, increment=30, textvariable=self.poll_var,
                        width=8).grid(row=5, column=1, sticky="w", padx=8)

            ttk.Label(d, text="Entry retest & breakout pattern", style="H.TLabel").grid(
                row=6, column=0, columnspan=3, sticky="w", pady=(16, 6))

            self.entry_pct_var = tk.DoubleVar(value=dc.get("entry_retracement_pct", 0.5))
            ttk.Label(d, text="Entry retest (% back to structure)", style="P.TLabel").grid(
                row=7, column=0, sticky="w", pady=4)
            ttk.Spinbox(d, from_=0.1, to=0.9, increment=0.05, textvariable=self.entry_pct_var,
                        width=8).grid(row=7, column=1, sticky="w", padx=8)
            ttk.Label(d, text="Reversal setups don't alert until price retraces this far "
                              "back toward the broken swing. 0.5 = halfway.",
                      style="M.TLabel").grid(row=7, column=2, sticky="w")

            self.breakout_var = tk.BooleanVar(value=self.cfg.get("breakout", {}).get("enabled", True))
            ttk.Checkbutton(d, text="Also watch for sweep -> breakout continuation setups",
                            variable=self.breakout_var).grid(row=8, column=0, columnspan=3,
                                                              sticky="w", pady=(6, 4))

            self.range_win_var = tk.IntVar(value=self.cfg.get("breakout", {}).get("range_window", 20))
            ttk.Label(d, text="Breakout range window (bars)", style="P.TLabel").grid(
                row=9, column=0, sticky="w", pady=4)
            ttk.Spinbox(d, from_=5, to=100, increment=5, textvariable=self.range_win_var,
                        width=8).grid(row=9, column=1, sticky="w", padx=8)
            ttk.Label(d, text="How far back to look for the range being swept/broken.",
                      style="M.TLabel").grid(row=9, column=2, sticky="w")

            # --- Alerts tab ---
            a = ttk.Frame(nb, style="P.TFrame", padding=14)
            nb.add(a, text="  Alerts  ")
            ac = self.cfg["alerts"]
            ttk.Label(a, text="Delivery options", style="H.TLabel").grid(
                row=0, column=0, columnspan=3, sticky="w", pady=(0, 10))
            self.sound_var = tk.BooleanVar(value=ac.get("sound", True))
            ttk.Checkbutton(a, text="Play a sound on each new setup",
                            variable=self.sound_var).grid(row=1, column=0, columnspan=3, sticky="w", pady=3)
            self.savelog_var = tk.BooleanVar(value=bool(ac.get("log_file")))
            ttk.Checkbutton(a, text="Save alerts to alerts.jsonl",
                            variable=self.savelog_var).grid(row=2, column=0, columnspan=3, sticky="w", pady=3)
            ttk.Label(a, text="Telegram (optional - get alerts on your phone)",
                      style="H.TLabel").grid(row=3, column=0, columnspan=3, sticky="w", pady=(14, 6))
            ttk.Label(a, text="Bot token", style="P.TLabel").grid(row=4, column=0, sticky="w")
            self.tg_tok = tk.StringVar(value=ac.get("telegram_token", ""))
            ttk.Entry(a, textvariable=self.tg_tok, width=44, show="*").grid(row=4, column=1, sticky="w", padx=8, pady=3)
            ttk.Label(a, text="Chat ID", style="P.TLabel").grid(row=5, column=0, sticky="w")
            self.tg_chat = tk.StringVar(value=ac.get("telegram_chat_id", ""))
            ttk.Entry(a, textvariable=self.tg_chat, width=24).grid(row=5, column=1, sticky="w", padx=8, pady=3)
            ttk.Button(a, text="Find my chat ID", style="S.TButton",
                       command=self.find_chat_id).grid(row=5, column=2, sticky="w", padx=8, pady=3)
            ttk.Button(a, text="Send test message", style="S.TButton",
                       command=self.test_tg).grid(row=6, column=1, sticky="w", padx=8, pady=6)
            ttk.Label(a, text="Message @BotFather on Telegram to create a bot and get a token. "
                              "Then message YOUR bot (press Start) before using 'Find my chat ID'.",
                      style="M.TLabel").grid(row=7, column=0, columnspan=3, sticky="w")

            # --- Activity Log tab ---
            lf = ttk.Frame(nb, style="P.TFrame", padding=10)
            nb.add(lf, text="  Activity Log  ")
            self.logbox = tk.Text(lf, height=10, bg=FIELD, fg=MUTED, relief="flat",
                                   font=("Consolas", 9), highlightthickness=1,
                                   highlightbackground=BORDER)
            self.logbox.pack(fill="both", expand=True)

            # --- controls ---
            ctl = ttk.Frame(self, padding=(10, 6))
            ctl.pack(fill="x")
            self.start_btn = ttk.Button(ctl, text="▶  Start watching", command=self.start)
            self.start_btn.pack(side="left", padx=3)
            self.bt_btn = ttk.Button(ctl, text="⟲  Backtest history", style="S.TButton",
                                      command=lambda: self.start(backtest=True))
            self.bt_btn.pack(side="left", padx=3)
            self.stop_btn = ttk.Button(ctl, text="■  Stop", style="S.TButton",
                                        command=self.stop, state="disabled")
            self.stop_btn.pack(side="left", padx=3)
            ttk.Button(ctl, text="Clear", style="S.TButton",
                       command=self.clear).pack(side="left", padx=3)
            self.count_var = tk.StringVar(value="0 alerts")
            ttk.Label(ctl, textvariable=self.count_var).pack(side="right", padx=6)

            # --- dashboard: results list (left) + setup detail & chart (right) ---
            pan = ttk.Panedwindow(self, orient="horizontal")
            pan.pack(fill="both", expand=True, padx=10, pady=(4, 10))

            left = ttk.Frame(pan)
            pan.add(left, weight=3)
            cols = ("time", "symbol", "tf", "direction", "confidence", "strength", "fvg50")
            self.tree = ttk.Treeview(left, columns=cols, show="headings", height=16)
            widths = {"time": 118, "symbol": 88, "tf": 42, "direction": 76,
                      "confidence": 112, "strength": 68, "fvg50": 95}
            heads = {"time": "Time", "symbol": "Symbol", "tf": "TF", "direction": "Direction",
                     "confidence": "Confidence", "strength": "Strength", "fvg50": "FVG 50%"}
            for c in cols:
                self.tree.heading(c, text=heads[c], command=lambda x=c: self._sort(x))
                self.tree.column(c, width=widths[c])
            self.tree.tag_configure("bearish", foreground="#ff6b6b")
            self.tree.tag_configure("bullish", foreground="#5ed99a")
            sb = ttk.Scrollbar(left, orient="vertical", command=self.tree.yview)
            self.tree.configure(yscrollcommand=sb.set)
            self.tree.pack(side="left", fill="both", expand=True)
            sb.pack(side="right", fill="y")
            self.tree.bind("<<TreeviewSelect>>", self._on_select_signal)

            right = ttk.Frame(pan, style="P.TFrame", padding=10)
            pan.add(right, weight=2)
            ttk.Label(right, text="Setup detail", style="H.TLabel").pack(anchor="w")
            self.detail_var = tk.StringVar(
                value="Select a setup on the left to see its chart and full stats.")
            ttk.Label(right, textvariable=self.detail_var, style="P.TLabel", justify="left",
                      font=("Consolas", 9)).pack(anchor="w", pady=(6, 8), fill="x")
            self.chart_canvas = tk.Canvas(right, width=440, height=230, bg=FIELD,
                                           highlightthickness=1, highlightbackground=BORDER)
            self.chart_canvas.pack(fill="both", expand=True)

        # ---------- helpers ----------
        def log(self, msg):
            self.logq.put(str(msg))

        def _drain(self):
            try:
                while True:
                    self.logbox.insert("end", self.logq.get_nowait() + "\n")
                    self.logbox.see("end")
            except queue.Empty:
                pass
            self.after(150, self._drain)

        def _sort(self, col):
            items = self.tree.get_children("")
            if col == "confidence":
                data = [(self.signal_by_item.get(k, {}).get("score", 0), k) for k in items]
                data.sort(key=lambda t: t[0], reverse=True)
            else:
                data = [(self.tree.set(k, col), k) for k in items]
                try:
                    data.sort(key=lambda t: float(str(t[0]).split()[0].replace("x", "")))
                except ValueError:
                    data.sort()
            for i, (_, k) in enumerate(data):
                self.tree.move(k, "", i)

        def collect_cfg(self):
            syms = [s.strip() for s in self.sym_txt.get("1.0", "end").splitlines() if s.strip()]
            tfs = [t for t, v in self.tf_vars.items() if v.get()]
            cfg = load_config()
            cfg.update({
                "feed": self.feed_var.get(),
                "symbols": syms,
                "timeframes": tfs or ["1h"],
                "poll_seconds": int(self.poll_var.get()),
            })
            cfg["detector"].update({
                "min_displacement_atr": float(self.atr_var.get()),
                "min_body_ratio": float(self.body_var.get()),
                "swing_left": int(self.swing_var.get()),
                "swing_right": int(self.swing_var.get()),
                "require_structure_break": bool(self.mss_var.get()),
                "entry_retracement_pct": float(self.entry_pct_var.get()),
            })
            cfg.setdefault("breakout", {}).update({
                "enabled": bool(self.breakout_var.get()),
                "range_window": int(self.range_win_var.get()),
            })
            cfg["alerts"].update({
                "console": True,
                "sound": bool(self.sound_var.get()),
                "log_file": "alerts.jsonl" if self.savelog_var.get() else "",
                "telegram_token": self.tg_tok.get().strip(),
                "telegram_chat_id": self.tg_chat.get().strip(),
            })
            return cfg

        def add_signal(self, p, chart=None):
            def ins():
                t = p.get("time")
                t = t.strftime("%d %b %H:%M") if hasattr(t, "strftime") else str(t)[:16]
                label, score = confidence_score(p)
                conf_text = f"{CONF_DOTS[label]} {label}"
                stage = p.get("stage")
                tag = {"entry": "ENTRY", "breakout_entry": "BREAKOUT ENTRY"}.get(stage, "")
                dir_text = f"{p['direction'].upper()} {tag}".strip()
                item = self.tree.insert("", 0, values=(
                    t, p["symbol"], p["timeframe"], dir_text,
                    conf_text, f"{p['displacement_atr_mult']:.1f}x",
                    f"{p['fvg_midpoint']:.6g}"), tags=(p["direction"],))
                self.signal_by_item[item] = {"payload": p, "chart": chart,
                                              "label": label, "score": score}
                self.signals.append(p)
                self.counts[label] = self.counts.get(label, 0) + 1
                self.count_var.set(f"{len(self.signals)} alerts")
                self._refresh_status_line()
            self.after(0, ins)

        def _on_select_signal(self, event=None):
            sel = self.tree.selection()
            if not sel:
                return
            rec = self.signal_by_item.get(sel[0])
            if not rec:
                return
            p, chart, label, score = rec["payload"], rec["chart"], rec["label"], rec["score"]
            t = p.get("time")
            t = t.strftime("%Y-%m-%d %H:%M") if hasattr(t, "strftime") else str(t)
            stage = p.get("stage")
            pattern = p.get("pattern", "reversal")
            kind = {"entry": " · ENTRY", "breakout_entry": " · BREAKOUT ENTRY"}.get(stage, "")
            lines = [
                f"{p['symbol']}  ·  {p['timeframe']}  ·  {p['direction'].upper()}{kind}",
                f"Confidence   : {CONF_DOTS[label]} {label}  ({score}/100)",
                f"Time         : {t}",
                "",
                f"Swept level  : {p['swept_level']:.6g}",
                f"Displacement : {p['displacement_atr_mult']:.1f}x ATR",
            ]
            if p.get("broke_structure_at") is not None:
                label_word = "Range" if pattern == "breakout" else "Structure"
                lines.append(f"{label_word:<13}: broke at {p['broke_structure_at']:.6g}")
            lines += [
                f"FVG zone     : {p['fvg_bottom']:.6g} - {p['fvg_top']:.6g}",
                f"FVG 50%      : {p['fvg_midpoint']:.6g}",
            ]
            if stage in ("entry", "breakout_entry"):
                lines.append(f"Entry level  : {p.get('entry_level'):.6g} "
                             f"({(p.get('entry_pct') or 0.5)*100:.0f}% retest - triggered)")
            elif p.get("entry_level") is not None:
                lines.append(f"Watching for : {p['entry_level']:.6g} "
                             f"({(p.get('entry_pct') or 0.5)*100:.0f}% retest)")
            lines.append(f"Last close   : {p['close']:.6g}")
            self.detail_var.set("\n".join(lines))
            draw_candle_chart(self.chart_canvas, p, chart, 440, 230)

        def _refresh_status_line(self):
            c = self.counts
            self.stats_var.set(
                f"{c.get('STRONG', 0)} STRONG  ·  {c.get('MEDIUM', 0)} MEDIUM  ·  "
                f"{c.get('WEAK', 0)} WEAK")
            if self.mode == "watching":
                if self.next_check_at:
                    remaining = max(0, int(self.next_check_at - time.time()))
                    mm, ss = divmod(remaining, 60)
                    self.status.set(f"● Watching (next check {mm:d}:{ss:02d})")
                else:
                    self.status.set("● Watching")
                self.status_lbl.configure(fg=GREEN)
            elif self.mode == "backtesting":
                self.status.set("● Backtesting...")
                self.status_lbl.configure(fg=YELLOW)
            elif self.mode == "stopping":
                self.status.set("● Stopping...")
                self.status_lbl.configure(fg=YELLOW)
            else:
                self.status.set("● Ready")
                self.status_lbl.configure(fg=MUTED)

        def _tick_status(self):
            self._refresh_status_line()
            self.after(1000, self._tick_status)

        def clear(self):
            self.tree.delete(*self.tree.get_children())
            self.signal_by_item.clear()
            self.signals.clear()
            self.seen.clear()
            self.armed.clear()
            self.counts = {"STRONG": 0, "MEDIUM": 0, "WEAK": 0}
            self.logbox.delete("1.0", "end")
            self.count_var.set("0 alerts")
            self.detail_var.set("Select a setup on the left to see its chart and full stats.")
            self.chart_canvas.delete("all")
            self._refresh_status_line()

        def do_netcheck(self):
            self.log("Testing connections, this takes a moment...")
            threading.Thread(target=lambda: netcheck(self.log), daemon=True).start()

        def test_tg(self):
            cfg = self.collect_cfg()["alerts"]
            if not cfg["telegram_token"] or not cfg["telegram_chat_id"]:
                messagebox.showerror(APP_NAME, "Enter both the bot token and chat ID first.")
                return
            def run():
                ok, msg = Notifier(cfg, log=self.log)._telegram(
                    f"{APP_NAME} test message - alerts are working.")
                if ok:
                    self.log("Telegram test sent (check your phone).")
                else:
                    self.log(f"Telegram test FAILED: {msg}")
            threading.Thread(target=run, daemon=True).start()

        def find_chat_id(self):
            token = self.tg_tok.get().strip()
            if token.lower().startswith("bot") and ":" in token:
                token = token[3:]
            if not token or ":" not in token:
                messagebox.showerror(
                    APP_NAME, "Enter a valid bot token first "
                              "(looks like 123456789:AAExxxxxxxx).")
                return

            def run():
                self.log("Looking up chats this bot has seen "
                          "(message your bot on Telegram first if you haven't)...")
                try:
                    url = f"https://api.telegram.org/bot{token}/getUpdates"
                    req = urllib.request.Request(url, headers={"Accept": "application/json"})
                    with urllib.request.urlopen(req, timeout=15) as r:
                        data = json.loads(r.read().decode("utf-8"))
                    if not data.get("ok"):
                        self.log(f"Telegram error: {data.get('description')}")
                        return
                    updates = data.get("result", [])
                    if not updates:
                        self.log("No messages seen yet. Open Telegram, find your bot "
                                  "by the username @BotFather gave it, press START "
                                  "(or send it any message), then click 'Find my chat "
                                  "ID' again.")
                        return
                    chats = {}
                    for upd in updates:
                        msg = upd.get("message") or upd.get("channel_post") or {}
                        chat = msg.get("chat")
                        if chat and "id" in chat:
                            chats[chat["id"]] = chat
                    if not chats:
                        self.log("Got updates but no chat info in them - try messaging "
                                  "the bot again and retry.")
                        return
                    for cid, chat in chats.items():
                        name = chat.get("title") or chat.get("username") or \
                               chat.get("first_name") or ""
                        self.log(f"  chat ID {cid}  ({chat.get('type')}"
                                  f"{' - ' + name if name else ''})")
                    if len(chats) == 1:
                        cid = next(iter(chats))
                        self.tg_chat.set(str(cid))
                        self.log(f"Filled in Chat ID: {cid}")
                    else:
                        self.log("Multiple chats found above - copy the right one "
                                  "into the Chat ID field.")
                except urllib.error.HTTPError as e:
                    if e.code == 401:
                        self.log("Telegram rejected the token (401). Copy it again "
                                  "from @BotFather.")
                    else:
                        self.log(f"Lookup failed: HTTP {e.code}")
                except Exception as e:
                    self.log(f"Lookup failed: {e}")

            threading.Thread(target=run, daemon=True).start()

        def start(self, backtest=False):
            cfg = self.collect_cfg()
            if not cfg["symbols"]:
                messagebox.showerror(APP_NAME, "Add at least one symbol.")
                return
            save_config(cfg)
            self.stop_evt.clear()
            self.start_btn.configure(state="disabled")
            self.bt_btn.configure(state="disabled")
            self.stop_btn.configure(state="normal")
            self.mode = "backtesting" if backtest else "watching"
            self.next_check_at = None
            self._refresh_status_line()

            def run():
                try:
                    feed = make_feed(cfg, log=self.log)
                except FeedError as e:
                    self.log(f"Data source failed: {e}")
                    self.after(0, self._done)
                    return
                note = Notifier(cfg["alerts"], log=self.log, on_signal=self.add_signal)
                try:
                    if backtest:
                        self.log("Scanning history...")
                        n = scan_once(feed, cfg, note, self.seen, armed=self.armed,
                                      backtest=True, log=self.log, stop=self.stop_evt)
                        self.log(f"\nBacktest done: {n} setups/entries found.")
                    else:
                        self.log("Priming (recording existing setups quietly)...")
                        scan_once(feed, cfg, Notifier({"console": False},
                                                       log=lambda m: None),
                                  self.seen, armed=self.armed, prime=True,
                                  log=lambda m: None, stop=self.stop_evt)
                        self.log(f"Primed with {len(self.seen)}. Watching for new setups.\n")
                        while not self.stop_evt.is_set():
                            scan_once(feed, cfg, note, self.seen, armed=self.armed,
                                      log=self.log, stop=self.stop_evt)
                            self.next_check_at = time.time() + int(cfg["poll_seconds"])
                            for _ in range(int(cfg["poll_seconds"])):
                                if self.stop_evt.is_set():
                                    break
                                time.sleep(1)
                except Exception as e:
                    self.log(f"Error: {e}")
                finally:
                    try:
                        feed.close()
                    except Exception:
                        pass
                    self.after(0, self._done)

            self.worker = threading.Thread(target=run, daemon=True)
            self.worker.start()

        def _done(self):
            self.start_btn.configure(state="normal")
            self.bt_btn.configure(state="normal")
            self.stop_btn.configure(state="disabled")
            self.mode = "ready"
            self.next_check_at = None
            self._refresh_status_line()

        def stop(self):
            self.stop_evt.set()
            self.mode = "stopping"
            self.status.set("● Stopping...")
            self.status_lbl.configure(fg=YELLOW)
            self.log("Stopping after current check...")

        def _close(self):
            self.stop_evt.set()
            try:
                save_config(self.collect_cfg())
            except Exception:
                pass
            self.destroy()

    App().mainloop()
    return 0


# ==========================================================================
def main():
    ap = argparse.ArgumentParser(description=f"{APP_NAME} - sweep/displacement/FVG scanner")
    ap.add_argument("--cli", action="store_true", help="console mode instead of the app window")
    ap.add_argument("--once", action="store_true", help="one pass then exit (cli)")
    ap.add_argument("--backtest", action="store_true", help="scan history (cli)")
    ap.add_argument("--netcheck", action="store_true", help="test which data sources you can reach")
    ap.add_argument("--config", default=CONFIG_PATH)
    ap.add_argument("--state", default=None,
                     help="path to a JSON file that remembers seen/armed setups across runs - "
                          "for a scheduled one-shot runner (e.g. GitHub Actions) instead of the "
                          "long-running watch loop. Implies a single pass, like --once.")
    args = ap.parse_args()

    if args.netcheck and not args.cli:
        netcheck()
        return 0
    if args.cli or args.once or args.backtest or args.state:
        return run_cli(args)
    return run_gui()


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""A simple BTC/USDT paper trader with a make.fun replay mode.

Standalone: reads public Binance 1-minute klines and simulates trades locally.
Competition mode: when fed bars-replay/v1 JSON lines, uses only those supplied bars.
No API keys are used, and no real orders are sent.
"""

import argparse
from collections import defaultdict, deque
from decimal import Decimal
import json
from pathlib import Path
import sys
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BINANCE_KLINES = "https://data-api.binance.vision/api/v3/klines"
SYMBOL = "BTCUSDT"
STATE_FILE = Path(__file__).with_name("bot_state.json")
PROTOCOL = "agent-fun/bars-replay/v1"
BUY_DROP = Decimal("0.01")
TAKE_PROFIT = Decimal("0.015")
STOP_LOSS = Decimal("0.02")


def fetch_market_data():
    """Return the latest BTCUSDT price and the close from 60 one-minute bars earlier."""
    query = urlencode({"symbol": SYMBOL, "interval": "1m", "limit": 61})
    request = Request(
        f"{BINANCE_KLINES}?{query}",
        headers={"Accept": "application/json", "User-Agent": "simple-btc-paper-bot/1.0"},
    )
    with urlopen(request, timeout=10) as response:
        candles = json.load(response)

    if not isinstance(candles, list) or len(candles) < 61:
        raise RuntimeError("Binance returned fewer than 61 one-minute candles")
    current = Decimal(candles[-1][4])
    one_hour_ago = Decimal(candles[-61][4])
    if current <= 0 or one_hour_ago <= 0:
        raise RuntimeError("Binance returned an invalid BTCUSDT price")
    return current, one_hour_ago, int(candles[-1][0])


def load_position():
    try:
        saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        entry = Decimal(str(saved["entry_price"]))
        return {"entry_price": entry, "entry_time": str(saved.get("entry_time", ""))}
    except (OSError, ValueError, KeyError, TypeError):
        return None


def save_position(position):
    if position is None:
        try:
            STATE_FILE.unlink()
        except FileNotFoundError:
            pass
        return

    temporary = STATE_FILE.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            {
                "entry_price": str(position["entry_price"]),
                "entry_time": position["entry_time"],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(STATE_FILE)


def paper_cycle():
    current, reference, candle_open_ms = fetch_market_data()
    one_hour_change = current / reference - Decimal(1)
    position = load_position()

    if position is None:
        if one_hour_change <= -BUY_DROP:
            position = {
                "entry_price": current,
                "entry_time": str(candle_open_ms),
            }
            save_position(position)
            action = "PAPER BUY"
        else:
            action = "HOLD"
    else:
        entry = position["entry_price"]
        if current >= entry * (Decimal(1) + TAKE_PROFIT):
            save_position(None)
            action = "PAPER SELL take-profit"
        elif current <= entry * (Decimal(1) - STOP_LOSS):
            save_position(None)
            action = "PAPER SELL stop-loss"
        else:
            action = "HOLD"

    print(
        f"{SYMBOL} price={current:.2f} "
        f"1h_change={one_hour_change * 100:+.2f}% action={action}"
    )


def estimate_entry_from_position(position, symbol_info, quote_scale):
    qty = int(position["qty"])
    if qty <= 0:
        return 0
    cost = int(position["cost_quote_units"])
    numerator = cost * 10 ** (symbol_info["qty_scale"] + symbol_info["price_scale"])
    denominator = qty * 10**quote_scale
    return numerator // denominator if denominator else 0


def run_replay(hello, stream):
    """Speak bars-replay/v1 using the bars supplied by the competition harness."""
    symbols = {
        item["symbol"]: item
        for item in hello["symbols"]
    }
    history = defaultdict(lambda: deque(maxlen=90))
    entry_prices = {}
    quote_scale = int(hello["capital"]["quote_scale"])
    caps = hello["caps"]
    fees = hello["fees"]
    warmup_bars = int(hello.get("warmup_bars", 0))

    for raw_line in stream:
        if not raw_line.strip():
            continue
        message = json.loads(raw_line)
        kind = message.get("type")
        if kind == "done":
            return
        if kind != "batch":
            raise ValueError(f"unexpected replay message: {kind}")

        for fill in message["fills"]:
            if fill["status"] != "filled":
                continue
            symbol = fill["symbol"]
            if fill["side"] == "buy":
                entry_prices[symbol] = int(fill["price"])
            elif fill["side"] == "sell":
                entry_prices.pop(symbol, None)

        bars = message["bars"]
        for bar in bars:
            history[bar["symbol"]].append((int(bar["t"]), int(bar["c"])))

        orders = []
        if int(message["seq"]) >= warmup_bars:
            positions = {
                item["symbol"]: item
                for item in message["account"]["positions"]
            }
            for bar in bars:
                symbol = bar["symbol"]
                info = symbols.get(symbol)
                if info is None:
                    continue
                current = int(bar["c"])
                held = positions.get(symbol)

                if held is not None:
                    entry = entry_prices.get(symbol)
                    if entry is None:
                        entry = estimate_entry_from_position(
                            held, info, quote_scale
                        )
                    if entry > 0 and (
                        current * 200 >= entry * 203
                        or current * 100 <= entry * 98
                    ):
                        orders.append(
                            {
                                "side": "sell",
                                "client_id": f"sell-{message['seq']}",
                                "symbol": symbol,
                                "qty": str(int(held["qty"])),
                                "max_slippage_bps": 1000,
                            }
                        )
                        break
                    continue

                cutoff = int(bar["t"]) - 60 * 60 * 1000
                reference = None
                for timestamp, old_close in reversed(history[symbol]):
                    if timestamp <= cutoff:
                        reference = old_close
                        break
                if reference is None or reference <= 0:
                    continue
                if current * 100 > reference * 99:
                    continue

                account = message["account"]
                cash = int(account["cash_quote_units"])
                equity = int(account["equity_quote_units"])
                max_weight = int(caps["max_weight_bps"])
                notional = min(cash, equity * max_weight // 10_000)
                if notional >= int(fees["min_notional_quote_units"]):
                    orders.append(
                        {
                            "side": "buy",
                            "client_id": f"buy-{message['seq']}",
                            "symbol": symbol,
                            "notional_quote_units": str(notional),
                            "max_slippage_bps": 1000,
                        }
                    )
                    break

        reply = {"type": "reply", "seq": int(message["seq"]), "orders": orders}
        sys.stdout.write(json.dumps(reply, separators=(",", ":")) + "\n")
        sys.stdout.flush()


def run_standalone(loop):
    while True:
        paper_cycle()
        if not loop:
            return
        time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="Simple BTC/USDT paper trader")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="repeat the Binance public-data paper check every 60 seconds",
    )
    args = parser.parse_args()

    if sys.stdin.isatty():
        run_standalone(args.loop)
        return

    first_line = sys.stdin.readline()
    if not first_line:
        run_standalone(args.loop)
        return

    first = json.loads(first_line)
    if first.get("type") == "hello" and first.get("protocol") == PROTOCOL:
        run_replay(first, sys.stdin)
    else:
        run_standalone(args.loop)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)

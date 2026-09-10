#!/usr/bin/env python3
"""OKX daily loss auto-halt (mandate schema has no daily-loss field).

Tracks UTC-day opening equity under ~/.vibe-trading/live/okx/ and trips the
Vibe-Trading kill switch when drawdown from that baseline reaches the cap in
daily_loss_policy.json (default $50).

Always forces the OKX-whitelist proxy so cron (which does not inherit the
LiveRunner systemd Environment=) cannot call OKX from the residential IP.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Cron has no proxy — pin whitelist egress before any OKX import/network call.
_PROXY = os.environ.get("OKX_EGRESS_PROXY", "http://100.64.0.5:8888")
for _k in (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
):
    os.environ.setdefault(_k, _PROXY)
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost,::1")
os.environ.setdefault("no_proxy", os.environ["NO_PROXY"])

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "agent"))

from src.live.halt import halt_flag_set, trip_halt  # noqa: E402
from src.live.paths import broker_dir  # noqa: E402
from src.trading.connectors.okx import sdk  # noqa: E402


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _equity(cfg) -> float:
    snap = sdk.get_account_snapshot(cfg)
    acct = (snap or {}).get("account") or {}
    raw = acct.get("total_equity")
    if raw is None:
        client = sdk._account_client(cfg)
        resp = client.get_account_balance()
        if str(resp.get("code")) != "0":
            raise RuntimeError(f"OKX balance error: {resp}")
        data = (resp.get("data") or [{}])[0]
        raw = data.get("totalEq") or 0
    return float(raw or 0)


def main() -> int:
    broker = "okx"
    base = broker_dir(broker)
    policy_path = base / "daily_loss_policy.json"
    state_path = base / "daily_loss_state.json"
    policy = {"daily_loss_cap_usd": 50.0}
    if policy_path.is_file():
        policy.update(json.loads(policy_path.read_text()))

    cap = float(policy.get("daily_loss_cap_usd") or 50.0)
    cfg = sdk.load_config()
    from src.trading.connectors.okx.sdk import OKXConfig

    live = OKXConfig(
        api_key=cfg.api_key,
        api_secret=cfg.api_secret,
        passphrase=cfg.passphrase,
        profile="live",
        host=cfg.host,
        timeout=cfg.timeout,
        readonly=False,
    )

    equity = _equity(live)
    day = _utc_day()
    state: dict = {}
    if state_path.is_file():
        state = json.loads(state_path.read_text())

    if state.get("utc_day") != day:
        state = {"utc_day": day, "open_equity_usd": equity, "halted_for_day": False}

    open_eq = float(state.get("open_equity_usd") or equity)
    loss = max(0.0, open_eq - equity)
    state["last_equity_usd"] = equity
    state["loss_usd"] = loss
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, indent=2) + "\n")
    state_path.chmod(0o600)

    print(f"day={day} open={open_eq:.4f} equity={equity:.4f} loss={loss:.4f} cap={cap:.2f}")

    if loss + 1e-9 >= cap:
        if halt_flag_set(broker=broker) or state.get("halted_for_day"):
            print("already halted")
            return 0
        trip_halt(
            by="okx-daily-loss-halt",
            reason=f"daily loss {loss:.2f} >= {cap:.2f}",
            broker=broker,
        )
        state["halted_for_day"] = True
        state_path.write_text(json.dumps(state, indent=2) + "\n")
        print("HALT tripped")
        return 0

    print("ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

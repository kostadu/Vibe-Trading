#!/usr/bin/env python3
"""OKX LiveRunner for Vibe-Trading (SDK + mandate-gated path).

Vibe's built-in LiveRunner only wires Robinhood remote MCP. This daemon builds
the same LiveRunner class with OKX python-okx reads and mandate-gated
``trading.service.place_order`` / ``cancel_order`` writes, then drives an
autonomous agent tick on a crypto 24/7 schedule.

Stop: systemctl --user stop vibe-okx-liverunner
Halt trading: vibe-trading connector halt   (or trip kill switch)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
AGENT = ROOT / "agent"
sys.path.insert(0, str(AGENT))

os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost,::1")
os.environ.setdefault("no_proxy", os.environ["NO_PROXY"])

from src.live.audit import write_live_action  # noqa: E402
from src.live.halt import halt_flag_set  # noqa: E402
from src.live.mandate.store import load_mandate  # noqa: E402
from src.live.runtime.reconcile import _persist_state, _utc_now_iso_ms, reconcile  # noqa: E402
from src.live.runtime.runner import LiveRunner  # noqa: E402
from src.live.runtime.scheduler import Scheduler  # noqa: E402
from src.live.runtime.triggers import Trigger  # noqa: E402
from src.trading.connectors.okx import sdk as okx_sdk  # noqa: E402
from src.trading.connectors.okx.sdk import OKXConfig  # noqa: E402
from src.trading.service import cancel_order, place_order  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [okx-liverunner] %(message)s",
)
logger = logging.getLogger("okx-liverunner")

BROKER = "okx"
PROFILE = "okx-live-trade"
# 30-minute ticks — low-churn regime (less LLM noise / less overtrading).
TICK_INTERVAL_MS = 30 * 60 * 1000
# Back off when LLM/provider is down (e.g. 402 Insufficient Balance).
LLM_BACKOFF_PATH = Path.home() / ".vibe-trading" / "live" / "okx" / "llm_backoff.json"
LLM_BACKOFF_SEC = 30 * 60
VIBE_BIN = ROOT / ".venv" / "bin" / "vibe-trading"
STATE_DIR = Path.home() / ".vibe-trading" / "live" / "okx"
PID_FILE = STATE_DIR / "liverunner.pid"
LOG_TAIL = STATE_DIR / "liverunner-last-tick.log"


def _live_cfg() -> OKXConfig:
    base = okx_sdk.load_config()
    return OKXConfig(
        api_key=base.api_key,
        api_secret=base.api_secret,
        passphrase=base.passphrase,
        profile="live",
        host=base.host or "https://eea.okx.com",
        expected_uid=base.expected_uid,
        timeout=base.timeout,
        readonly=False,
    )


def _read_balance() -> dict[str, Any]:
    snap = okx_sdk.get_account_snapshot(_live_cfg())
    acct = snap.get("account") if isinstance(snap, dict) else None
    return dict(acct) if isinstance(acct, dict) else {"raw": snap}


# Residual dust after market sells (e.g. 7e-7 ETH) is not a tradable position
# and must not trip UNKNOWN_FILL vs a rounded flat book.
_DUST_QTY = 1e-6


def _read_positions() -> list[dict[str, Any]]:
    """Spot holdings live in account details (OKX positions API is for derivatives)."""
    snap = okx_sdk.get_account_snapshot(_live_cfg())
    details = ((snap.get("account") or {}).get("details") or []) if isinstance(snap, dict) else []
    cash = {"USDC", "USDT", "EUR", "USD", "USDG"}
    rows: list[dict[str, Any]] = []
    for d in details:
        if not isinstance(d, dict):
            continue
        ccy = str(d.get("currency") or d.get("ccy") or "").upper()
        qty = float(d.get("equity") or d.get("eq") or 0)
        if not ccy or ccy in cash or qty < _DUST_QTY:
            continue
        rows.append(
            {
                "symbol": ccy,
                "quantity": qty,
                "available": float(d.get("available") or d.get("availBal") or qty),
                "asset_class": "crypto",
            }
        )
    return rows


def _accept_broker_truth(reason: str) -> None:
    """Advance runtime_state to current broker snapshot (operator/agent-ack path).

    LiveRunner reconcile treats any position delta vs the prior file as
    UNKNOWN_FILL and blocks forever. Agent ticks place real orders outside that
    file, so after a successful tick we must adopt broker truth as the new
    baseline — otherwise the next pre-tick reconcile locks the runner.
    """
    try:
        orders = _read_orders()
        positions = _read_positions()
        balance = _read_balance()
        ts = _utc_now_iso_ms()
        _persist_state(BROKER, orders, positions, balance, ts)
        logger.info(
            "accepted broker truth (%s): positions=%s open_orders=%d",
            reason,
            [f"{p.get('symbol')}={p.get('quantity')}" for p in positions],
            len(orders),
        )
    except Exception as exc:
        logger.warning("failed to accept broker truth (%s): %s", reason, exc)


def _reconcile_okx(
    broker: str,
    read_positions: Any,
    read_balance: Any,
    read_open_orders: Any,
) -> Any:
    """Reconcile with one auto-heal for intentional agent fills.

    If the only blocking deltas are position UNKNOWN_FILLs (no mid-order
    ambiguity), adopt broker truth and re-reconcile once. OKX agent ticks are
    mandate-gated; those fills are expected and must not permanently halt.
    """
    report = reconcile(broker, read_positions, read_balance, read_open_orders)
    if getattr(report, "is_safe", False):
        return report
    deltas = list(getattr(report, "deltas", ()) or ())
    blocking = [d for d in deltas if getattr(d, "kind", "") in ("unknown_fill", "mid_order_ambiguous")]
    if not blocking:
        return report
    if any(getattr(d, "kind", "") == "mid_order_ambiguous" for d in blocking):
        return report
    if any(getattr(d, "subject", "") != "position" for d in blocking):
        return report
    logger.warning(
        "reconcile unsafe on position fills only — auto-accepting broker truth: %s",
        [(d.kind, d.identity) for d in blocking],
    )
    _accept_broker_truth("auto-heal unknown_fill")
    return reconcile(broker, read_positions, read_balance, read_open_orders)


def _read_orders() -> list[dict[str, Any]]:
    raw = okx_sdk.get_open_orders(_live_cfg())
    orders = raw.get("open_orders") if isinstance(raw, dict) else None
    if isinstance(orders, list):
        return [dict(o) for o in orders if isinstance(o, dict)]
    return []


def _submit(order: dict[str, Any]) -> dict[str, Any]:
    """Halt-sweep / flatten write surface — always mandate-gated via service."""
    if order.get("action") == "cancel":
        return cancel_order(
            str(order.get("order_id") or ""),
            PROFILE,
            symbol=order.get("symbol"),
            session_id="okx-liverunner",
        )
    return place_order(
        str(order.get("symbol") or ""),
        PROFILE,
        side=str(order.get("side") or "sell"),
        quantity=order.get("quantity"),
        notional=order.get("notional"),
        order_type=str(order.get("order_type") or "market"),
        limit_price=order.get("limit_price"),
        session_id="okx-liverunner",
    )


def _augment_prompt(base: str) -> str:
    return (
        base
        + "\n\n=== OPERATIONAL RULES (OKX EEA) — LOW-CHURN REGIME ===\n"
        "- Connector profile: okx-live-trade\n"
        "- Trade ONLY BTC-USDC and ETH-USDC (USDT pairs are compliance-blocked)\n"
        "- Spot only, no leverage / no perps\n"
        "- Tool budget: ONE pass only — trading_account + trading_positions + "
        "trading_orders + quotes for BTC-USDC and ETH-USDC (and history if needed). "
        "Do NOT re-call the same read tools. Then decide and stop.\n"
        "- DEFAULT IS HOLD. Trade only on a clear, tool-verified edge.\n"
        "- Target ~30%+ USDC cash buffer; do NOT push exposure to 100% of equity\n"
        "- Max 1 action per tick; prefer 0–1 trades/day unless invalidation\n"
        "- Rotate BTC↔ETH only if relative strength gap ≥ ~2% over 24h "
        "(tool-verified) AND the move pays for fees; otherwise HOLD\n"
        "- Add only on clear dip/breakout with room under exposure cap\n"
        "- Trim on structure break or to restore the cash buffer — not on mild red\n"
        "- Cancel stale unfilled limits that no longer match the thesis\n"
        "- Never invent prices; if tools fail, HOLD and say so in plain text\n"
        "- End with a short final answer: HOLD or the single order you placed\n"
        "- Respect remaining daily trade count and exposure headroom\n"
    )


def _llm_backoff_active() -> bool:
    try:
        if not LLM_BACKOFF_PATH.is_file():
            return False
        raw = json.loads(LLM_BACKOFF_PATH.read_text(encoding="utf-8"))
        until = float(raw.get("until_ts") or 0)
        return until > datetime.now(timezone.utc).timestamp()
    except Exception:
        return False


def _set_llm_backoff(reason: str, seconds: int = LLM_BACKOFF_SEC) -> None:
    until = datetime.now(timezone.utc).timestamp() + max(60, int(seconds))
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    LLM_BACKOFF_PATH.write_text(
        json.dumps(
            {
                "until_ts": until,
                "reason": reason[:500],
                "set_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    logger.warning("LLM backoff %ss: %s", seconds, reason[:200])


def _clear_llm_backoff() -> None:
    try:
        LLM_BACKOFF_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def _is_llm_hard_fail(text: str) -> bool:
    low = (text or "").lower()
    needles = (
        "insufficient balance",
        "error code: 402",
        "provider_stream_error",
        "invalid_api_key",
        "authentication",
        "rate limit",
        "429",
    )
    return any(n in low for n in needles)


async def _agent_caller(session_id: str, prompt: str) -> dict[str, Any]:
    """Drive one autonomous vibe-trading run (has full tool registry + mandate gate)."""
    if not VIBE_BIN.is_file():
        raise RuntimeError(f"vibe-trading binary missing: {VIBE_BIN}")

    # Daily-loss helper before agent spend
    helper = ROOT / "scripts" / "okx-daily-loss-halt.py"
    if helper.is_file():
        proc_h = await asyncio.create_subprocess_exec(
            str(ROOT / ".venv" / "bin" / "python"),
            str(helper),
            cwd=str(ROOT),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        await proc_h.communicate()

    if halt_flag_set(broker=BROKER) or halt_flag_set(broker=None):
        return {"status": "blocked", "reason": "halt flag set before agent invoke"}

    if _llm_backoff_active():
        logger.info("skipping tick — LLM backoff active")
        return {"status": "skipped", "reason": "llm_backoff"}

    full_prompt = _augment_prompt(prompt)
    env = os.environ.copy()
    env["NO_PROXY"] = "127.0.0.1,localhost,::1"
    env["no_proxy"] = env["NO_PROXY"]
    # Slightly looser content-filter warning; hard rejects still apply.
    env.setdefault("CONTENT_FILTER_WARNING_THRESHOLD", "0.08")

    logger.info("invoking autonomous tick session=%s", session_id)
    proc = await asyncio.create_subprocess_exec(
        str(VIBE_BIN),
        "run",
        "-p",
        full_prompt,
        "--max-iter",
        "14",
        "--no-rich",
        cwd=str(ROOT),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out_b, _ = await proc.communicate()
    text = (out_b or b"").decode("utf-8", errors="replace")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_TAIL.write_text(text[-12000:], encoding="utf-8")
    result = {
        "status": "ok" if proc.returncode == 0 else "error",
        "exit_code": proc.returncode,
        "session_id": session_id,
        "output_tail": text[-2000:],
    }
    logger.info("tick finished exit=%s bytes=%s", proc.returncode, len(text))
    if proc.returncode == 0:
        _clear_llm_backoff()
    elif _is_llm_hard_fail(text):
        _set_llm_backoff(text[-400:])
    # Always adopt broker truth after a tick. Mandate-gated place_order can
    # succeed even when vibe-trading exits non-zero (e.g. grounding/filter),
    # and skipping sync leaves the next reconcile stuck on UNKNOWN_FILL.
    _accept_broker_truth(f"post-tick exit={proc.returncode} {session_id}")
    return result


def _write_pid() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()) + "\n", encoding="utf-8")


def main() -> int:
    mandate = load_mandate(BROKER)
    if mandate is None:
        logger.error("no committed OKX mandate — refuse to start")
        return 2
    if halt_flag_set(broker=BROKER) or halt_flag_set(broker=None):
        logger.error("kill switch tripped — resume before starting")
        return 3

    # Quick connectivity check
    try:
        bal = _read_balance()
        logger.info("OKX balance snapshot status=%s", bal.get("status"))
    except Exception as exc:
        logger.error("OKX connectivity failed: %s", exc)
        return 4

    _write_pid()
    session_id = f"okx-liverunner-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

    runner_holder: dict[str, Any] = {}

    async def _on_fire(_job: Any) -> None:
        runner = runner_holder.get("runner")
        if runner is not None:
            result = await runner.run_once()
            logger.info("tick outcome=%s reason=%s", result.get("outcome"), result.get("reason"))

    scheduler = Scheduler(_on_fire)
    runner = LiveRunner(
        BROKER,
        agent_caller=_agent_caller,
        reconcile_fn=_reconcile_okx,
        read_positions=_read_positions,
        read_balance=_read_balance,
        read_open_orders=_read_orders,
        submit_fn=_submit,
        write_audit_fn=write_live_action,
        scheduler=scheduler,
        triggers=[Trigger.market("crypto")],
        session_id=session_id,
        market_watch_ms=TICK_INTERVAL_MS,
    )
    runner_holder["runner"] = runner

    logger.info(
        "OKX LiveRunner starting session=%s interval_ms=%s mandate_expires=%s",
        session_id,
        TICK_INTERVAL_MS,
        mandate.consent.expires_at,
    )

    async def _boot() -> None:
        first = await runner.run_once()
        logger.info("bootstrap tick outcome=%s", first.get("outcome"))
        # run_loop must run ON the event loop (scheduler.start uses get_running_loop).
        runner.run_loop()
        sched = runner._scheduler
        task = getattr(sched, "_task", None) if sched is not None else None
        if task is not None:
            await task
        else:
            # Fallback: keep process alive if scheduler task missing.
            while True:
                await asyncio.sleep(60)

    try:
        asyncio.run(_boot())
    except KeyboardInterrupt:
        logger.info("stopped by KeyboardInterrupt")
        try:
            runner.stop_loop()
        except Exception:
            pass
    finally:
        try:
            PID_FILE.unlink(missing_ok=True)
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

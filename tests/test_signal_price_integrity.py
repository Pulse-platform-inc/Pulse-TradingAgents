"""Price integrity: entries anchor to the live quote, stop/target/RR always
present on directional signals, and the tracker expires played-out signals.

Regression for the July 2026 incident where the trader LLM wrote pre-split
training-data prices (NVDA entry $920 vs live $190) and they reached the site.
"""

import sqlite3
from unittest.mock import patch

from tradingagents.agents.utils.agent_utils import yf_symbol


def _state(pm: str, trader: str) -> dict:
    return {"final_trade_decision": pm, "trader_investment_plan": trader}


def _normalize(pm, trader, live):
    with patch("api.signals_engine.get_live_price", return_value=live):
        from api.signals_engine import normalize_signal

        return normalize_signal("NVDA", "stocks", _state(pm, trader))


def test_hallucinated_entry_snaps_to_live_price():
    sig = _normalize(
        "**Rating**: Sell\n**Price Target**: 185.0",
        "**Action**: Sell\n**Entry Price**: 920.0\n**Stop Loss**: 950.0",
        190.0,
    )
    assert sig["entry_price"] == 190.0  # not 920
    assert sig["stop_loss"] == 199.5  # fallback: entry * 1.05 (950 rejected)
    assert sig["price_target"] == 185.0  # sane: below entry, within 2x
    assert sig["rr"] is not None and sig["rr"] > 0


def test_sane_llm_entry_is_kept():
    sig = _normalize(
        "**Rating**: Buy\n**Price Target**: 210.0",
        "**Action**: Buy\n**Entry Price**: 188.5\n**Stop Loss**: 180.0",
        190.0,
    )
    assert sig["entry_price"] == 188.5  # within 5% of live — trader's level kept
    assert sig["stop_loss"] == 180.0
    assert sig["rr"] is not None


def test_missing_stop_and_target_get_fallbacks():
    sig = _normalize("**Rating**: Buy", "**Action**: Buy", 100.0)
    assert sig["entry_price"] == 100.0
    assert sig["stop_loss"] == 95.0
    assert sig["price_target"] == 110.0
    assert sig["rr"] == 2.0


def test_wrong_direction_levels_are_replaced():
    # Buy with stop above entry and target below entry: both nonsense.
    sig = _normalize(
        "**Rating**: Buy\n**Price Target**: 80.0",
        "**Action**: Buy\n**Entry Price**: 100.0\n**Stop Loss**: 120.0",
        100.0,
    )
    assert sig["stop_loss"] == 95.0
    assert sig["price_target"] == 110.0


def test_sub_dollar_precision_preserved():
    sig = _normalize("**Rating**: Sell", "**Action**: Sell", 0.072335)
    assert sig["entry_price"] == 0.07233  # 4 significant digits, not 0.07


def test_arb_maps_to_arbitrum_not_scam_token():
    assert yf_symbol("ARB", "crypto") == "ARB11841-USD"
    assert yf_symbol("BTC", "crypto") == "BTC-USD"
    assert yf_symbol("NVDA", "stocks") == "NVDA"


def test_tracker_expires_signal_outside_band(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADINGAGENTS_SIGNALS_DB_PATH", str(tmp_path / "s.db"))
    import importlib

    import api.config, api.database, api.scheduler

    importlib.reload(api.config)
    importlib.reload(api.database)
    api.database.init_db()
    conn = api.database.get_db_connection()
    base = "INSERT INTO trading_signals (id, ticker, asset_type, signal_type, confidence, price_target, entry_price, stop_loss, reasoning_summary, generated_at, status) VALUES (?, ?, 'stocks', ?, 0.9, ?, ?, ?, 'x', ?, 'active')"
    # buy: live 94 <= stop 95 -> expired; sell stays active (live within band)
    conn.execute(base, ("a", "AAA", "buy", 110.0, 100.0, 95.0, "2026-07-08 10:00:00"))
    conn.execute(base, ("b", "BBB", "sell", 90.0, 100.0, 105.0, "2026-07-08 10:00:00"))
    conn.commit()
    conn.close()

    with patch.object(api.scheduler, "get_live_price", side_effect=lambda t, a: {"AAA": 94.0, "BBB": 99.0}[t]), \
         patch.object(api.scheduler, "get_db_connection", api.database.get_db_connection):
        api.scheduler.SignalScheduler().update_signal_statuses()

    conn = api.database.get_db_connection()
    statuses = dict(
        conn.execute("SELECT ticker, status FROM trading_signals").fetchall()
    )
    conn.close()
    assert statuses == {"AAA": "expired", "BBB": "active"}

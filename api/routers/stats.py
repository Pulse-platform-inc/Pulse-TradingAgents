from fastapi import APIRouter, Request

from api.auth import get_user_claims_async
from api.database import get_db_connection
from api.models import StatsResponse

router = APIRouter(tags=["stats"])


@router.get("/signals-ms/stats", response_model=StatsResponse)
async def get_stats(request: Request):
    await get_user_claims_async(request)  # auth required, no quota cost

    conn = get_db_connection()
    try:
        # Mirror the feed: summarize the latest signal per ticker (buy/sell only).
        # A calendar-day filter read 0 between midnight and the nightly
        # regeneration even while those same signals were still on the feed.
        rows = conn.execute(
            """
            SELECT s1.signal_type, s1.confidence FROM trading_signals s1
            INNER JOIN (
                SELECT ticker, MAX(generated_at) as max_gen
                FROM trading_signals GROUP BY ticker
            ) s2 ON s1.ticker = s2.ticker AND s1.generated_at = s2.max_gen
            WHERE s1.signal_type IN ('buy','sell')
            """
        ).fetchall()

        signals_today = len(rows)
        buy = sum(1 for r in rows if r["signal_type"] == "buy")
        sell = sum(1 for r in rows if r["signal_type"] == "sell")
        hold = 0
        avg_conf = (
            round(sum(r["confidence"] for r in rows) / signals_today, 2)
            if signals_today
            else 0.0
        )
        watchlist_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM watchlist_tickers"
        ).fetchone()["cnt"]

        return StatsResponse(
            signals_today=signals_today,
            buy_signals=buy,
            sell_signals=sell,
            hold_signals=hold,
            avg_confidence=avg_conf,
            active_watchlist=watchlist_count,
        )
    finally:
        conn.close()

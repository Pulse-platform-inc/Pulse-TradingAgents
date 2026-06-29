import string

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request

from api.auth import get_user_claims_async
from api.scheduler import redis_client, scheduler

router = APIRouter(tags=["analyze"])

_TICKER_CHARS = set(string.ascii_uppercase + string.digits + "-")
_COOLDOWN_SECONDS = 300  # one analysis per ticker per user per 5 minutes


@router.post("/signals-ms/analyze")
async def analyze_on_demand(
    request: Request,
    background_tasks: BackgroundTasks,
    ticker: str = Query(..., description="Ticker to analyze (e.g. AAPL, BTC)"),
    asset_type: str = Query("stocks", description="'stocks' or 'crypto'"),
):
    identity = await get_user_claims_async(request)
    if identity.tier != "pro" and not identity.is_admin:
        raise HTTPException(status_code=403, detail="Pro required")

    ticker_upper = ticker.strip().upper()
    if not (1 <= len(ticker_upper) <= 10 and set(ticker_upper) <= _TICKER_CHARS):
        raise HTTPException(status_code=400, detail="Invalid ticker format")

    asset_type = asset_type.strip().lower()
    if asset_type not in ("stocks", "crypto"):
        raise HTTPException(
            status_code=400, detail="asset_type must be 'stocks' or 'crypto'"
        )

    cooldown_key = f"analyze_cd:{identity.user_id}:{ticker_upper}"
    try:
        if await redis_client.exists(cooldown_key):
            ttl = await redis_client.ttl(cooldown_key)
            raise HTTPException(
                status_code=429,
                detail=f"Analysis already queued for {ticker_upper}. Retry in {ttl}s.",
            )
        await redis_client.setex(cooldown_key, _COOLDOWN_SECONDS, "1")
    except HTTPException:
        raise
    except Exception:
        pass  # Redis unavailable — allow through rather than block

    background_tasks.add_task(
        scheduler.execute_agent_run, ticker_upper, asset_type, None
    )
    return {"status": "triggered", "ticker": ticker_upper, "asset_type": asset_type}

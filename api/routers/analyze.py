import re

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request

from api.auth import get_user_claims_async
from api.scheduler import scheduler

router = APIRouter(tags=["analyze"])

_TICKER_RE = re.compile(r"^[A-Z0-9\-]{1,10}$")


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
    if not _TICKER_RE.match(ticker_upper):
        raise HTTPException(status_code=400, detail="Invalid ticker format")

    asset_type = asset_type.strip().lower()
    if asset_type not in ("stocks", "crypto"):
        raise HTTPException(
            status_code=400, detail="asset_type must be 'stocks' or 'crypto'"
        )

    background_tasks.add_task(
        scheduler.execute_agent_run, ticker_upper, asset_type, None
    )
    return {"status": "triggered", "ticker": ticker_upper, "asset_type": asset_type}

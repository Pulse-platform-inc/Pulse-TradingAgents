from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request

from api.auth import get_user_claims_async
from api.scheduler import scheduler

router = APIRouter(tags=["admin"])


@router.post("/signals-ms/generate")
async def force_generate(
    request: Request,
    background_tasks: BackgroundTasks,
    ticker: Optional[str] = Query(None, description="Ticker to force-generate"),
):
    identity = await get_user_claims_async(request)

    if ticker:
        # Pro users can trigger analysis for a specific ticker (their feature)
        if identity.tier != "pro" and not identity.is_admin:
            raise HTTPException(status_code=403, detail="Pro required")
    else:
        # Full-cycle (all tickers) is admin-only
        if not identity.is_admin:
            raise HTTPException(status_code=403, detail="Admin required")

    background_tasks.add_task(scheduler.run_scheduler_cycle, ticker)
    return {"status": "triggered", "ticker": ticker or "all"}

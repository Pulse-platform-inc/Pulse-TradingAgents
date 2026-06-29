from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query, Request

from api.auth import get_user_claims_async
from api.config import INTERNAL_API_KEY
from api.scheduler import scheduler

router = APIRouter(tags=["admin"])


def _require_internal_key(x_internal_api_key: str = Header(...)) -> None:
    if not INTERNAL_API_KEY or x_internal_api_key != INTERNAL_API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")


@router.post("/signals-ms/generate")
async def force_generate(
    request: Request,
    background_tasks: BackgroundTasks,
    ticker: Optional[str] = Query(None, description="Ticker to force-generate"),
    x_internal_api_key: Optional[str] = Header(None),
):
    if ticker:
        # Pro users can trigger analysis for a specific ticker (their feature)
        _, tier = await get_user_claims_async(request)
        if tier != "pro":
            raise HTTPException(status_code=403, detail="Pro required")
    else:
        # Full-cycle trigger (all tickers) is operator-only
        _require_internal_key(x_internal_api_key or "")
    background_tasks.add_task(scheduler.run_scheduler_cycle, ticker)
    return {"status": "triggered", "ticker": ticker or "all"}

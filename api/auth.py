import datetime
import hashlib
import json
import logging
import urllib.error as _ue
import urllib.request as _ur
from typing import NamedTuple, Optional

import jwt
from fastapi import HTTPException, Request

from api.config import (
    AUTH_SERVICE_URL,
    DEV_BYPASS_AUTH,
    FREE_TIER_QUOTA_LIMIT,
    JWT_ALGORITHM,
    JWT_SECRET,
)
from api.models import EntitlementBlock

logger = logging.getLogger("pulse-trading-signals-service")


def _get_redis():
    from api.scheduler import (
        redis_client,
    )  # imported lazily to avoid circular import at module load

    return redis_client


class Identity(NamedTuple):
    user_id: str
    tier: str  # "free" | "pro"
    is_admin: bool
    signals_remaining: Optional[int]  # None = unlimited (Pro)
    token: str  # raw JWT — never persisted to Redis


async def _resolve_identity_from_auth_service(token: str) -> Identity:
    if DEV_BYPASS_AUTH in ("pro", "free"):
        logger.warning(
            "DEV_BYPASS_AUTH active — skipping auth service (development only)"
        )
        payload = jwt.decode(token, options={"verify_signature": False})
        return Identity(
            user_id=str(payload.get("sub") or "dev-user"),
            tier=DEV_BYPASS_AUTH,
            is_admin=False,
            signals_remaining=FREE_TIER_QUOTA_LIMIT
            if DEV_BYPASS_AUTH == "free"
            else None,
            token=token,
        )

    cache_key = f"identity:{hashlib.sha256(token.encode()).hexdigest()}"
    try:
        cached = await _get_redis().get(cache_key)
        if cached:
            user_id, tier, admin_flag, sig_rem = cached.split(":", 3)
            signals_remaining = None if sig_rem == "" else int(sig_rem)
            return Identity(
                user_id=user_id,
                tier=tier,
                is_admin=admin_flag == "1",
                signals_remaining=signals_remaining,
                token=token,  # always from the live request, never from cache
            )
    except Exception:
        pass

    try:
        req = _ur.Request(
            f"{AUTH_SERVICE_URL}/auth-ms/me/entitlements",
            headers={"Authorization": f"Bearer {token}"},
        )
        with _ur.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        tier = "pro" if data.get("is_pro") else "free"
        is_admin = bool(data.get("is_admin", False))
        signals_remaining = data.get("signals_remaining_today")  # None = unlimited
    except _ue.HTTPError as e:
        if e.code == 401:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        raise HTTPException(status_code=503, detail="Auth service unavailable")
    except Exception:
        raise HTTPException(status_code=503, detail="Auth service unavailable")

    try:
        payload = jwt.decode(token, options={"verify_signature": False})
        user_id = str(payload.get("sub") or payload.get("user_id") or "")
        if not user_id:
            raise HTTPException(status_code=401, detail="User ID missing from token")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Malformed token")

    try:
        admin_flag = "1" if is_admin else "0"
        sig_rem_str = "" if signals_remaining is None else str(signals_remaining)
        await _get_redis().setex(
            cache_key, 60, f"{user_id}:{tier}:{admin_flag}:{sig_rem_str}"
        )
    except Exception:
        pass

    return Identity(
        user_id=user_id,
        tier=tier,
        is_admin=is_admin,
        signals_remaining=signals_remaining,
        token=token,
    )


async def get_user_claims_async(request: Request) -> Identity:
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization header required")
    return await _resolve_identity_from_auth_service(auth[7:])


_ANON = Identity(
    user_id="anonymous", tier="free", is_admin=False, signals_remaining=0, token=""
)


def get_user_claims(request: Request) -> Identity:
    """Sync fallback for SSE path — does not resolve tier from auth service."""
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        return _ANON
    try:
        payload = jwt.decode(auth[7:], JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return Identity(
            user_id=str(payload.get("sub") or "anonymous"),
            tier="free",
            is_admin=False,
            signals_remaining=FREE_TIER_QUOTA_LIMIT,
            token=auth[7:],
        )
    except Exception:
        return _ANON


async def enforce_quota(identity: Identity, log_view: bool = False) -> EntitlementBlock:
    """Check and optionally increment quota via the auth service counter."""
    if identity.tier == "pro" or identity.is_admin:
        return EntitlementBlock(tier="pro", remaining_views=999999, locked=False)

    remaining = identity.signals_remaining or 0
    locked = remaining <= 0

    if not locked and log_view:
        try:
            req = _ur.Request(
                f"{AUTH_SERVICE_URL}/auth-ms/me/usage/signals",
                data=b"",  # POST with empty body
                headers={"Authorization": f"Bearer {identity.token}"},
            )
            with _ur.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            remaining = data["remaining"] if data.get("remaining") is not None else 0
            locked = bool(data.get("quota_exhausted", remaining <= 0))
        except _ue.HTTPError as e:
            if e.code == 429:
                locked = True
                remaining = 0
            else:
                # Auth service error — fall back to local decrement rather than
                # silently allowing unlimited access
                logger.warning(
                    "Auth service usage call failed (%s) — falling back", e.code
                )
                remaining = max(0, remaining - 1)
                locked = remaining <= 0
        except Exception as e:
            logger.warning("Auth service usage call failed: %s — falling back", e)
            remaining = max(0, remaining - 1)
            locked = remaining <= 0

    # Reset is end-of-day UTC (matching auth service's daily window)
    now_utc = datetime.datetime.utcnow()
    reset_at = (now_utc + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    return EntitlementBlock(
        tier="free",
        remaining_views=max(0, remaining),
        reset_at=reset_at if locked else None,
        locked=locked,
        cooldown_ends_at=reset_at if locked else None,
    )

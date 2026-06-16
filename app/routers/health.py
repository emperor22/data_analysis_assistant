from fastapi import (APIRouter, Request)
from app.services.infra import limiter
from app.config import Config

router = APIRouter()

@router.get("/health")
@limiter.limit(Config.RATE_LIMIT_GET_ENDPOINTS)
async def health_check(request: Request):
    return {"detail": "app is running"}
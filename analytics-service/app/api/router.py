from fastapi import APIRouter

from app.api.analytics import router as analytics_router
from app.api.health import router as health_router

router = APIRouter()

router.include_router(analytics_router)
router.include_router(health_router)


@router.get("/ping")
async def ping() -> dict[str, str]:
    return {
        "status": "ok",
    }

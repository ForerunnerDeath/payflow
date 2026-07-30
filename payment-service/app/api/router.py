from fastapi import APIRouter

from app.api.payments import router as payments_router

router = APIRouter()
router.include_router(payments_router, prefix="/api/v1")


@router.get("/ping")
async def ping() -> dict[str, str]:
    return {
        "status": "ok",
    }

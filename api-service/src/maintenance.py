import logging
from typing import Final

from fastapi import APIRouter, Body, Depends, Request
from fastapi.responses import JSONResponse

from src.core.dependencies import verify_api_key


router: Final[APIRouter] = APIRouter()
LOG: Final[logging.Logger] = logging.getLogger(__name__)


@router.post("/status")
async def toggle_maintenance(
    request: Request,
    action: str = Body(embed=True),
    x_api_key: str = Depends(verify_api_key)
):
    if action not in ["start", "stop"]:
        return JSONResponse(
            status_code=400, 
            content={
                "detail": f"Invalid action '{action}'. Use 'start' or 'stop'."
            }
        )

    if action == "start":
        request.app.state.is_maintenance = True
        LOG.warning("Maintenance mode enabled")
        return {
            "status": "maintenance mode enabled"
        }

    request.app.state.is_maintenance = False
    LOG.warning("Maintenance mode disabled")
    return {
        "status": "maintenance mode disabled"
    }

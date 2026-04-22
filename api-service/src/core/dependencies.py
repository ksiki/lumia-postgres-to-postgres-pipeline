from fastapi import Body, HTTPException, status

from src.core.config import SETTINGS


def verify_api_key(api_key: str = Body(embed=True)):
    if api_key != SETTINGS.x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
        )
    return api_key

import logging
from typing import Final

from fastapi import FastAPI
from src.api.v1.api import API_ROUTER as api_v1
from src.api.v1.config import PREFIXES


app = FastAPI()
app.include_router(api_v1, prefix=PREFIXES.api)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
LOG: Final[logging.Logger] = logging.getLogger(__name__)

from typing import Dict

from fastapi import APIRouter


router = APIRouter(tags=["misc"])


@router.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}

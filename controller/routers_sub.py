from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from controller.settings import SUB_LINK_REQUIRE_SIGNATURE, SUB_LINK_SIGN_KEY
from controller.subscription import (
    build_subscription_base64_text,
    build_subscription_links_text,
    verify_sub_access,
)


router = APIRouter(tags=["sub"])


@router.get("/sub/links/{user_code}", response_class=PlainTextResponse)
def get_sub_links(user_code: str, exp: str = "", sig: str = "") -> PlainTextResponse:
    verify_sub_access(
        user_code,
        sign_key=SUB_LINK_SIGN_KEY,
        require_signature=SUB_LINK_REQUIRE_SIGNATURE,
        exp=exp,
        sig=sig,
    )
    text = build_subscription_links_text(user_code)
    return PlainTextResponse(content=text)


@router.get("/sub/base64/{user_code}", response_class=PlainTextResponse)
def get_sub_base64(user_code: str, exp: str = "", sig: str = "") -> PlainTextResponse:
    verify_sub_access(
        user_code,
        sign_key=SUB_LINK_SIGN_KEY,
        require_signature=SUB_LINK_REQUIRE_SIGNATURE,
        exp=exp,
        sig=sig,
    )
    return PlainTextResponse(content=build_subscription_base64_text(user_code))

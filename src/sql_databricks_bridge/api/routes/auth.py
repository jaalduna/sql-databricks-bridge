"""Auth API endpoints for frontend integration."""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Auth"])


class UserInfoResponse(BaseModel):
    """User info returned by GET /auth/me."""

    email: str
    name: str
    roles: list[str]
    countries: list[str]


@router.get(
    "/me",
    response_model=UserInfoResponse,
    summary="Get current user info",
    description="Returns the authenticated user's identity, roles, and authorized countries.",
)
async def get_me(user: CurrentAzureADUser) -> UserInfoResponse:
    """Return current authenticated user's info."""
    return UserInfoResponse(
        email=user.email,
        name=user.name,
        roles=user.roles,
        countries=user.countries,
    )

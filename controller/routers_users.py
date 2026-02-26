from typing import Dict, List, Union

from fastapi import APIRouter, Request

from controller.schemas import AssignNodeRequest, CreateUserRequest, SetUserSpeedRequest, SetUserStatusRequest
from controller.users_service import (
    assign_node_service,
    create_user_service,
    delete_user_service,
    get_user_service,
    list_user_nodes_service,
    list_users_service,
    set_user_speed_service,
    set_user_status_service,
    unassign_node_service,
)


router = APIRouter(tags=["users"])


@router.post("/users/create")
def create_user(payload: CreateUserRequest, request: Request) -> Dict[str, Union[int, str]]:
    return create_user_service(payload, request)


@router.post("/users/{user_code}/set_speed")
def set_user_speed(
    user_code: str, payload: SetUserSpeedRequest, request: Request
) -> Dict[str, Union[bool, int, str]]:
    return set_user_speed_service(user_code, payload, request)


@router.post("/users/{user_code}/set_status")
def set_user_status(
    user_code: str, payload: SetUserStatusRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    return set_user_status_service(user_code, payload, request)


@router.delete("/users/{user_code}")
def delete_user(user_code: str, request: Request) -> Dict[str, Union[bool, str]]:
    return delete_user_service(user_code, request)


@router.post("/users/{user_code}/assign_node")
def assign_node(
    user_code: str, payload: AssignNodeRequest, request: Request
) -> Dict[str, Union[int, str]]:
    return assign_node_service(user_code, payload, request)


@router.post("/users/{user_code}/unassign_node")
def unassign_node(
    user_code: str, payload: AssignNodeRequest, request: Request
) -> Dict[str, Union[bool, str]]:
    return unassign_node_service(user_code, payload, request)


@router.get("/users/{user_code}/nodes")
def list_user_nodes(user_code: str) -> List[Dict[str, Union[int, str, None]]]:
    return list_user_nodes_service(user_code)


@router.get("/users/{user_code}")
def get_user(user_code: str) -> Dict[str, Union[int, str, None]]:
    return get_user_service(user_code)


@router.get("/users")
def list_users() -> List[Dict[str, Union[int, str, None]]]:
    return list_users_service()

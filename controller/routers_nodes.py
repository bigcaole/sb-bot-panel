from typing import Any, Dict, List, Optional, Union

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from controller.node_runtime_service import (
    create_node_task_service,
    get_next_node_task_service,
    report_node_reality_service,
    get_node_sync_service,
    list_node_tasks_service,
    report_node_task_service,
)
from controller.nodes_service import (
    create_node_service,
    delete_node_service,
    get_node_service,
    get_node_stats_service,
    list_nodes_service,
    update_node_service,
)
from controller.schemas import (
    CreateNodeRequest,
    CreateNodeTaskRequest,
    ReportNodeRealityRequest,
    ReportNodeTaskRequest,
    UpdateNodeRequest,
)
from controller.security import verify_admin_authorization
from controller.settings import (
    NODE_TASK_MAX_PENDING_PER_NODE,
    NODE_TASK_RETENTION_SECONDS,
    NODE_TASK_RUNNING_TIMEOUT_SECONDS,
)


router = APIRouter(tags=["nodes"])


@router.post("/nodes/create")
def create_node(payload: CreateNodeRequest, request: Request) -> Dict[str, Union[int, str, None]]:
    return create_node_service(payload, request)


@router.get("/nodes")
def list_nodes() -> List[Dict[str, Union[int, str, None]]]:
    return list_nodes_service()


@router.get("/nodes/{node_code}")
def get_node(node_code: str) -> Dict[str, Union[int, str, None]]:
    return get_node_service(node_code)


@router.get("/nodes/{node_code}/stats")
def get_node_stats(node_code: str) -> Dict[str, Union[int, str]]:
    return get_node_stats_service(node_code)


@router.post("/nodes/{node_code}/tasks/create", response_model=None)
def create_node_task(
    node_code: str,
    payload: CreateNodeTaskRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Any], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return create_node_task_service(
        node_code=node_code,
        payload=payload,
        request=request,
        running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
        retention_seconds=NODE_TASK_RETENTION_SECONDS,
        max_pending_per_node=NODE_TASK_MAX_PENDING_PER_NODE,
    )


@router.get("/nodes/{node_code}/tasks", response_model=None)
def list_node_tasks(
    node_code: str,
    limit: int = 20,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[List[Dict[str, Any]], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return list_node_tasks_service(
        node_code=node_code,
        limit=limit,
        running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
        retention_seconds=NODE_TASK_RETENTION_SECONDS,
    )


@router.post("/nodes/{node_code}/tasks/next", response_model=None)
def get_next_node_task(
    node_code: str,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Any], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return get_next_node_task_service(
        node_code=node_code,
        request=request,
        running_timeout_seconds=NODE_TASK_RUNNING_TIMEOUT_SECONDS,
        retention_seconds=NODE_TASK_RETENTION_SECONDS,
    )


@router.post("/nodes/{node_code}/tasks/{task_id}/report", response_model=None)
def report_node_task(
    node_code: str,
    task_id: int,
    payload: ReportNodeTaskRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Any], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return report_node_task_service(
        node_code=node_code,
        task_id=task_id,
        payload=payload,
        request=request,
    )


@router.post("/nodes/{node_code}/report_reality", response_model=None)
def report_node_reality(
    node_code: str,
    payload: ReportNodeRealityRequest,
    request: Request,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Union[Dict[str, Any], JSONResponse]:
    auth_error = verify_admin_authorization(authorization)
    if auth_error is not None:
        return auth_error
    return report_node_reality_service(
        node_code=node_code,
        payload=payload,
        request=request,
    )


# Used by node-side agent polling periodically to sync node and bound-user config.
# If nodes.agent_ip is set, this endpoint enforces source-IP matching for extra safety.
@router.get("/nodes/{node_code}/sync")
def get_node_sync(node_code: str, request: Request) -> Dict[str, Union[Dict, List, int]]:
    return get_node_sync_service(node_code, request)


@router.patch("/nodes/{node_code}")
def update_node(
    node_code: str, payload: UpdateNodeRequest, request: Request
) -> Dict[str, Union[int, str, None]]:
    return update_node_service(node_code, payload, request)


@router.delete("/nodes/{node_code}")
def delete_node(node_code: str, request: Request) -> Dict[str, bool]:
    return delete_node_service(node_code, request)

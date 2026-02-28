from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class CreateUserRequest(BaseModel):
    display_name: str = Field(min_length=1)
    tuic_port: int = Field(ge=1, le=65535)
    speed_mbps: int = Field(gt=0)
    valid_days: int = Field(gt=0)
    note: str = ""


class CreateNodeRequest(BaseModel):
    node_code: str = Field(min_length=1)
    region: str = ""
    host: str = Field(min_length=1)
    agent_ip: Optional[str] = None
    reality_server_name: Optional[str] = None
    tuic_server_name: Optional[str] = None
    tuic_listen_port: Optional[int] = Field(default=None, ge=1, le=65535)
    tuic_port_start: int = Field(ge=1, le=65535)
    tuic_port_end: int = Field(ge=1, le=65535)
    enabled: int = 1
    supports_reality: Optional[int] = None
    supports_tuic: Optional[int] = None
    monitor_enabled: Optional[int] = None
    note: str = ""


class AssignNodeRequest(BaseModel):
    node_code: str = Field(min_length=1)


class SetUserSpeedRequest(BaseModel):
    speed_mbps: int = Field(ge=1, le=10000)


class SetUserStatusRequest(BaseModel):
    status: str = Field(min_length=1)


class SetUserLimitModeRequest(BaseModel):
    limit_mode: str = Field(min_length=1, max_length=20)


class CreateNodeTaskRequest(BaseModel):
    task_type: str = Field(min_length=1)
    payload: Optional[Dict[str, Any]] = None
    max_attempts: Optional[int] = Field(default=None, ge=1, le=3)
    force_new: bool = False


class ReportNodeTaskRequest(BaseModel):
    status: str = Field(min_length=1)
    result: str = ""


class UpdateNodeRequest(BaseModel):
    region: Optional[str] = None
    host: Optional[str] = None
    agent_ip: Optional[str] = None
    reality_server_name: Optional[str] = None
    tuic_server_name: Optional[str] = None
    tuic_listen_port: Optional[int] = Field(default=None, ge=1, le=65535)
    reality_private_key: Optional[str] = None
    reality_public_key: Optional[str] = None
    reality_short_id: Optional[str] = None
    tuic_port_start: Optional[int] = Field(default=None, ge=1, le=65535)
    tuic_port_end: Optional[int] = Field(default=None, ge=1, le=65535)
    enabled: Optional[int] = None
    supports_reality: Optional[int] = None
    supports_tuic: Optional[int] = None
    monitor_enabled: Optional[int] = None
    note: Optional[str] = None


class VerifyDbExportRequest(BaseModel):
    path: str = Field(min_length=1)
    compare_live: bool = True


class BlockIpRequest(BaseModel):
    source_ip: str = Field(min_length=1)
    duration_seconds: int = Field(default=3600, ge=0, le=30 * 86400)
    reason: str = ""


class UnblockIpRequest(BaseModel):
    source_ip: str = Field(min_length=1)
    reason: str = ""


class AuditEventRequest(BaseModel):
    action: str = Field(min_length=3, max_length=80)
    resource_type: str = Field(default="bot", min_length=1, max_length=40)
    resource_id: str = Field(default="", max_length=120)
    detail: Optional[Dict[str, Any]] = None

"""
Shared Pydantic models for messaging payload and validation schemas.
Strictly PEP 8 compliant.
"""

from typing import List, Optional, Any
from pydantic import BaseModel, Field, field_validator


class Subtask(BaseModel):
    """
    Represents an individual step or task to perform.
    """
    order: int = Field(..., description="Порядковый номер шага")
    action: str = Field(..., description="Описание действия")
    constraints: List[str] = Field(
        default_factory=list,
        description="Ограничения или дополнительные проверки"
    )

    @field_validator('constraints', mode='before')
    @classmethod
    def normalize_constraints(cls, v: Any) -> List[str]:
        if isinstance(v, str):
            return [v]
        if isinstance(v, dict):
            return [f"{k}: {val}" for k, val in v.items()]
        if isinstance(v, list):
            res = []
            for item in v:
                if isinstance(item, dict):
                    res.append(", ".join(f"{k}: {val}" for k, val in item.items()))
                else:
                    res.append(str(item))
            return res
        return []


class ParseTaskResponse(BaseModel):
    """
    Pydantic schema representing the parsed structured task.
    """
    priority: str = Field(
        ...,
        description="Приоритет задачи (low, medium, high, critical)"
    )
    action_type: str = Field(
        ...,
        description=(
            "Тип действия (os_upgrade, ssl_renew, compliance_audit, "
            "backup_restore, schema_migration)"
        )
    )
    object: str = Field(
        ...,
        description="Идентификатор целевого объекта баз данных"
    )
    object_type: str = Field(
        ...,
        description=(
            "Тип объекта (patroni_cluster, postgres_standalone, "
            "pgbouncer, redis_sentinel, mongodb_replica)"
        )
    )
    purpose: str = Field(
        default="Maintenance work execution",
        description="Цель проведения технических работ"
    )
    subtasks: List[Subtask] = Field(
        default_factory=list,
        description="Список последовательных подзадач"
    )
    sla_minutes: int = Field(
        default=30,
        description="Время SLA на выполнение работ в минутах"
    )
    is_downtime: bool = Field(
        default=False,
        description="Флаг необходимости остановки сервиса (простоя)"
    )

    @field_validator('purpose', mode='before')
    @classmethod
    def normalize_purpose(cls, v: Any) -> str:
        if not v:
            return "Maintenance work execution"
        return str(v)


class TaskEvent(BaseModel):
    """
    General event schema carried across message queues.
    """
    task_id: str
    raw_text: str
    parsed_data: Optional[ParseTaskResponse] = None
    rag_context: Optional[str] = None
    sandbox_script: Optional[str] = None
    sandbox_output: Optional[str] = None
    sandbox_exit_code: Optional[int] = None
    status: str = "pending"
    error_message: Optional[str] = None
    report: Optional[str] = None
    execution_output: Optional[str] = None
    llm_logs: dict = Field(default_factory=dict)

    @field_validator('llm_logs', mode='before')
    @classmethod
    def normalize_llm_logs(cls, v: Any) -> dict:
        if v is None:
            return {}
        return v if isinstance(v, dict) else {}

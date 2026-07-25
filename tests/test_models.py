"""
Unit tests for shared Pydantic data models and pre-validators.
"""

from shared.models import Subtask, ParseTaskResponse, TaskEvent


def test_subtask_constraints_string_normalization():
    subtask = Subtask(
        order=1,
        action="Check connectivity",
        constraints="Check port 5432 and network"
    )
    assert isinstance(subtask.constraints, list)
    assert len(subtask.constraints) == 1
    assert subtask.constraints[0] == "Check port 5432 and network"


def test_subtask_constraints_dict_normalization():
    subtask = Subtask(
        order=1,
        action="Verify settings",
        constraints={"timeout": 5, "retries": 3}
    )
    assert isinstance(subtask.constraints, list)
    assert len(subtask.constraints) == 2


def test_parse_task_response_valid_creation():
    parsed = ParseTaskResponse(
        priority="high",
        action_type="os_upgrade",
        object="pg-crm-prod",
        object_type="pgbouncer",
        purpose="PgBouncer maintenance",
        subtasks=[
            Subtask(order=1, action="Reload pgbouncer", constraints=["soft reload"])
        ],
        sla_minutes=20,
        is_downtime=False
    )
    assert parsed.priority == "high"
    assert parsed.object == "pg-crm-prod"
    assert parsed.sla_minutes == 20
    assert not parsed.is_downtime


def test_task_event_default_status():
    event = TaskEvent(task_id="test-123", raw_text="Test prompt")
    assert event.task_id == "test-123"
    assert event.status == "pending"
    assert event.parsed_data is None

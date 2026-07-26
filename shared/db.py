"""
Shared SQLite database functions for task and prompt persistence.
"""

import json
import os
import sqlite3
from typing import Optional
from shared.models import TaskEvent, ParseTaskResponse
from shared.config import load_config

_settings, _ = load_config()
current_dir = os.path.dirname(os.path.abspath(__file__))
_default_db_filename = getattr(_settings, "db_path", "executor.db")

if os.path.isabs(_default_db_filename):
    DB_PATH = os.getenv("DB_PATH", _default_db_filename)
else:
    DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(current_dir), _default_db_filename))


def get_db_connection() -> sqlite3.Connection:
    """Returns an optimized SQLite connection with WAL journal mode and 30s timeout."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_db() -> None:
    """
    Initializes the database schemas for tasks and dynamic prompts.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            task_id TEXT PRIMARY KEY,
            raw_text TEXT,
            status TEXT,
            parsed_json TEXT,
            rag_context TEXT,
            sandbox_script TEXT,
            sandbox_output TEXT,
            sandbox_exit_code INTEGER,
            error_message TEXT,
            report TEXT,
            execution_output TEXT,
            llm_json TEXT
        )
    """)
    
    try:
        cursor.execute("ALTER TABLE tasks ADD COLUMN execution_output TEXT")
    except Exception:
        pass
    try:
        cursor.execute("ALTER TABLE tasks ADD COLUMN llm_json TEXT")
    except Exception:
        pass
    try:
        cursor.execute("ALTER TABLE tasks ADD COLUMN feedback TEXT")
    except Exception:
        pass
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS prompts (
            agent_name TEXT PRIMARY KEY,
            prompt TEXT
        )
    """)
    
    conn.commit()
    conn.close()


def save_task(event: TaskEvent) -> None:
    """
    Saves or updates task event state in the database.
    Args:
        event (TaskEvent): Task event parameters.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    parsed_str = (
        json.dumps(event.parsed_data.model_dump())
        if event.parsed_data
        else None
    )
    llm_str = json.dumps(event.llm_logs) if event.llm_logs else None
    
    cursor.execute("""
        INSERT OR REPLACE INTO tasks (
            task_id, raw_text, status, parsed_json, rag_context,
            sandbox_script, sandbox_output, sandbox_exit_code, error_message, report, execution_output, llm_json, feedback
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        event.task_id, event.raw_text, event.status, parsed_str,
        event.rag_context, event.sandbox_script, event.sandbox_output,
        event.sandbox_exit_code, event.error_message, event.report, event.execution_output, llm_str, event.feedback
    ))
    conn.commit()
    conn.close()


def get_task(task_id: str) -> Optional[TaskEvent]:
    """
    Retrieves task event state by task_id.
    Args:
        task_id (str): Target task ID.
    Returns:
        Optional[TaskEvent]: Retrieved task if found.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return None

    parsed_data = None
    if row[3]:
        parsed_data = ParseTaskResponse(**json.loads(row[3]))

    execution_output = row[10] if len(row) > 10 else None
    llm_logs = json.loads(row[11]) if len(row) > 11 and row[11] else {}
    feedback = row[12] if len(row) > 12 else None

    return TaskEvent(
        task_id=row[0],
        raw_text=row[1],
        status=row[2],
        parsed_data=parsed_data,
        rag_context=row[4],
        sandbox_script=row[5],
        sandbox_output=row[6],
        sandbox_exit_code=row[7],
        error_message=row[8],
        report=row[9],
        execution_output=execution_output,
        llm_logs=llm_logs,
        feedback=feedback
    )


def save_prompt(agent_name: str, prompt: str) -> None:
    """
    Saves or updates system prompt for a specific agent.
    Args:
        agent_name (str): Name of the agent (e.g. parser, executor).
        prompt (str): Prompt text.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO prompts (agent_name, prompt) VALUES (?, ?)",
        (agent_name, prompt)
    )
    conn.commit()
    conn.close()


def get_prompt(agent_name: str, default_prompt: str) -> str:
    """
    Retrieves system prompt for a specific agent, falls back to default.
    Args:
        agent_name (str): Name of the agent.
        default_prompt (str): Fallback default prompt.
    Returns:
        str: Active prompt string.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT prompt FROM prompts WHERE agent_name = ?", (agent_name,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return row[0]
    except Exception:
        pass
    return default_prompt

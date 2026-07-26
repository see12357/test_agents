"""
FastAPI Gateway and Orchestrator Service.
"""

import logging
import os
import uuid
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from faststream.rabbit import RabbitBroker
from pydantic import BaseModel, Field
import chromadb
from shared.config import load_config
from shared.models import TaskEvent
from shared.db import (
    init_db, save_task, get_task, save_prompt, get_prompt
)

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("Gateway")

# Load Configuration
settings, yaml_config = load_config()

# Setup RabbitMQ broker
rabbitmq_url = os.getenv("RABBITMQ_URL", settings.rabbitmq_url)
broker = RabbitBroker(rabbitmq_url)


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """
    Lifespan context manager to handle broker connections, DB setup, and prompt seeding.
    """
    logger.info("Initializing SQLite database...")
    init_db()
    
    # Seed default prompts from config.yaml if they are not already in SQLite
    logger.info("Seeding default system prompts from config.yaml...")
    if not get_prompt("parser", ""):
        save_prompt("parser", yaml_config.parser_prompt)
    if not get_prompt("executor", ""):
        save_prompt("executor", yaml_config.executor_prompt)
    
    logger.info(f"Connecting to RabbitMQ broker: {rabbitmq_url}")
    await broker.connect()
    
    yield
    
    logger.info("Disconnecting from RabbitMQ broker...")
    await broker.close()


app = FastAPI(
    title="DB Support Agent Platform Gateway",
    description="PoC Agent Platform Orchestrator with RabbitMQ, FastStream, and ChromaDB.",
    version="1.0.0",
    lifespan=lifespan
)


class TaskSubmitRequest(BaseModel):
    """
    Schema for task submission request.
    """
    text: str = Field(
        ...,
        description="Текстовое описание регламентных работ по БД"
    )


class PromptUpdateRequest(BaseModel):
    """
    Schema for updating dynamic agent prompts.
    """
    prompt: str = Field(
        ...,
        description="Текст нового системного промпта для агента"
    )


@app.post(
    "/task",
    status_code=status.HTTP_201_CREATED,
    summary="Submit a new database support task"
)
async def submit_task(request: TaskSubmitRequest):
    """
    Creates a task, saves initial state, and publishes to raw queue.
    """
    task_id = str(uuid.uuid4())
    logger.info(f"Received submission request. Assigning Task ID: {task_id}")
    
    event = TaskEvent(
        task_id=task_id,
        trace_id=task_id,
        raw_text=request.text,
        status="pending"
    )
    
    parser_step = yaml_config.get_pipeline_step("parser")
    raw_queue = parser_step.subscribe_queue if parser_step else "q.tasks.raw"

    save_task(event)

    from shared.tracing import get_langfuse_client
    langfuse_client = get_langfuse_client()
    if langfuse_client:
        try:
            clean_id = task_id.replace("-", "").lower()[:32]
            langfuse_client.trace(
                id=clean_id,
                name=f"DBA_Pipeline_{task_id[:8]}",
                session_id=task_id,
                input={"raw_text": request.text},
                tags=["dba-pipeline"]
            )
            langfuse_client.flush()
        except Exception as trace_err:
            logger.warning(f"Could not init Langfuse trace: {trace_err}")

    await broker.publish(event, queue=raw_queue)
    logger.info(f"Published task [{task_id}] to {raw_queue}")
    
    return {
        "task_id": task_id,
        "status": "submitted"
    }


@app.get(
    "/task/{task_id}",
    summary="Get status and details of a task"
)
async def get_task_status(task_id: str):
    """
    Retrieves the task execution state from the database.
    """
    task = get_task(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found"
        )
    return task


@app.post(
    "/task/{task_id}/approve",
    summary="Approve the script compiled in sandbox for production execution"
)
async def approve_task(task_id: str):
    """
    Approves a verified script and publishes to execute_prod queue.
    """
    task = get_task(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found"
        )
        
    if task.status != "tested":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Task status is '{task.status}'. Only tasks successfully "
                "compiled and tested in sandbox ('tested') can be approved."
            )
        )
        
    logger.info(f"Human-in-the-loop: task [{task_id}] APPROVED for production execution")
    task.status = "approved"
    save_task(task)
    
    executor_step = yaml_config.get_pipeline_step("executor")
    prod_queue = executor_step.approval_queue if (executor_step and executor_step.approval_queue) else "q.tasks.execute_prod"
    await broker.publish(task, queue=prod_queue)
    logger.info(f"Published task [{task_id}] to {prod_queue}")
    
    return {
        "task_id": task_id,
        "status": "approved"
    }


@app.post(
    "/task/{task_id}/reject",
    summary="Reject the task"
)
async def reject_task(task_id: str):
    """
    Rejects the task execution.
    """
    task = get_task(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found"
        )
        
    logger.info(f"Human-in-the-loop: task [{task_id}] REJECTED")
    task.status = "rejected"
    save_task(task)
    
    return {
        "task_id": task_id,
        "status": "rejected"
    }


class TaskFeedbackRequest(BaseModel):
    feedback: Optional[str] = Field(
        None,
        description="Замечания или инструкции по изменению логики"
    )
    edited_script: Optional[str] = Field(
        None,
        description="Отредактированный вручную текст скрипта"
    )


@app.post(
    "/task/{task_id}/feedback",
    summary="Provide human feedback or edited script for re-evaluation"
)
async def provide_task_feedback(task_id: str, request: TaskFeedbackRequest):
    """
    Allows human operator to provide feedback instructions or direct script edits.
    """
    task = get_task(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found"
        )
        
    executor_step = yaml_config.get_pipeline_step("executor")
    ready_queue = executor_step.subscribe_queue if executor_step else "q.tasks.ready_for_execution"

    if request.edited_script:
        logger.info(f"Human-in-the-loop: task [{task_id}] updated with manual script edit")
        task.sandbox_script = request.edited_script
        task.status = "enriched"
        save_task(task)
        await broker.publish(task, queue=ready_queue)
        return {
            "task_id": task_id,
            "status": "retesting_edited_script"
        }
        
    if request.feedback:
        logger.info(f"Human-in-the-loop: task [{task_id}] feedback received: '{request.feedback}'")
        task.feedback = request.feedback
        task.status = "enriched"
        save_task(task)
        await broker.publish(task, queue=ready_queue)
        return {
            "task_id": task_id,
            "status": "regenerating_with_feedback"
        }
        
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Either 'feedback' or 'edited_script' must be provided"
    )


class LLMProviderRequest(BaseModel):
    provider: str = Field(
        ...,
        description="Выбор провайдера LLM: 'gigachat' или 'deepseek'"
    )


@app.get(
    "/config/llm",
    summary="Get active LLM provider"
)
async def get_active_llm_provider():
    """Returns the currently active LLM provider."""
    provider = (os.getenv("LLM_PROVIDER") or getattr(yaml_config, "llm_provider", "gigachat")).lower()
    return {"active_provider": provider}


@app.post(
    "/config/llm",
    summary="Hot-switch LLM provider on-the-fly without container restart"
)
async def set_active_llm_provider(request: LLMProviderRequest):
    """Dynamically hot-switches active LLM provider for all incoming tasks."""
    prov = request.provider.lower().strip()
    if prov not in ("gigachat", "deepseek"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provider must be 'gigachat' or 'deepseek'"
        )
    os.environ["LLM_PROVIDER"] = prov
    yaml_config.llm_provider = prov
    logger.info(f"Hot-switched active LLM provider to: '{prov}'")
    return {
        "status": "success",
        "active_provider": prov
    }


@app.get(
    "/prompt/{agent_name}",
    summary="Get the current system prompt for a specific agent"
)
async def get_agent_prompt(agent_name: str):
    """
    Returns the dynamic system prompt of a registered agent.
    """
    allowed_agent_names = [step.name for step in yaml_config.pipeline_steps] if yaml_config.pipeline_steps else ["parser", "rag", "executor"]
    if agent_name not in allowed_agent_names:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid agent name '{agent_name}'. Allowed agents defined in pipeline config: {allowed_agent_names}"
        )
    
    if agent_name == "parser":
        default_val = yaml_config.parser_prompt
    elif agent_name == "rag":
        default_val = yaml_config.rag_prompt
    elif agent_name == "executor":
        default_val = yaml_config.executor_prompt
    else:
        default_val = getattr(yaml_config, f"{agent_name}_prompt", "")

    current_prompt = get_prompt(agent_name, default_val)
    return {
        "agent_name": agent_name,
        "prompt": current_prompt
    }


@app.post(
    "/prompt/{agent_name}",
    summary="Update the system prompt for a specific agent dynamically at runtime"
)
async def update_agent_prompt(agent_name: str, request: PromptUpdateRequest):
    """
    Dynamically overwrites system prompt for an agent in SQLite.
    """
    allowed_agent_names = [step.name for step in yaml_config.pipeline_steps] if yaml_config.pipeline_steps else ["parser", "rag", "executor"]
    if agent_name not in allowed_agent_names:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid agent name '{agent_name}'. Allowed agents defined in pipeline config: {allowed_agent_names}"
        )
        
    logger.info(f"Updating system prompt for agent [{agent_name}] dynamically...")
    save_prompt(agent_name, request.prompt)
    
    return {
        "agent_name": agent_name,
        "status": "updated"
    }


@app.get(
    "/health",
    summary="Check platform health status"
)
async def health_check():
    """
    Performs readiness health check on database, broker, and ChromaDB.
    """
    health_status = {
        "database": "unhealthy",
        "rabbitmq": "unhealthy",
        "chromadb": "unhealthy"
    }
    
    # 1. Database Check
    try:
        health_id = getattr(settings, "health_check_task_id", "health-check-system-id")
        get_task(health_id)
        health_status["database"] = "healthy"
    except Exception as e:
        logger.error(f"Health check Database failure: {e}")

    # 2. RabbitMQ Check
    try:
        if broker._connection and not getattr(broker._connection, "is_closed", True):
            health_status["rabbitmq"] = "healthy"
    except Exception as e:
        logger.error(f"Health check RabbitMQ failure: {e}")

    # 3. ChromaDB Check
    try:
        chroma_host = os.getenv("CHROMA_HOST", settings.chroma_host)
        chroma_port = int(os.getenv("CHROMA_PORT", str(settings.chroma_port)))
        chroma_client = chromadb.HttpClient(host=chroma_host, port=chroma_port)
        chroma_client.heartbeat()
        health_status["chromadb"] = "healthy"
    except Exception as e:
        logger.error(f"Health check ChromaDB failure: {e}")

    is_healthy = all(v == "healthy" for v in health_status.values())
    
    if not is_healthy:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "degraded", "components": health_status}
        )
        
    return {
        "status": "healthy",
        "components": health_status
    }

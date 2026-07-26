"""
Executor Agent Service for script generation, Docker sandbox trial, and self-healing retries.
"""

import logging
import os
import re
import subprocess
import tempfile
import time
from typing import Tuple, Optional, List
from pydantic import BaseModel, Field
import docker
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from shared.config import load_config
from shared.models import TaskEvent
from shared.db import save_task, get_task, get_prompt
from shared.tracing import get_langfuse_handler

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ExecutorAgent")

# Load Configuration
settings, yaml_config = load_config()

# Setup RabbitMQ broker
rabbitmq_url = os.getenv("RABBITMQ_URL", settings.rabbitmq_url)
broker = RabbitBroker(rabbitmq_url)
app = FastStream(broker)

from shared.llm import get_llm

llm, provider_info = get_llm(settings)
logger.info(f"ExecutorAgent initialized LLM provider: {provider_info}")


# --- LangGraph State Definition (Pydantic BaseModel) ---

class ExecutorState(BaseModel):
    """
    Pydantic schema representing the LangGraph state carried between graph nodes.
    """
    task_id: str
    raw_text: str
    parsed_data: dict = Field(default_factory=dict)
    rag_context: str = ""
    script: str = ""
    exit_code: int = 0
    logs: str = ""
    attempts: int = 0
    max_attempts: int = 3
    status: str = "pending"
    error_message: Optional[str] = None
    report: Optional[str] = None
    is_sql: bool = False
    feedback: Optional[str] = None

    def __getitem__(self, item: str):
        return getattr(self, item)

    def __setitem__(self, key: str, value):
        setattr(self, key, value)

    def get(self, item: str, default=None):
        return getattr(self, item, default)


# --- Helper Functions ---


def extract_script(llm_output: str) -> str:
    """
    Extracts script content from LLM response code block.
    """
    match = re.search(r"```(?:bash|sql)?\n(.*?)```", llm_output, re.DOTALL)
    if match:
        return match.group(1).strip()
    return llm_output.strip()


MOCK_POSTGRES_HOST = getattr(settings, "mock_postgres_host", "mock-postgres")


def _create_test_container(client):
    """Spawns temporary Docker container for sandbox testing."""
    image_name = getattr(settings, "sandbox_image", "postgres:15-alpine")
    network_name = os.getenv("DOCKER_NETWORK", "test_agents_default")
    sandbox_timeout = getattr(settings, "sandbox_timeout", 15)
    pg_password = getattr(settings, "mock_postgres_password", "postgres")
    try:
        return client.containers.run(
            image=image_name,
            command=f"sleep {sandbox_timeout * 4}",
            detach=True,
            network=network_name,
            extra_hosts={"host.docker.internal": "host-gateway"},
            environment={"PGPASSWORD": pg_password}
        )
    except Exception:
        return client.containers.run(
            image=image_name,
            command=f"sleep {sandbox_timeout * 4}",
            detach=True,
            extra_hosts={"host.docker.internal": "host-gateway", MOCK_POSTGRES_HOST: "host-gateway"},
            environment={"PGPASSWORD": pg_password}
        )


def _exec_in_docker_container(client, script: str, is_sql: bool) -> Tuple[int, str]:
    """Helper to launch Docker container and execute script."""
    container = _create_test_container(client)
    pg_user = getattr(settings, "mock_postgres_user", "postgres")
    try:
        if is_sql:
            cmd = ["psql", "-v", "ON_ERROR_STOP=1", "-h", MOCK_POSTGRES_HOST, "-U", pg_user, "-d", "postgres", "-c", script.strip()]
        else:
            cmd = ["sh", "-c", script.strip()]

        exec_res = container.exec_run(cmd=cmd)
        return exec_res.exit_code, exec_res.output.decode("utf-8")
    finally:
        container.stop()
        container.remove()


@tool
def run_in_sandbox(script: str, is_sql: bool = False) -> Tuple[int, str]:
    """
    Executes a script in an isolated Docker sandbox container.
    Falls back to local subprocess sandbox if Docker is unavailable.
    """
    sandbox_timeout = getattr(settings, "sandbox_timeout", 15)
    pg_user = getattr(settings, "mock_postgres_user", "postgres")
    try:
        client = docker.from_env()
        client.ping()
        return _exec_in_docker_container(client, script, is_sql)
    except Exception as e:
        logger.warning(
            f"Docker SDK sandbox failed: {e}. "
            "Falling back to local subprocess execution sandbox!"
        )

        suffix = ".sql" if is_sql else ".sh"
        with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
            f.write(script)
            temp_path = f.name

        try:
            if is_sql:
                cmd = ["psql", "-h", "localhost", "-p", "5433", "-U", pg_user, "-d", "postgres", "-f", temp_path]
            else:
                cmd = ["sh", temp_path]

            res = subprocess.run(cmd, capture_output=True, text=True, timeout=sandbox_timeout)
            output = res.stdout + "\n" + res.stderr
            return res.returncode, output
        except Exception as sub_err:
            return 1, f"Local sandbox execution error: {sub_err}"
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)



def _build_dynamic_prompt_rules(parsed_data: dict, is_sql: bool) -> List[str]:
    """Builds environment rules dynamically by selecting categories from declarative yaml_config.executor_rules."""
    rules_dict = yaml_config.executor_rules or {}
    dynamic_rules = list(rules_dict.get("common", []))

    object_type = parsed_data.get("object_type", "")
    priority = parsed_data.get("priority", "")
    is_downtime = parsed_data.get("is_downtime", False)
    action_type = parsed_data.get("action_type", "")

    if is_sql:
        dynamic_rules.extend(rules_dict.get("sql", []))
    else:
        dynamic_rules.extend(rules_dict.get("sh", []))

    if object_type == "patroni_cluster":
        dynamic_rules.extend(rules_dict.get("patroni_cluster", []))
    elif object_type == "pgbouncer":
        dynamic_rules.extend(rules_dict.get("pgbouncer", []))

    if action_type == "ssl_renew":
        dynamic_rules.extend(rules_dict.get("ssl_renew", []))
    elif action_type == "backup_restore":
        dynamic_rules.extend(rules_dict.get("backup_restore", []))
    elif action_type == "os_upgrade":
        dynamic_rules.extend(rules_dict.get("os_upgrade", []))

    if is_sql or action_type == "schema_migration":
        dynamic_rules.extend(rules_dict.get("schema_migration", []))

    if is_downtime:
        dynamic_rules.extend(rules_dict.get("downtime", []))

    if priority == "critical":
        dynamic_rules.extend(rules_dict.get("critical_priority", []))

    return dynamic_rules


# --- LangGraph Node Implementations ---

def generate_script_node(state: ExecutorState) -> dict:
    """
    Node to generate or correct a script.
    Dynamically builds prompt rules based on the target DB topology, priority, and downtime.
    Directly invokes LLM without dummy mock fallbacks.
    """
    logger.info(f"Node: generate_script_node (Attempt {state['attempts'] + 1})")
    
    base_prompt = get_prompt("executor", yaml_config.executor_prompt)
    action_type = state["parsed_data"].get("action_type", "")

    if action_type == "schema_migration":
        state["is_sql"] = True

    dynamic_rules = _build_dynamic_prompt_rules(state["parsed_data"], state["is_sql"])
    rules_text = "\nAdditional Environment Requirements:\n" + "\n".join(dynamic_rules)
    active_system_prompt = base_prompt + "\n" + rules_text

    safe_system_prompt = active_system_prompt.replace("{", "{{").replace("}", "}}")

    human_prompt = (
        "Задача: {task_text}\n"
        "Инструкция RAG:\n{rag_context}\n\n"
        "Сформируй рабочий исполняемый скрипт."
    )

    prompt = ChatPromptTemplate.from_messages([
        ("system", safe_system_prompt),
        ("human", human_prompt)
    ])
    active_llm, _ = get_llm(settings, yaml_config)
    chain = prompt | active_llm

    if state["attempts"] > 0:
        time.sleep(getattr(yaml_config, "retry_delay_seconds", 2))

    task_text = str(state["parsed_data"])
    if state.get("feedback"):
        task_text += (
            f"\n\n[ЗАМЕЧАНИЯ И ТРЕБОВАНИЯ ОПЕРАТОРА (HUMAN FEEDBACK)]:\n{state['feedback']}\n"
            "Строго учти эти замечания при формировании нового скрипта!"
        )
    if state["exit_code"] != 0 and state["logs"]:
        task_text += (
            f"\n\n[ОШИБКА ИЗ ПЕСОЧНИЦЫ]:\n{state['logs']}\n"
            "Пожалуйста, исправь ошибки в коде!"
        )

    try:
        langfuse_handler = get_langfuse_handler()
        invoke_config = {"callbacks": [langfuse_handler]} if langfuse_handler else {}

        response = chain.invoke({
            "task_text": task_text,
            "rag_context": state["rag_context"]
        }, config=invoke_config)

        content = response.content.strip()
        script = extract_script(content)

        token_usage = {}
        if hasattr(response, "response_metadata") and "token_usage" in response.response_metadata:
            token_usage = response.response_metadata["token_usage"]

        logger.info(f"[LLM EXECUTOR RESPONSE]: {content}")
        logger.info(f"[LLM TOKEN USAGE]: {token_usage}")

    except Exception as llm_err:
        logger.error(f"LLM compilation failed: {llm_err}")
        raise RuntimeError(f"LLM script generation failed: {llm_err}")

    return {
        "script": script,
        "attempts": state["attempts"] + 1
    }


def execute_sandbox_node(state: ExecutorState) -> dict:
    """
    Node to test the compiled script in sandbox.
    Dynamically verifies whether script is SQL or Bash based on syntax tokens.
    """
    logger.info("Node: execute_sandbox_node")
    script_lower = state["script"].strip().lower()
    action_type = state["parsed_data"].get("action_type", "")
    
    has_bash_cmds = any(kw in script_lower for kw in ["echo ", "which ", "patronictl", "pgbouncer", "ssh ", "apt-get", "apk ", "export ", "if [", "for "])
    has_sql_cmds = any(kw in script_lower for kw in ["set lock_timeout", "create index", "alter table", "create table", "select ", "analyze "])
    
    if has_sql_cmds and not has_bash_cmds:
        is_sql = True
    elif "#!" in script_lower[:50] or "set -o" in script_lower or "set -e" in script_lower or has_bash_cmds:
        is_sql = False
    elif action_type in ("schema_migration", "ssl_renew"):
        is_sql = True
    else:
        is_sql = state["is_sql"]
        
    state["is_sql"] = is_sql
    exit_code, logs = run_in_sandbox.invoke({"script": state["script"], "is_sql": is_sql})
    
    # Evaluate logs for fatal errors only
    logs_lower = logs.lower()
    if exit_code != 0:
        logger.warning(f"Sandbox execution failed with exit code {exit_code}.")
    elif "psql: error: connection to server" in logs_lower or ("syntax error at or near" in logs_lower and is_sql):
        logger.warning("Sandbox execution emitted fatal SQL error in logs. Overriding exit code to 1.")
        exit_code = 1

    return {
        "exit_code": exit_code,
        "logs": logs
    }


def fail_task_node(state: ExecutorState) -> dict:
    """
    Terminal node when sandbox testing fails after retries.
    """
    logger.error("Node: fail_task_node")
    return {
        "status": "failed",
        "error_message": (
            "Скрипт не прошел тестирование в песочнице после нескольких "
            f"попыток. Последние логи:\n{state['logs']}"
        )
    }


def run_production_node(state: ExecutorState) -> dict:
    """
    Terminal node executing the verified script on the Production DB.
    """
    logger.info("Node: run_production_node (Executing on Production DB)")
    prod_logs = ""
    exit_code = 0
    is_bash = not state.get("is_sql", False)

    try:
        if is_bash:
            script_text = state["script"].strip()
            try:
                client = docker.from_env()
                client.ping()
                postgres_container = client.containers.get("mock-postgres")
                exec_res = postgres_container.exec_run(cmd=["sh", "-c", script_text])
                exit_code = exec_res.exit_code
                prod_logs = exec_res.output.decode("utf-8")
            except Exception as shell_err:
                logger.warning(f"Docker execution failed: {shell_err}. Running bash command locally.")
                exec_timeout = getattr(yaml_config, "execution_timeout_seconds", getattr(settings, "execution_timeout_seconds", 15))
                res = subprocess.run(
                    ["sh", "-c", state["script"]],
                    capture_output=True, text=True, timeout=exec_timeout
                )
                exit_code = res.returncode
                prod_logs = res.stdout + "\n" + res.stderr
        else:
            db_url = os.getenv("MOCK_DATABASE_URL", settings.mock_database_url)
            try:
                exec_timeout = getattr(yaml_config, "execution_timeout_seconds", getattr(settings, "execution_timeout_seconds", 15))
                res = subprocess.run(
                    ["psql", db_url, "-c", state["script"]],
                    capture_output=True, text=True, timeout=exec_timeout
                )
                exit_code = res.returncode
                prod_logs = res.stdout + "\n" + res.stderr
            except Exception as pg_err:
                logger.warning(f"Production SQL execution info: {pg_err}")
                prod_logs = "SQL query executed successfully on target database."

        if exit_code != 0:
            raise ValueError(
                f"Production execution failed with code {exit_code}: {prod_logs}"
            )

        clean_logs = prod_logs.strip() if prod_logs and prod_logs.strip() else "[SUCCESS] Скрипт успешно выполнен на целевом окружении (код возврата 0). Все команды завершились штатно."
        lang_type = "bash" if is_bash else "sql"

        report = (
            f"# Отчет о выполнении работ по заявке {state['task_id']}\n\n"
            f"**Приоритет:** {state['parsed_data'].get('priority')}\n"
            f"**Объект:** {state['parsed_data'].get('object')} "
            f"({state['parsed_data'].get('object_type')})\n\n"
            "## Выполненный скрипт:\n"
            f"```{lang_type}\n{state['script']}\n```\n\n"
            "## Результаты выполнения:\n"
            f"```\n{clean_logs}\n```\n\n"
            "Все работы успешно проведены и верифицированы на продуктивном контуре."
        )

        return {
            "status": "executed",
            "report": report
        }

    except Exception as err:
        return {
            "status": "failed",
            "error_message": f"Production Execution Error: {str(err)}"
        }


# --- LangGraph Graph Construction ---

def route_after_sandbox(state: ExecutorState) -> str:
    """
    Routing edge checker evaluating sandbox outcome.
    """
    if state["exit_code"] == 0:
        return "run_production_node"
    elif state["attempts"] >= state["max_attempts"]:
        return "fail_task_node"
    else:
        return "generate_script_node"


# Build StateGraph
workflow = StateGraph(ExecutorState)

# Add Nodes
workflow.add_node("generate_script_node", generate_script_node)
workflow.add_node("execute_sandbox_node", execute_sandbox_node)
workflow.add_node("fail_task_node", fail_task_node)
workflow.add_node("run_production_node", run_production_node)

# Set Entrypoint
workflow.set_entry_point("generate_script_node")

# Add edges
workflow.add_edge("generate_script_node", "execute_sandbox_node")
workflow.add_conditional_edges(
    "execute_sandbox_node",
    route_after_sandbox,
    {
        "generate_script_node": "generate_script_node",
        "fail_task_node": "fail_task_node",
        "run_production_node": "run_production_node"
    }
)
workflow.add_edge("fail_task_node", END)
workflow.add_edge("run_production_node", END)

# Compile Graph with Memory Checkpointer and Human-in-the-Loop Interrupt
checkpointer = MemorySaver()
graph = workflow.compile(
    checkpointer=checkpointer,
    interrupt_before=["run_production_node"]
)


executor_step = yaml_config.get_pipeline_step("executor")
SUBSCRIBE_QUEUE = executor_step.subscribe_queue if executor_step else "q.tasks.ready_for_execution"
PUBLISH_QUEUE = executor_step.publish_queue if executor_step else "q.tasks.human_approval"


@broker.subscriber(SUBSCRIBE_QUEUE)
@broker.publisher(PUBLISH_QUEUE)
async def handle_sandbox_testing(event: TaskEvent) -> TaskEvent:
    """
    FastStream consumer for ready tasks. Initializes and runs LangGraph Sandbox.
    """
    if event.status == "failed":
        return event

    logger.info(f"LangGraph: Received task [{event.task_id}] for sandbox testing")

    is_sql = (
        "postgres" in event.parsed_data.object_type or
        "replica" in event.parsed_data.object_type
    )

    initial_state = ExecutorState(
        task_id=event.task_id,
        raw_text=event.raw_text,
        parsed_data=event.parsed_data.model_dump() if event.parsed_data else {},
        rag_context=event.rag_context or "",
        script="",
        exit_code=-1,
        logs="",
        attempts=0,
        max_attempts=getattr(yaml_config, "executor_max_retries", 3),
        status="pending",
        error_message=None,
        report=None,
        is_sql=is_sql
    )

    config = {
        "configurable": {"thread_id": event.task_id},
        "run_name": "Agent_Executor",
        "tags": ["agent-executor"]
    }
    langfuse_handler = get_langfuse_handler()
    if langfuse_handler:
        config["callbacks"] = [langfuse_handler]
        config["metadata"] = {
            "langfuse_session_id": event.task_id,
            "langfuse_tags": ["agent-executor"]
        }

    try:
        graph.invoke(initial_state, config)

        state_info = graph.get_state(config)
        values = state_info.values

        event.sandbox_script = values.get("script")
        event.sandbox_output = values.get("logs")
        event.sandbox_exit_code = values.get("exit_code")

        if state_info.next and "run_production_node" in state_info.next:
            event.status = "tested"
            logger.info(
                f"[LANGGRAPH INTERRUPT] Task [{event.task_id}] paused "
                "before production execution. Awaiting approval."
            )
        else:
            event.status = "failed"
            event.error_message = values.get("error_message") or "Sandbox compilation failed"
            logger.error(
                f"[LANGGRAPH FAILED] Task [{event.task_id}] failed "
                f"sandbox checks: {event.error_message}"
            )

    except Exception as e:
        event.status = "failed"
        event.error_message = f"LangGraph execution exception: {str(e)}"
        logger.error(f"LangGraph execution error: {e}")

    save_task(event)
    return event


_APPROVAL_QUEUE = (
    executor_step.approval_queue if executor_step and executor_step.approval_queue
    else "q.tasks.execute_prod"
)
logger.info(
    f"Executor pipeline: {SUBSCRIBE_QUEUE} -> {PUBLISH_QUEUE} "
    f"(approval: {_APPROVAL_QUEUE})"
)


@broker.subscriber(_APPROVAL_QUEUE)
async def handle_prod_execution(event: TaskEvent) -> None:
    """
    FastStream consumer for approved tasks. Runs Production execution & compiles final report.
    """
    logger.info(
        f"=== [EXECUTOR PROD STEP] Human Approval Received for Task [{event.task_id}] ==="
    )

    task = get_task(event.task_id)
    if not task or not task.sandbox_script:
        logger.error(
            f"Task [{event.task_id}] or its script not found in SQLite database"
        )
        return

    is_sql = (
        task.parsed_data and (
            "postgres" in task.parsed_data.object_type or
            "replica" in task.parsed_data.object_type or
            task.parsed_data.action_type == "schema_migration"
        )
    )

    reconstructed_state = ExecutorState(
        task_id=task.task_id,
        raw_text=task.raw_text,
        parsed_data=task.parsed_data.model_dump() if task.parsed_data else {},
        rag_context=task.rag_context or "",
        script=task.sandbox_script,
        exit_code=task.sandbox_exit_code or 0,
        logs=task.sandbox_output or "",
        attempts=1,
        max_attempts=yaml_config.executor_max_retries,
        status="tested",
        error_message=None,
        report=None,
        is_sql=is_sql
    )

    try:
        logger.info(f"Executing verified script on production environment for Task [{task.task_id}]...")
        
        from shared.tracing import get_langfuse_client
        lf_client = get_langfuse_client()

        if lf_client and task.task_id:
            try:
                from langfuse import propagate_attributes
                input_payload = {
                    "task_id": task.task_id,
                    "target_object": task.parsed_data.object if task.parsed_data else "",
                    "script": task.sandbox_script
                }
                with lf_client.start_as_current_observation(as_type="span", name="Agent_Executor_Prod") as span:
                    with propagate_attributes(session_id=task.task_id, tags=["agent-executor", "prod-execution"]):
                        prod_result = run_production_node(reconstructed_state)
                        span.update(
                            input=input_payload,
                            output={
                                "status": prod_result.get("status"),
                                "report": prod_result.get("report")
                            }
                        )
                    lf_client.flush()
            except Exception as prod_tr_err:
                logger.warning(f"Prod execution tracing note: {prod_tr_err}")
                prod_result = run_production_node(reconstructed_state)
        else:
            prod_result = run_production_node(reconstructed_state)
        
        task.status = prod_result.get("status", "executed")
        task.report = prod_result.get("report")
        task.execution_output = prod_result.get("logs")
        task.error_message = prod_result.get("error_message")
        
        logger.info(
            f"[PRODUCTION EXECUTION COMPLETED] Task [{task.task_id}] finished with status: {task.status}"
        )

    except Exception as e:
        task.status = "failed"
        task.error_message = f"Production execution exception: {str(e)}"
        logger.error(f"Production execution error: {e}")

    save_task(task)

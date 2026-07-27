"""
Parser Agent Service for structured extraction and input validation.
"""

import json
import logging
import os
import re
from typing import Tuple, Optional
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool
from shared.config import load_config
from shared.models import TaskEvent, ParseTaskResponse
from shared.db import save_task, init_db, get_prompt
from shared.tracing import get_langfuse_handler

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ParserAgent")

init_db()

# Load Configuration
settings, yaml_config = load_config()

# Resolve queue names from declarative pipeline config
_step = yaml_config.get_pipeline_step("parser")
_SUBSCRIBE_QUEUE = _step.subscribe_queue if _step else "q.tasks.raw"
_PUBLISH_QUEUE = _step.publish_queue if _step else "q.tasks.parsed"
logger.info(f"Parser pipeline: {_SUBSCRIBE_QUEUE} -> {_PUBLISH_QUEUE}")

# Setup RabbitMQ broker
rabbitmq_url = os.getenv("RABBITMQ_URL", settings.rabbitmq_url)
broker = RabbitBroker(rabbitmq_url)
app = FastStream(broker)

from shared.llm import get_llm

llm = None
provider_info = "uninitialized"
try:
    llm, provider_info = get_llm(settings)
except Exception as e:
    logger.warning(f"LLM init failed at startup (will retry on first request): {e}")
logger.info(f"ParserAgent LLM provider: {provider_info}")


def _match_regex_object(text_lower: str) -> str:
    for obj in yaml_config.allowed_objects:
        if obj.lower() in text_lower:
            return obj
    return yaml_config.allowed_objects[0] if (yaml_config.allowed_objects and len(yaml_config.allowed_objects) > 0) else "pg-billing-prod"


def _match_regex_object_type(text_lower: str) -> str:
    for obj_type in yaml_config.allowed_object_types:
        if obj_type.lower().replace("_", "") in text_lower.replace(" ", ""):
            return obj_type
    if "patroni" in text_lower:
        return "patroni_cluster"
    if any(k in text_lower for k in ["bouncer", "pgbouncer", "пул", "pool"]):
        return "pgbouncer"
    if "redis" in text_lower:
        return "redis_sentinel"
    if any(k in text_lower for k in ["postgres", "постгрес", "субд", "баз", "pg"]):
        return "postgres_standalone"
    return yaml_config.allowed_object_types[0] if yaml_config.allowed_object_types else "postgres_standalone"


def _match_regex_action(text_lower: str) -> str:
    for act in yaml_config.allowed_actions:
        if act.lower().replace("_", "") in text_lower.replace(" ", ""):
            return act
    if any(k in text_lower for k in ["миграци", "схем", "колонк", "индекс", "таблиц", "analyze", "ddl"]):
        return "schema_migration"
    if any(k in text_lower for k in ["бэкап", "резервн", "dump", "pg_dump", "basebackup", "копировани"]):
        return "backup_restore"
    if any(k in text_lower for k in ["сертификат", "ssl", "tls"]):
        return "ssl_renew"
    if any(k in text_lower for k in ["аудит", "безопасн", "compliance", "прав"]):
        return "compliance_audit"
    if any(k in text_lower for k in ["os", "пакет", "обновлени", "apt", "apk", "bouncer", "pgbouncer", "пул", "pool"]):
        return "os_upgrade"
    return "unknown_action"


def _match_regex_priority(text_lower: str) -> str:
    if any(kw in text_lower for kw in ["критич", "срочн", "critical", "urgent"]):
        return "critical"
    if "высок" in text_lower or "high" in text_lower:
        return "high"
    if "низк" in text_lower or "low" in text_lower:
        return "low"
    return "medium"


def parse_with_regex_fallback(raw_text: str) -> ParseTaskResponse:
    """
    Rule-based/Regex parser fallback that extracts task variables
    when LLM Studio / DeepSeek API is offline.
    """
    logger.warning("Parser: LLM API is offline. Running Regex Parser Fallback...")
    text_lower = raw_text.lower()
    
    matched_object = _match_regex_object(text_lower)
    matched_type = _match_regex_object_type(text_lower)
    matched_action = _match_regex_action(text_lower)
    priority = _match_regex_priority(text_lower)
        
    is_downtime = any(kw in text_lower for kw in ["downtime", "простой", "останов", "перезагруз", "reboot"])
    
    default_sla = getattr(yaml_config, "default_sla_minutes", 60)
    sla_match = re.search(r"(\d+)\s*(минут|мин|min)", text_lower)
    sla_minutes = int(sla_match.group(1)) if sla_match else default_sla
    
    from shared.models import Subtask
    subtasks = [
        Subtask(
            order=1,
            action=f"Выполнить операцию {matched_action} на объекте {matched_object}",
            constraints=["Убедиться в отсутствии блокировок"]
        ),
        Subtask(
            order=2,
            action="Сформировать лог-отчет проведения работ",
            constraints=[]
        )
    ]
    
    return ParseTaskResponse(
        priority=priority,
        action_type=matched_action,
        object=matched_object,
        object_type=matched_type,
        purpose=f"Проведение регламентных работ: {raw_text[:60]}...",
        subtasks=subtasks,
        sla_minutes=sla_minutes,
        is_downtime=is_downtime
    )


def parse_with_llm(raw_text: str, trace_id: Optional[str] = None) -> Tuple[ParseTaskResponse, dict]:
    """
    Calls LLM using Pydantic structured output validation (ParseTaskResponse).
    Guarantees schema validation against Pydantic model.
    """
    active_prompt = get_prompt("parser", yaml_config.parser_prompt)
    pydantic_parser = PydanticOutputParser(pydantic_object=ParseTaskResponse)
    format_instructions = pydantic_parser.get_format_instructions().replace("{", "{{").replace("}", "}}")
    escaped_prompt = active_prompt.replace("{", "{{").replace("}", "}}")
    full_system_prompt = f"{escaped_prompt}\n\nSTRICT SCHEMA FORMAT INSTRUCTIONS:\n{format_instructions}"

    prompt = ChatPromptTemplate.from_messages([
        ("system", full_system_prompt),
        ("human", "Request text: {text}")
    ])

    active_llm, _ = get_llm(settings, yaml_config)
    langfuse_handler = get_langfuse_handler()
    invoke_config = {
        "run_name": "Agent_Parser",
        "tags": ["agent-parser"]
    }
    if langfuse_handler:
        invoke_config["callbacks"] = [langfuse_handler]
        if trace_id:
            invoke_config["metadata"] = {
                "langfuse_session_id": trace_id,
                "langfuse_tags": ["agent-parser"]
            }

    try:
        chain = prompt | active_llm
        response = chain.invoke({"text": raw_text}, config=invoke_config)
        content = response.content.strip()

        token_usage = {}
        if hasattr(response, "response_metadata") and "token_usage" in response.response_metadata:
            token_usage = response.response_metadata["token_usage"]

        # Parse JSON and validate against Pydantic schema
        if content.startswith("```"):
            lines = content.splitlines()
            content = "\n".join(lines[1:-1]) if len(lines) > 2 else content.replace("```json", "").replace("```", "")
        
        parsed_json = json.loads(content.strip())
        parsed_model = ParseTaskResponse(**parsed_json)
        llm_info = {
            "prompt": active_prompt,
            "raw_response": content,
            "token_usage": token_usage
        }
        return parsed_model, llm_info
    except Exception as err:
        logger.warning(f"JSON parsing error: {err}. Running Regex Parser Fallback...")
        parsed_model = parse_with_regex_fallback(raw_text)
        return parsed_model, {
            "prompt": active_prompt,
            "raw_response": str(err),
            "fallback": "regex"
        }


@tool
def parse_task_intent_tool(raw_text: str) -> str:
    """
    LangChain Tool: Parses unstructured DBA request text into a validated Pydantic JSON structure.
    Returns JSON string representation of ParseTaskResponse.
    """
    parsed_model, _ = parse_with_llm(raw_text)
    return parsed_model.model_dump_json()


def _assign_risk_level(parsed: ParseTaskResponse, raw_text: str) -> ParseTaskResponse:
    """Evaluates and assigns risk_level and requires_critical_confirmation."""
    raw_lower = raw_text.lower()
    critical_kw = ["drop", "truncate", "delete", "grant superuser", "revoke all", "rm -rf"]
    if any(kw in raw_lower for kw in critical_kw):
        risk = "CRITICAL"
        req = True
    elif parsed.action_type == "os_upgrade" or "reboot" in raw_lower or "upgrade" in raw_lower:
        risk = "HIGH"
        req = False
    elif parsed.action_type == "compliance_audit" or ("select" in raw_lower and not parsed.is_downtime):
        risk = "LOW"
        req = False
    else:
        risk = "MEDIUM"
        req = False

    data = parsed.model_dump()
    data["risk_level"] = risk
    data["requires_critical_confirmation"] = req
    return ParseTaskResponse(**data)


def _validate_parsed_data(parsed_data: ParseTaskResponse) -> None:
    """Validates parsed task data against configured allowed values."""
    if parsed_data.object not in set(yaml_config.allowed_objects):
        raise ValueError(f"Объект с ID '{parsed_data.object}' отсутствует в разрешенном справочнике.")
    if parsed_data.object_type not in set(yaml_config.allowed_object_types):
        raise ValueError(f"Тип объекта '{parsed_data.object_type}' недопустим.")
    if parsed_data.priority not in set(yaml_config.allowed_priorities):
        raise ValueError(f"Приоритет '{parsed_data.priority}' недопустим.")
    if parsed_data.action_type not in set(yaml_config.allowed_actions):
        raise ValueError(f"Тип действия '{parsed_data.action_type}' недопустим.")


@broker.subscriber(_SUBSCRIBE_QUEUE)
@broker.publisher(_PUBLISH_QUEUE)
async def handle_parse(event: TaskEvent) -> TaskEvent:
    """
    Subscriber handler for raw tasks. Parses and runs input validation.
    """
    logger.info(f"Received raw task [{event.task_id}]: {event.raw_text}")
    save_task(event)

    try:
        if event.llm_logs is None:
            event.llm_logs = {}
        try:
            parsed_data, llm_info = parse_with_llm(event.raw_text, trace_id=event.trace_id)
            event.llm_logs["parser"] = llm_info
        except Exception as llm_err:
            logger.error(f"LLM call failed: {llm_err}. Trying regex fallback...")
            parsed_data = parse_with_regex_fallback(event.raw_text)
            event.llm_logs["parser"] = {"fallback": "regex", "error": str(llm_err)}

        parsed_data = _assign_risk_level(parsed_data, event.raw_text)
        logger.info("Running Input Guard validation...")
        _validate_parsed_data(parsed_data)

        event.parsed_data = parsed_data
        event.status = "parsed"
        logger.info(
            f"[GUARD PASSED] Task [{event.task_id}] validated! Risk Level: {parsed_data.risk_level} (Critical Confirmation: {parsed_data.requires_critical_confirmation})"
        )

    except Exception as err:
        event.status = "failed"
        event.error_message = f"Parser Error: {str(err)}"
        logger.error(
            f"[GUARD REJECTED] Task [{event.task_id}] failed: {err}"
        )

    save_task(event)
    return event

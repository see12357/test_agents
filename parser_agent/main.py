"""
Parser Agent Service.
Subscribes to q.tasks.raw, parses raw text using OpenAI-compatible ChatOpenAI,
validates against configuration guards, and publishes to q.tasks.parsed.
Loads system prompts dynamically from SQLite database at runtime.
Strictly PEP 8 compliant.
"""

import json
import logging
import os
import re
from typing import List, Tuple
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from shared.config import load_config
from shared.models import TaskEvent, ParseTaskResponse
from shared.db import save_task, init_db, get_prompt

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

llm, provider_info = get_llm(settings)
logger.info(f"ParserAgent initialized LLM provider: {provider_info}")


def get_langfuse_callbacks() -> List:
    """
    Registers and returns Langfuse callback handler if credentials are set.
    """
    public_key = os.getenv("LANGFUSE_PUBLIC_KEY", settings.langfuse_public_key)
    secret_key = os.getenv("LANGFUSE_SECRET_KEY", settings.langfuse_secret_key)
    
    if public_key and secret_key:
        try:
            from langfuse.langchain import CallbackHandler
            handler = CallbackHandler()
            logger.info("Langfuse Tracing callback successfully initialized.")
            return [handler]
        except Exception as e:
            logger.warning(f"Failed to initialize Langfuse callback: {e}")
    return []


def _match_regex_object(text_lower: str) -> str:
    for obj in yaml_config.allowed_objects:
        if obj.lower() in text_lower:
            return obj
    return "pg-billing-prod"


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
    return "postgres_standalone"


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
    return "os_upgrade"


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
    
    sla_match = re.search(r"(\d+)\s*(минут|мин|min)", text_lower)
    sla_minutes = int(sla_match.group(1)) if sla_match else 60
    
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


def parse_with_llm(raw_text: str, callbacks: List) -> Tuple[ParseTaskResponse, dict]:
    """
    Calls OpenAI-compatible LLM to parse raw text into structured JSON.
    Extracts token metrics and raw text.
    """
    active_prompt = get_prompt("parser", yaml_config.parser_prompt)
    active_prompt = active_prompt.replace("{", "{{").replace("}", "}}")

    prompt = ChatPromptTemplate.from_messages([
        ("system", active_prompt),
        ("human", "Текст заявки: {text}\n\nВерни JSON по схеме.")
    ])

    chain = prompt | llm
    response = chain.invoke({"text": raw_text}, config={"callbacks": callbacks})

    content = response.content.strip()
    logger.info(f"LLM response: {content}")
    
    token_usage = {}
    if hasattr(response, "response_metadata") and "token_usage" in response.response_metadata:
        token_usage = response.response_metadata["token_usage"]

    llm_info = {
        "prompt": active_prompt,
        "raw_response": content,
        "token_usage": token_usage
    }

    parsed_json = json.loads(content)
    return ParseTaskResponse(**parsed_json), llm_info




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
        callbacks = get_langfuse_callbacks()
        try:
            parsed_data, llm_info = parse_with_llm(event.raw_text, callbacks)
            event.llm_logs["parser"] = llm_info
        except Exception as llm_err:
            logger.error(f"LLM call failed: {llm_err}. Trying regex fallback...")
            parsed_data = parse_with_regex_fallback(event.raw_text)
            event.llm_logs["parser"] = {"fallback": "regex", "error": str(llm_err)}

        allowed_objs = set(yaml_config.allowed_objects)
        allowed_types = set(yaml_config.allowed_object_types)
        allowed_priorities = set(yaml_config.allowed_priorities)
        allowed_actions = set(yaml_config.allowed_actions)

        logger.info("Running Input Guard validation...")

        if parsed_data.object not in allowed_objs:
            raise ValueError(
                f"Объект с ID '{parsed_data.object}' отсутствует в разрешенном справочнике."
            )
        if parsed_data.object_type not in allowed_types:
            raise ValueError(
                f"Тип объекта '{parsed_data.object_type}' недопустим."
            )
        if parsed_data.priority not in allowed_priorities:
            raise ValueError(
                f"Приоритет '{parsed_data.priority}' недопустим."
            )
        if parsed_data.action_type not in allowed_actions:
            raise ValueError(
                f"Тип действия '{parsed_data.action_type}' недопустим."
            )

        event.parsed_data = parsed_data
        event.status = "parsed"
        logger.info(
            f"[GUARD PASSED] Task [{event.task_id}] successfully validated!"
        )

    except Exception as err:
        event.status = "failed"
        event.error_message = f"Parser Error: {str(err)}"
        logger.error(
            f"[GUARD REJECTED] Task [{event.task_id}] failed: {err}"
        )

    save_task(event)
    return event

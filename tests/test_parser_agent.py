"""
Unit tests for Parser Agent rule-based regex extraction and fallback logic.
"""

from parser_agent.main import parse_with_regex_fallback, _match_regex_priority


def test_parse_with_regex_fallback_pgbouncer():
    prompt = "Срочно на кластере pg-crm-prod провести оптимизацию пула соединений PgBouncer, SLA 20 минут."
    res = parse_with_regex_fallback(prompt)

    assert res.object == "pg-crm-prod"
    assert res.object_type == "pgbouncer"
    assert res.priority == "critical"
    assert res.sla_minutes == 20


def test_parse_with_regex_fallback_schema_migration():
    prompt = "Миграция схемы на pg-analytics-test: применить SQL индекс CONCURRENTLY."
    res = parse_with_regex_fallback(prompt)

    assert res.object == "pg-analytics-test"
    assert res.action_type == "schema_migration"
    assert not res.is_downtime


def test_parse_with_regex_fallback_backup():
    prompt = "Выполнить резервное копирование pg_dump на pg-orders-prod."
    res = parse_with_regex_fallback(prompt)

    assert res.object == "pg-orders-prod"
    assert res.action_type == "backup_restore"


def test_match_regex_priority():
    assert _match_regex_priority("срочная аварийная задача") == "critical"
    assert _match_regex_priority("высокий приоритет") == "high"
    assert _match_regex_priority("низкий приоритет") == "low"
    assert _match_regex_priority("обычная задача") == "medium"

"""
Unit tests for Executor Agent dynamic rule construction and prompt generation.
"""

from executor_agent.main import _build_dynamic_prompt_rules


def test_build_dynamic_prompt_rules_sql():
    parsed_data = {
        "object_type": "postgres_standalone",
        "action_type": "schema_migration",
        "priority": "high",
        "is_downtime": False
    }
    rules = _build_dynamic_prompt_rules(parsed_data, is_sql=True)
    joined = "\n".join(rules)

    assert "SET lock_timeout = '5s';" in joined
    assert "users" in joined


def test_build_dynamic_prompt_rules_patroni():
    parsed_data = {
        "object_type": "patroni_cluster",
        "action_type": "os_upgrade",
        "priority": "critical",
        "is_downtime": True
    }
    rules = _build_dynamic_prompt_rules(parsed_data, is_sql=False)
    joined = "\n".join(rules)

    assert "patronictl" in joined
    assert "CRITICAL: Service downtime is planned" in joined
    assert "HIGH PRIORITY TASK" in joined


def test_build_dynamic_prompt_rules_ssl():
    parsed_data = {
        "object_type": "postgres_standalone",
        "action_type": "ssl_renew",
        "priority": "medium",
        "is_downtime": False
    }
    rules = _build_dynamic_prompt_rules(parsed_data, is_sql=False)
    joined = "\n".join(rules)

    assert "openssl" in joined


def test_build_dynamic_prompt_rules_os_upgrade():
    parsed_data = {
        "object_type": "postgres_standalone",
        "action_type": "os_upgrade",
        "priority": "medium",
        "is_downtime": False
    }
    rules = _build_dynamic_prompt_rules(parsed_data, is_sql=False)
    joined = "\n".join(rules)

    assert "apk" in joined

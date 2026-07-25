#!/usr/bin/env python3
"""
Automated End-to-End Test Runner for 10 DBA Support Prompts.
Submits tasks, monitors agent pipeline, auto-approves human-in-the-loop,
verifies production execution, and generates a structured summary table.
"""

import time
import requests

GATEWAY_URL = "http://localhost:8081"

TEST_PROMPTS = [
    (1, "PgBouncer Optimization", "Срочно на кластере pg-crm-prod провести оптимизацию пула соединений PgBouncer, обновить конфигурацию pgbouncer.ini и выполнить мягкий перезапуск без разрыва сессий пользователей. Время SLA - 20 минут."),
    (2, "Zero-Downtime Migration", "Критическая миграция схемы на pg-analytics-test: применить SQL-скрипт создания индекса CONCURRENTLY на таблице transactions с обязательной установкой lock_timeout. Допустимый простой - 0 минут."),
    (3, "Patroni Backup & Replication", "На кластере pg-orders-prod выполнить плановое создание резервной копии pg_dump и проверить статус репликации standby узлов перед обновлением Patroni."),
    (4, "SSL/TLS Cert Rotation", "На сервере pg-auth-stage выполнить плановую ротацию SSL/TLS сертификатов базы данных, проверить срок действия и валидность параметров ssl_cert_file без перезапуска СУБД."),
    (5, "Compliance & Audit", "Провести плановый аудит безопасности и соответствия стандартам compliance на pg-warehouse-prod: проверить список активных пользователей, права доступа и параметры логирования pg_stat_statements."),
    (6, "OS Upgrade Standalone", "Провести обновление пакетов операционной системы на автономном сервере pg-users-stage, проверить отсутствие блокировок пакетного менеджера и готовность службы PostgreSQL."),
    (7, "Patroni Maintenance Mode", "Критическая задача на pg-billing-prod: перевести кластер Patroni в режим обслуживания (pause), проверить доступность WAL-репликации на слейв-узлах и подготовить отчет."),
    (8, "Add Column & ANALYZE", "На кластере pg-analytics-test выполнить добавление новой колонки в таблицу users, обновить статистику планировщика ANALYZE и проверить отклик базы."),
    (9, "Emergency PgBouncer Max Conn", "Срочная аварийная задача на pg-orders-prod: увеличить лимит клиентских соединений max_client_conn до 1000 в pgbouncer.ini и применить перезагрузку конфигурации."),
    (10, "Physical pg_basebackup", "На сервере pg-warehouse-prod выполнить физическое резервное копирование pg_basebackup, проверить целостность получившегося дампа и зафиксировать SLA.")
]


def poll_status(task_id, timeout_sec=120):
    """Poll task status until terminal state or timeout, tolerating transient errors."""
    start = time.time()
    last_status = None
    while time.time() - start < timeout_sec:
        try:
            r = requests.get(f"{GATEWAY_URL}/task/{task_id}", timeout=30)
            r.raise_for_status()
            data = r.json()
            st = data.get("status")
            if st != last_status:
                last_status = st
            if st == "tested":
                try:
                    requests.post(f"{GATEWAY_URL}/task/{task_id}/approve", timeout=30)
                except Exception:
                    pass
            if st in ("executed", "failed", "rejected"):
                return data
        except Exception:
            pass
        time.sleep(2)
    return None


def run_e2e_tests():
    print("=" * 75)
    print("      AUTOMATED END-TO-END SUITE: TESTING 10 DBA PROMPTS")
    print("=" * 75)

    results = []

    for test_no, title, prompt in TEST_PROMPTS:
        print(f"\n[TEST {test_no:02d}/10] Submitting: '{title}'...")
        try:
            resp = requests.post(f"{GATEWAY_URL}/task", json={"text": prompt}, timeout=30)
            resp.raise_for_status()
            task_id = resp.json()["task_id"]
            print(f"  |-- Task ID: {task_id}")

            data = poll_status(task_id, timeout_sec=120)

            if data is None:
                print("  |-- TIMEOUT: Pipeline did not reach terminal state in 120s")
                results.append({"test_no": test_no, "title": title, "object": "N/A",
                                "action_type": "N/A", "status": "timeout"})
                continue

            parsed = data.get("parsed_data") or {}
            final_status = data.get("status", "unknown")
            results.append({
                "test_no": test_no,
                "title": title,
                "object": parsed.get("object", "N/A"),
                "action_type": parsed.get("action_type", "N/A"),
                "status": final_status
            })
            print(f"  |-- RESULT: [{final_status.upper()}] Object: {parsed.get('object')}, Action: {parsed.get('action_type')}")

        except Exception as e:
            print(f"  |-- ERROR: {e}")
            results.append({"test_no": test_no, "title": title, "object": "N/A",
                            "action_type": "N/A", "status": "error"})

    print("\n" + "=" * 88)
    print("                     E2E TEST RESULTS SUMMARY TABLE")
    print("=" * 88)
    print(f"{'#':<3} | {'Test Scenario':<30} | {'Target Object':<18} | {'Action':<16} | {'Status':<10}")
    print("-" * 88)
    for r in results:
        print(f"{r['test_no']:<3} | {r['title']:<30} | {r['object']:<18} | {r['action_type']:<16} | {r['status'].upper():<10}")
    print("=" * 88)


if __name__ == "__main__":
    run_e2e_tests()

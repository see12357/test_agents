# Multi-Agent Platform for Autonomous DB Support

Мультиагентная платформа на базе **FastStream**, **RabbitMQ**, **LangChain**, **LangGraph** и **ChromaDB** для автоматизированного обслуживания, миграций и резервного копирования инфраструктуры PostgreSQL/Patroni/PgBouncer с поддержкой песочницы Docker и контроля оператора (Human-in-the-Loop).

---

## 1. Как запускать

Для работы требуется **Docker**, **Docker Compose** и **Python 3.11+**.

### Предварительная настройка

1. **Клонировать репозиторий и перейти в директорию проекта:**
   ```bash
   git clone <repository_url>
   cd test_agents
   ```

2. **Установите виртуальное окружение и зависимости:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Настройка переменных окружения (`.env`):**
   Укажите выбор провайдера (`deepseek` или `gigachat`) и ключи API в файле `.env`:
   ```env
   # Выбор активного провайдера: "deepseek" | "gigachat"
   LLM_PROVIDER=deepseek

   # DeepSeek Cloud API
   DEEPSEEK_API_KEY=your_deepseek_api_key_here
   DEEPSEEK_MODEL=deepseek-v4-flash
   DEEPSEEK_BASE_URL=https://api.deepseek.com

   # Sber GigaChat API
   GIGACHAT_CREDENTIALS=your_gigachat_credentials_here
   GIGACHAT_MODEL=GigaChat-2
   GIGACHAT_SCOPE=GIGACHAT_API_B2B
   ```

4. **Запуск инфраструктурных сервисов и агентов в Docker:**
   ```bash
   docker compose up -d --build
   ```

---

### Запуск системы и тестирования

* **Интерактивный CLI для подачи заявки и approval оператора:**
  ```bash
  python cli.py "На кластере pg-analytics-test выполнить ANALYZE на таблице users."
  ```

* **Запуск изолированных Unit-тестов (15 тестов):**
  ```bash
  PYTHONPATH=. pytest tests/
  ```

* **Запуск автоматической E2E-сьюты из 10 DBA промптов:**
  ```bash
  python test_e2e_all_prompts.py
  ```

---

## Особенности запуска и работы

### 1. Инициализация (Первый запуск)
При первом запуске Docker Compose разворачивает контейнеры инфраструктуры и агентов:
- **`rabbitmq`**: Брокер сообщений AMQP (управление очередями).
- **`chromadb`**: Векторная база данных.
- **`mock-postgres`**: Целевой тестовый экземпляр PostgreSQL 15.
- **`gateway`**: FastAPI Шлюз REST API + управление состоянием задач в SQLite.
- **`agent-parser`**: Агент структурирования заявок.
- **`agent-rag`**: Агент векторного поиска регламентов.
- **`agent-executor`**: Агент генерации и тестирования скриптов в Docker-песочнице.

При старте `agent-rag` автоматически загрузит модель эмбеддингов `intfloat/multilingual-e5-large` из HuggingFace.

### 2. Декларативная маршрутизация пайплайна
Маршрутизация очередей агентов полностью декларируется в `config.yaml` в секции `pipeline.steps`. Каждое звено агентов динамически считывает имена очередей подписки и публикации при старте.

### 3. Гарантия безопасности (Sandbox ReALF Trial + Human-in-the-Loop)
Ни один сгенерированный SQL или Bash скрипт не попадает на продуктивную СУБД напрямую.
1. **Песочница Docker:** Скрипт исполняется в изолированном контейнере Alpine PostgreSQL.
2. **Проверка тайм-аутов блокировок:** Для SQL-миграций автоматически выставляется `SET lock_timeout = '5s';`.
3. **Approval Оператора:** При успешном прохождении песочницы система ставит задачу на паузу (статус `TESTED`) и запрашивает явное подтверждение оператора через CLI или API.

---

## 2. Базовое решение (Пайплайн агентов)

В рамках архитектуры платформы реализован 4-стадийный асинхронный граф взаимодействия агентов:

**Техническая реализация:**
- **Модель:** DeepSeek / OpenAI Chat API (через `langchain-openai`).
- **Embeddings:** `intfloat/multilingual-e5-large` (через `langchain-community` & `ChromaDB`).
- **Брокер сообщений:** RabbitMQ + FastStream.
- **Оркестрация и песочница:** LangGraph (StateGraph с условными переходами) + Docker SDK.

**Логика работы пайплайна:**
1. **Gateway API:** Принимает текст заявки, сохраняет задачу в SQLite со статусом `pending` и публикует событие в очередь `q.tasks.raw`.
2. **Parser Agent (`q.tasks.raw` ➔ `q.tasks.parsed`):** Извлекает объект (`pg-analytics-test`), тип объекта (`patroni_cluster`), приоритет, SLA и операцию. Валидирует параметры через Pydantic Input Guard. При сбое сети мгновенно включает Regex Fallback.
3. **RAG Agent (`q.tasks.parsed` ➔ `q.tasks.ready_for_execution`):** Находит соответствующие инструкции DBA регламентов в ChromaDB по вектору `e5-large`. При недоступности вектора использует робастный keyword-поиск по локальным файлам.
4. **Executor Agent (`q.tasks.ready_for_execution` ➔ `q.tasks.human_approval`):** Генерирует POSIX-совместимый Bash/SQL скрипт, выполняет его в изолированном Docker-контейнере и проверяет логи на ошибки. При успехе задерживает выполнение в точке прерывания LangGraph (статус `TESTED`).
5. **Production Execution:** После вызова эндпоинта `/task/{id}/approve` скрипт безопасно применяется к целевой базе и генерируется финальный отчёт.

---

## 3. Расширенные возможности (Декларативность и тестирование)

- **Декларативный конфигурационный файл `config.yaml`:**
  ```yaml
  pipeline:
    steps:
      - name: parser
        subscribe_queue: "q.tasks.raw"
        publish_queue: "q.tasks.parsed"
      - name: rag
        subscribe_queue: "q.tasks.parsed"
        publish_queue: "q.tasks.ready_for_execution"
      - name: executor
        subscribe_queue: "q.tasks.ready_for_execution"
        publish_queue: "q.tasks.human_approval"
  ```
- **Изолированное Unit-тестирование:** Набор из 15 unit-тестов проверяет регулярный парсер, нормализаторы Pydantic, динамические правила промпта и загрузку конфигурации без внешних зависимостей.

---

## 4. Примеры выполнения задач

### a. Выполнение заявки через интерактивный CLI

```text
(.venv) danilaganits@MacBook-Pro-Danila test_agents % python cli.py "На кластере pg-analytics-test выполнить ANALYZE на таблице users."

=================================================================
 DB SUPPORT AGENT PLATFORM - TASK SUBMISSION
=================================================================
[*] Request: 'На кластере pg-analytics-test выполнить ANALYZE на таблице users.'
[✓] Task successfully created! Assigned Task ID: 2e1c6c22-04ab-424f-8a2a-ecf5909728f3

=================================================================
 REAL-TIME MULTI-AGENT EXECUTION & REASONING PIPELINE
=================================================================

[AGENT EVENT] Status transition: INITIAL -> [PENDING]

--- [STEP 1/4] PARSER AGENT (Structured Extraction & Guard Validation) ---
  • Target Object : pg-analytics-test (patroni_cluster)
  • Action Type   : schema_migration
  • Priority      : LOW
  • SLA Limit     : 30 minutes
  • Downtime Flag : False
  • Input Guard   : PASSED (All parameter constraints validated)

--- [STEP 2/4] RAG AGENT (ChromaDB Vector Retrieval: e5-large) ---
  • Search Query  : Extracted from parsed subtasks
  • Vector Index  : ChromaDB / intfloat/multilingual-e5-large
  • Retrieved RAG Context Guidelines:
    --- Manual (Matched Guideline: schema_migration.md) ---
    # Schema Migration Guidelines

=================================================================
 STEP 3/4: EXECUTOR AGENT (Sandbox ReALF Trial Results)
=================================================================

--- SANDBOX TRIAL LOGS (Docker / Subprocess Execution) ---
INFO: === Starting Schema Migration (ANALYZE) on pg-analytics-test ===
Target table: users
SET lock_timeout = '5s'; ANALYZE users;
ANALYZE completed successfully.
-----------------------------------------------------------------

=================================================================
 STEP 4/4: HUMAN-IN-THE-LOOP (Operator Approval Required)
=================================================================
Task ID: 2e1c6c22-04ab-424f-8a2a-ecf5909728f3

--- GENERATED SQL / BASH SCRIPT ---
SET lock_timeout = '5s';
ANALYZE VERBOSE users;
-----------------------------------------------------------------

Approve script for production execution? (y/n) [y]: y

[✓] Task APPROVED! Sent to q.tasks.execute_prod queue.
[*] Executing script on target environment...

=================================================================
 LLM CALL TRANSPARENCY & TOKEN METRICS
=================================================================

--- [PARSER AGENT LLM TRACE] ---
  • Active System Prompt : You are a Senior Database Administrator AI Assistant...
  • Raw LLM JSON Return  : {"priority": "low", "action_type": "schema_migration", ...}
  • Token Metrics        : Prompt: 512 | Completion: 184 | Total: 696 tokens

=================================================================
 PRODUCTION EXECUTION COMPLETED - FINAL REPORT
=================================================================

--- VERIFIED PRODUCTION OUTPUT ---
ANALYZE

--- AGENT SUMMARY REPORT ---
# Отчет о выполнении работ по заявке 2e1c6c22-04ab-424f-8a2a-ecf5909728f3
Статус: УСПЕШНО ВЫПОЛНЕНО
=================================================================
```

---

### b. Сводный отчёт прогона автоматической E2E-сьюты из 10 DBA промптов

```text
========================================================================================
                     E2E TEST RESULTS SUMMARY TABLE
========================================================================================
#   | Test Scenario                  | Target Object      | Action           | Status    
----------------------------------------------------------------------------------------
1   | PgBouncer Optimization         | pg-crm-prod        | schema_migration | EXECUTED  
2   | Zero-Downtime Migration        | pg-analytics-test  | schema_migration | EXECUTED  
3   | Patroni Backup & Replication   | pg-orders-prod     | backup_restore   | EXECUTED  
4   | SSL/TLS Cert Rotation          | pg-auth-stage      | ssl_renew        | EXECUTED  
5   | Compliance & Audit             | pg-warehouse-prod  | compliance_audit | EXECUTED  
6   | OS Upgrade Standalone          | pg-users-stage     | os_upgrade       | EXECUTED  
7   | Patroni Maintenance Mode       | pg-billing-prod    | compliance_audit | EXECUTED  
8   | Add Column & ANALYZE           | pg-analytics-test  | schema_migration | EXECUTED  
9   | Emergency PgBouncer Max Conn   | pg-orders-prod     | schema_migration | EXECUTED  
10  | Physical pg_basebackup         | pg-warehouse-prod  | backup_restore   | EXECUTED  
========================================================================================
```

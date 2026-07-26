"""
Configuration loader for environment variables and YAML settings.
"""

import os
from typing import List, Tuple, Optional
import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PlatformSettings(BaseSettings):
    """
    Environment settings loaded from .env.
    """
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # Provider selection: "deepseek" | "gigachat"
    llm_provider: str = Field(default="deepseek")

    # DeepSeek Cloud configuration
    deepseek_api_key: str = Field(default="")
    deepseek_model: str = Field(default="deepseek-v4-flash")
    deepseek_base_url: str = Field(default="https://api.deepseek.com")

    # GigaChat (Sber) configuration
    gigachat_credentials: str = Field(default="")
    gigachat_model: str = Field(default="GigaChat-3-Ultra")
    gigachat_base_url: str = Field(default="https://gigachat.devices.sberbank.ru/api/v1")
    gigachat_verify_ssl_certs: bool = Field(default=False)


    gigachat_auth_url: str = Field(default="https://ngw.devices.sberbank.ru:9443/api/v2/oauth")
    llm_timeout: float = Field(default=45.0)
    llm_temperature: float = Field(default=0.0)

    rabbitmq_url: str = Field(default="amqp://guest:guest@localhost:5672/")
    chroma_host: str = Field(default="localhost")
    chroma_port: int = Field(default=8000)
    mock_database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/postgres"
    )
    gateway_url: str = Field(default="http://localhost:8081")
    sandbox_image: str = Field(default="postgres:15-alpine")
    sandbox_timeout: int = Field(default=15)
    retry_delay_seconds: int = Field(default=2)
    execution_timeout_seconds: int = Field(default=15)
    mock_postgres_host: str = Field(default="mock-postgres")
    mock_postgres_user: str = Field(default="postgres")
    mock_postgres_password: str = Field(default="postgres")
    db_path: str = Field(default="executor.db")
    health_check_task_id: str = Field(default="health-check-system-id")


class PipelineStep(BaseModel):
    """
    Model representing a step in the declarative agent pipeline.
    """
    name: str
    description: str = ""
    subscribe_queue: str
    publish_queue: str = ""
    approval_queue: str = ""  # Optional: callback queue for human-in-the-loop approval
    enabled: bool = True


class YAMLConfig(BaseModel):
    """
    YAML configuration models for agents, pipeline routing, and validation.
    """
    llm_provider: str = "gigachat"
    embedding_model: str = "intfloat/multilingual-e5-large"
    chroma_collection_name: str = "db_manuals"
    rag_fallback_mapping: dict = Field(default_factory=dict)
    allowed_objects: List[str] = Field(default_factory=list)
    allowed_object_types: List[str] = Field(default_factory=list)
    allowed_priorities: List[str] = Field(default_factory=list)
    allowed_actions: List[str] = Field(default_factory=list)
    pipeline_steps: List[PipelineStep] = Field(default_factory=list)
    chunk_size: int = 1000
    chunk_overlap: int = 200
    rag_initial_candidates: int = 5
    rag_reranker_enabled: bool = True
    rag_reranker_top_k: int = 2
    rag_reranker_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    parser_prompt: str = ""
    rag_prompt: str = ""
    executor_prompt: str = ""
    executor_rules: dict = Field(default_factory=dict)
    executor_max_retries: int = 3
    default_sla_minutes: int = 60
    retry_delay_seconds: int = 2
    execution_timeout_seconds: int = 15

    def get_pipeline_step(self, name: str) -> Optional[PipelineStep]:
        """Returns the pipeline step configuration for a given agent name."""
        for step in self.pipeline_steps:
            if step.name == name:
                return step
        return None


def load_config() -> Tuple[PlatformSettings, YAMLConfig]:
    """
    Load env settings and YAML configurations.
    Returns:
        Tuple[PlatformSettings, YAMLConfig]: Config instances.
    """
    settings = PlatformSettings()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(os.path.dirname(current_dir), "config.yaml")

    yaml_data = {}
    if os.path.exists(yaml_path):
        with open(yaml_path, "r", encoding="utf-8") as f:
            raw_yaml = yaml.safe_load(f) or {}

            yaml_data["llm_provider"] = (
                raw_yaml.get("platform", {})
                .get("llm_provider", "gigachat")
            )
            yaml_data["embedding_model"] = (
                raw_yaml.get("platform", {})
                .get("embedding_model", "intfloat/multilingual-e5-large")
            )
            yaml_data["chroma_collection_name"] = (
                raw_yaml.get("platform", {})
                .get("chroma_collection_name", "db_manuals")
            )
            yaml_data["rag_fallback_mapping"] = (
                raw_yaml.get("platform", {})
                .get("rag_fallback_mapping", {})
            )
            yaml_data["allowed_objects"] = (
                raw_yaml.get("validation", {})
                .get("allowed_objects", [])
            )
            yaml_data["allowed_object_types"] = (
                raw_yaml.get("validation", {})
                .get("allowed_object_types", [])
            )
            yaml_data["allowed_priorities"] = (
                raw_yaml.get("validation", {})
                .get("allowed_priorities", [])
            )
            yaml_data["allowed_actions"] = (
                raw_yaml.get("validation", {})
                .get("allowed_actions", [])
            )
            
            steps_raw = raw_yaml.get("pipeline", {}).get("steps", [])
            yaml_data["pipeline_steps"] = [
                PipelineStep(**step) for step in steps_raw
            ]

            rag_cfg = raw_yaml.get("agents", {}).get("rag", {})
            yaml_data["chunk_size"] = rag_cfg.get("chunk_size", 1000)
            yaml_data["chunk_overlap"] = rag_cfg.get("chunk_overlap", 200)
            yaml_data["rag_initial_candidates"] = rag_cfg.get("initial_candidates", 5)
            yaml_data["rag_reranker_enabled"] = rag_cfg.get("reranker_enabled", True)
            yaml_data["rag_reranker_top_k"] = rag_cfg.get("reranker_top_k", 2)
            yaml_data["rag_reranker_model"] = rag_cfg.get("reranker_model", "cross-encoder/ms-marco-MiniLM-L-6-v2")

            yaml_data["parser_prompt"] = (
                raw_yaml.get("agents", {})
                .get("parser", {})
                .get("prompt", "")
            )
            yaml_data["rag_prompt"] = rag_cfg.get("prompt", "")
            yaml_data["executor_prompt"] = (
                raw_yaml.get("agents", {})
                .get("executor", {})
                .get("prompt", "")
            )
            yaml_data["executor_rules"] = (
                raw_yaml.get("agents", {})
                .get("executor", {})
                .get("rules", {})
            )
            yaml_data["executor_max_retries"] = (
                raw_yaml.get("agents", {})
                .get("executor", {})
                .get("max_retries", 3)
            )

    yaml_config = YAMLConfig(**yaml_data)
    return settings, yaml_config

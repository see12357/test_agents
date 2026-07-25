"""
Configuration loader for the Agent Platform.
Loads environment variables and YAML configurations.
Strictly PEP 8 compliant.
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
    gigachat_model: str = Field(default="GigaChat")
    gigachat_base_url: str = Field(default="https://gigachat.devices.sberbank.ru/api/v1")
    gigachat_verify_ssl_certs: bool = Field(default=False)


    rabbitmq_url: str = Field(default="amqp://guest:guest@localhost:5672/")
    chroma_host: str = Field(default="localhost")
    chroma_port: int = Field(default=8000)
    mock_database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/postgres"
    )

    # Langfuse Config (Self-Hosted Local Instance)
    langfuse_secret_key: str = Field(default="")
    langfuse_public_key: str = Field(default="")
    langfuse_base_url: str = Field(default="http://localhost:3000")


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
    embedding_model: str = "intfloat/multilingual-e5-large"
    allowed_objects: List[str] = Field(default_factory=list)
    allowed_object_types: List[str] = Field(default_factory=list)
    allowed_priorities: List[str] = Field(default_factory=list)
    allowed_actions: List[str] = Field(default_factory=list)
    pipeline_steps: List[PipelineStep] = Field(default_factory=list)
    chunk_size: int = 1000
    chunk_overlap: int = 200
    rag_initial_candidates: int = 5
    rag_reranker_top_k: int = 2
    rag_reranker_model: str = "cross-encoder/ms-marco-MiniLM-L-6-v2"
    parser_prompt: str = ""
    rag_prompt: str = ""
    executor_prompt: str = ""
    executor_max_retries: int = 3

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

            yaml_data["embedding_model"] = (
                raw_yaml.get("platform", {})
                .get("embedding_model", "intfloat/multilingual-e5-large")
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
            yaml_data["executor_max_retries"] = (
                raw_yaml.get("agents", {})
                .get("executor", {})
                .get("max_retries", 3)
            )

    yaml_config = YAMLConfig(**yaml_data)
    return settings, yaml_config

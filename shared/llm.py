"""
Unified LLM Provider Initializer for DeepSeek Cloud and Sber GigaChat API.
"""

import logging
import os
import time
import uuid
import httpx
import requests
from typing import Tuple
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_openai import ChatOpenAI
from shared.config import PlatformSettings

logger = logging.getLogger("LLMProvider")


class GigaChatAuth(httpx.Auth):
    """
    Custom HTTPX Auth class for GigaChat OAuth token management.
    """
    def __init__(self, credentials: str, scope: str = "GIGACHAT_API_PERS", verify_ssl: bool = False, auth_url: str = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth", timeout: float = 45.0):
        self.credentials = credentials
        self.scope = scope
        self.verify_ssl = verify_ssl
        self.auth_url = auth_url
        self.timeout = timeout
        self.token = ""
        self.expires_at = 0.0

    def get_token(self) -> str:
        now = time.time()
        if self.token and self.expires_at > now + 60:
            return self.token

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Authorization": f"Basic {self.credentials}",
            "RqUID": str(uuid.uuid4())
        }
        payload = {"scope": self.scope}
        resp = requests.post(
            self.auth_url,
            headers=headers,
            data=payload,
            verify=self.verify_ssl,
            timeout=self.timeout
        )
        resp.raise_for_status()
        data = resp.json()

        self.token = data.get("access_token", "")
        expires_at_ms = data.get("expires_at", 0)
        if expires_at_ms > 1e11:
            self.expires_at = expires_at_ms / 1000.0
        else:
            self.expires_at = expires_at_ms if expires_at_ms > 0 else now + 1750.0

        logger.info("Refreshed GigaChat OAuth token (valid for ~30m)")
        return self.token

    def sync_auth_flow(self, request):
        request.headers["Authorization"] = f"Bearer {self.get_token()}"
        yield request

    async def async_auth_flow(self, request):
        request.headers["Authorization"] = f"Bearer {self.get_token()}"
        yield request


def get_llm(settings: PlatformSettings, yaml_config=None) -> Tuple[BaseChatModel, str]:
    """
    Initializes LLM instance dynamically based on environment or config.yaml settings.

    Returns:
        Tuple[BaseChatModel, str]: LLM instance and provider description.
    """
    provider = (
        os.getenv("LLM_PROVIDER")
        or (getattr(yaml_config, "llm_provider", "") if yaml_config else "")
        or getattr(settings, "llm_provider", "gigachat")
    ).lower().strip()
    gigachat_creds = (os.getenv("GIGACHAT_CREDENTIALS") or getattr(settings, "gigachat_credentials", "") or "").strip()
    deepseek_key = (os.getenv("DEEPSEEK_API_KEY") or getattr(settings, "deepseek_api_key", "") or "").strip()

    # 1. GigaChat Provider (When explicitly selected via LLM_PROVIDER=gigachat)
    if provider == "gigachat":
        gigachat_model = (os.getenv("GIGACHAT_MODEL") or getattr(settings, "gigachat_model", "GigaChat") or "GigaChat").strip()
        gigachat_base_url = (os.getenv("GIGACHAT_BASE_URL") or getattr(settings, "gigachat_base_url", "https://api.giga.chat/v1") or "https://api.giga.chat/v1").strip()
        scope = (os.getenv("GIGACHAT_SCOPE") or "GIGACHAT_API_PERS").strip()
        verify_ssl = getattr(settings, "gigachat_verify_ssl_certs", False)
        gigachat_auth_url = getattr(settings, "gigachat_auth_url", "https://ngw.devices.sberbank.ru:9443/api/v2/oauth")
        llm_timeout = getattr(settings, "llm_timeout", 45.0)
        llm_temp = getattr(settings, "llm_temperature", 0.0)

        try:
            auth_handler = GigaChatAuth(gigachat_creds, scope=scope, verify_ssl=verify_ssl, auth_url=gigachat_auth_url)
            initial_token = auth_handler.get_token()

            http_client = httpx.Client(verify=verify_ssl, auth=auth_handler)
            http_async_client = httpx.AsyncClient(verify=verify_ssl, auth=auth_handler)

            llm = ChatOpenAI(
                base_url=gigachat_base_url,
                api_key=initial_token,
                model=gigachat_model,
                temperature=llm_temp,
                request_timeout=llm_timeout,
                timeout=llm_timeout,
                max_retries=1,
                http_client=http_client,
                http_async_client=http_async_client
            )
            logger.info(f"Connected to GigaChat API ({gigachat_base_url}) with model {gigachat_model}")
            return llm, f"GigaChat ({gigachat_model})"
        except Exception as e:
            logger.error(f"GigaChat API connection failed: {e}")
            raise RuntimeError(f"GigaChat connection error: {e}")

    # 2. DeepSeek Cloud Provider (Default)
    deepseek_base_url = (os.getenv("DEEPSEEK_BASE_URL") or getattr(settings, "deepseek_base_url", "https://api.deepseek.com") or "https://api.deepseek.com").strip()
    if not deepseek_base_url.endswith("/v1") and not deepseek_base_url.endswith("/v1/"):
        deepseek_base_url = f"{deepseek_base_url.rstrip('/')}/v1"
    deepseek_model = (os.getenv("DEEPSEEK_MODEL") or getattr(settings, "deepseek_model", "deepseek-v4-flash") or "deepseek-v4-flash").strip()
    llm_timeout = getattr(settings, "llm_timeout", 45.0)
    llm_temp = getattr(settings, "llm_temperature", 0.0)

    llm = ChatOpenAI(
        base_url=deepseek_base_url,
        api_key=deepseek_key,
        model=deepseek_model,
        temperature=llm_temp,
        request_timeout=llm_timeout,
        timeout=llm_timeout,
        max_retries=1
    )
    logger.info(f"Connected to DeepSeek API ({deepseek_base_url}) with model {deepseek_model}")
    return llm, f"DeepSeek ({deepseek_model})"

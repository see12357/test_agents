"""
Unit tests for shared configuration loading and pipeline step mapping.
"""

from shared.config import load_config, YAMLConfig, PipelineStep


def test_load_config():
    settings, yaml_config = load_config()
    assert settings is not None
    assert yaml_config is not None
    assert isinstance(yaml_config.allowed_objects, list)
    assert len(yaml_config.allowed_objects) > 0


def test_pipeline_step_mapping():
    _, yaml_config = load_config()
    parser_step = yaml_config.get_pipeline_step("parser")
    assert parser_step is not None
    assert parser_step.subscribe_queue == "q.tasks.raw"
    assert parser_step.publish_queue == "q.tasks.parsed"

    rag_step = yaml_config.get_pipeline_step("rag")
    assert rag_step is not None
    assert rag_step.subscribe_queue == "q.tasks.parsed"
    assert rag_step.publish_queue == "q.tasks.ready_for_execution"

    executor_step = yaml_config.get_pipeline_step("executor")
    assert executor_step is not None
    assert executor_step.subscribe_queue == "q.tasks.ready_for_execution"


def test_nonexistent_pipeline_step():
    yaml_cfg = YAMLConfig()
    assert yaml_cfg.get_pipeline_step("unknown_agent") is None

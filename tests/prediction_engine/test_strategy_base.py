import pytest
from prediction_engine.strategies.base import Strategy, register, STRATEGY_REGISTRY
from schema.prediction import ScenarioConfig


class _NoopConfig(ScenarioConfig):
    pass


def test_registry_decorator_registers_class():
    # Use a one-off name to avoid collisions with real strategies.
    @register("test_noop_xyz")
    class NoopStrategy(Strategy):
        name = "test_noop_xyz"
        config_schema = _NoopConfig
        def predict(self, snapshot, scenario):
            raise NotImplementedError

    assert "test_noop_xyz" in STRATEGY_REGISTRY
    assert STRATEGY_REGISTRY["test_noop_xyz"] is NoopStrategy
    # Cleanup so reruns don't fail.
    del STRATEGY_REGISTRY["test_noop_xyz"]


def test_register_rejects_duplicate_name():
    @register("test_dup")
    class A(Strategy):
        name = "test_dup"
        config_schema = _NoopConfig
        def predict(self, snapshot, scenario): pass

    with pytest.raises(ValueError, match="already registered"):
        @register("test_dup")
        class B(Strategy):
            name = "test_dup"
            config_schema = _NoopConfig
            def predict(self, snapshot, scenario): pass

    del STRATEGY_REGISTRY["test_dup"]

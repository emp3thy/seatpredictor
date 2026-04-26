import pytest
from prediction_engine.strategies.base import Strategy, register, STRATEGY_REGISTRY
from schema.prediction import ScenarioConfig


class _NoopConfig(ScenarioConfig):
    pass


@pytest.fixture(autouse=True)
def _restore_registry():
    """Guarantee the registry is clean of test-only keys after every test in this
    module, even if a test fails mid-body. Without this, a leaked key would cause
    spurious failures on a same-process re-run (pytest --count=2, watch mode)."""
    keys_before = set(STRATEGY_REGISTRY.keys())
    yield
    extras = set(STRATEGY_REGISTRY.keys()) - keys_before
    for k in extras:
        del STRATEGY_REGISTRY[k]


def test_registry_decorator_registers_class():
    @register("test_noop_xyz")
    class NoopStrategy(Strategy):
        name = "test_noop_xyz"
        config_schema = _NoopConfig
        def predict(self, snapshot, scenario):
            raise NotImplementedError

    assert "test_noop_xyz" in STRATEGY_REGISTRY
    assert STRATEGY_REGISTRY["test_noop_xyz"] is NoopStrategy


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

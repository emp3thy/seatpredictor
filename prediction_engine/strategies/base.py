from abc import ABC, abstractmethod
from typing import Type

from prediction_engine.snapshot_loader import Snapshot
from schema.prediction import ScenarioConfig


class Strategy(ABC):
    name: str
    config_schema: Type[ScenarioConfig]

    @abstractmethod
    def predict(self, snapshot: Snapshot, scenario: ScenarioConfig):
        """Return a PredictionResult."""
        ...


STRATEGY_REGISTRY: dict[str, Type[Strategy]] = {}


def register(name: str):
    def decorator(cls: Type[Strategy]) -> Type[Strategy]:
        if name in STRATEGY_REGISTRY:
            raise ValueError(f"strategy {name!r} already registered")
        STRATEGY_REGISTRY[name] = cls
        return cls
    return decorator

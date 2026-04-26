# Importing the strategy modules triggers their @register decorators,
# populating STRATEGY_REGISTRY at import time. Adding a new strategy
# requires importing it here.
from prediction_engine.strategies import base  # noqa: F401
from prediction_engine.strategies import uniform_swing  # noqa: F401
from prediction_engine.strategies import reform_threat_consolidation  # noqa: F401

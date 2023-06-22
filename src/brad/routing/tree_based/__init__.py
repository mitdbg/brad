from brad.config.engine import Engine

# Used to assign a numerical index to each engine.
ORDERED_ENGINES = [Engine.Aurora, Engine.Redshift, Engine.Athena]

# Numerical labels to specific engines.
ENGINE_LABELS = {idx: engine for idx, engine in enumerate(ORDERED_ENGINES)}

from brad.config.engine import Engine

"""Used to assign a numerical index to each engine."""
ORDERED_ENGINES = [Engine.Aurora, Engine.Redshift, Engine.Athena]

"""Maps each engine to a numerical label."""
ENGINE_LABELS = {engine: idx for idx, engine in enumerate(ORDERED_ENGINES)}

import enum


class ExtractionStrategy(str, enum.Enum):
    SequenceTrigger = "sequence_trigger"

    @classmethod
    def from_str(cls, candidate: str) -> "ExtractionStrategy":
        if candidate == cls.SequenceTrigger:
            return cls.SequenceTrigger
        raise ValueError("Unsupported extraction strategy {}".format(candidate))

import enum


class TransitionState(enum.Enum):
    # No transition in progress.
    Stable = "stable"

    # Preparing to activate the next blueprint (i.e., transition in progress),
    # but we can still abort.
    TransitioningButAbortable = "transitioning_but_abortable"

    # Preparing to activate the next blueprint, but we are past the point of no
    # return (i.e., transition in progress but cannot abort).
    Transitioning = "transitioning"

    # Next blueprint is stable (we are already running on it), but we are
    # running some clean up tasks in the background.
    CleaningUp = "cleaning_up"

    @staticmethod
    def from_str(self, candidate: str) -> "TransitionState":
        if candidate == self.Stable.value:
            return self.Stable
        elif candidate == self.TransitioningButAbortable.value:
            return self.TransitioningButAbortable
        elif candidate == self.Transitioning.value:
            return self.Transitioning
        elif candidate == self.CleaningUp.value:
            return self.CleaningUp
        raise RuntimeError(f"Invalid blueprint transition state: {candidate}")

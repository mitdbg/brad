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
    def from_str(candidate: str) -> "TransitionState":
        if candidate == TransitionState.Stable.value:
            return TransitionState.Stable
        elif candidate == TransitionState.TransitioningButAbortable.value:
            return TransitionState.TransitioningButAbortable
        elif candidate == TransitionState.Transitioning.value:
            return TransitionState.Transitioning
        elif candidate == TransitionState.CleaningUp.value:
            return TransitionState.CleaningUp
        raise RuntimeError(f"Invalid blueprint transition state: {candidate}")

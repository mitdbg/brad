import enum


class TransitionState(enum.Enum):
    # No transition in progress.
    Stable = "stable"

    # Preparing to activate the next blueprint.
    Transitioning = "transitioning"

    # We activated the next blueprint but have not yet started cleaning up (this
    # means we are waiting for some front ends to complete transitioning to the
    # new blueprint).
    TransitionedPreCleanUp = "transitioned_pre_clean_up"

    # Next blueprint is stable (we are already running on it), but we are
    # running some clean up tasks in the background.
    CleaningUp = "cleaning_up"

    @staticmethod
    def from_str(candidate: str) -> "TransitionState":
        if candidate == TransitionState.Stable.value:
            return TransitionState.Stable
        elif candidate == TransitionState.Transitioning.value:
            return TransitionState.Transitioning
        elif candidate == TransitionState.TransitionedPreCleanUp.value:
            return TransitionState.TransitionedPreCleanUp
        elif candidate == TransitionState.CleaningUp.value:
            return TransitionState.CleaningUp
        raise RuntimeError(f"Invalid blueprint transition state: {candidate}")

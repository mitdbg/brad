import enum


class SystemEvent(enum.Enum):
    """
    A list of important system events. These are logged when they occur for
    later analysis (e.g., to understand what BRAD was doing).
    """

    StartUp = "start_up"
    ShutDown = "shut_down"

    TriggeredReplan = "triggered_replan"
    ManuallyTriggeredReplan = "manually_triggered_replan"

    # Proposed: Blueprint planner suggests a blueprint
    # Skipped: Daemon skips the proposed blueprint because it is too similar to
    #          the current blueprint
    # Accepted: Daemon accepts the blueprint and begins a transition
    NewBlueprintProposed = "new_blueprint_proposed"
    NewBlueprintAccepted = "new_blueprint_accepted"
    NewBlueprintSkipped = "new_blueprint_skipped"

    PreTransitionStarted = "pre_transition_started"
    PreTransitionCompleted = "pre_transition_completed"
    PostTransitionStarted = "post_transition_started"
    PostTransitionCompleted = "post_transition_completed"
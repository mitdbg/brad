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

    AuroraPrimaryFailover = "aurora_primary_failover"

    # If this event occurs, we must redo the experiment.
    WatchdogFired = "watchdog_fired"

    # Use this for long running experiments.
    ReachedExpectedState = "reached_expected_state"

    # Used when a service level objective is changed while BRAD is running (used
    # for experiments).
    ChangedSlos = "changed_slos"

    # Used to mark table movement progress.
    PreTableMovementStarted = "pre_table_movement_started"
    PreTableMovementCompleted = "pre_table_movement_completed"
    PostTableMovementStarted = "post_table_movement_started"
    PostTableMovementCompleted = "post_table_movement_completed"

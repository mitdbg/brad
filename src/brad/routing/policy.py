import enum


class RoutingPolicy(str, enum.Enum):
    """
    This is used to override the policy specified by the blueprint (usually for
    testing purposes).
    """

    Default = "default"
    AlwaysAthena = "always_athena"
    AlwaysAurora = "always_aurora"
    AlwaysRedshift = "always_redshift"
    RuleBased = "rule_based"
    ForestTablePresence = "df_table_presence"
    ForestTableSelectivity = "df_table_selectivity"

    @staticmethod
    def from_str(candidate: str) -> "RoutingPolicy":
        if candidate == RoutingPolicy.Default.value:
            return RoutingPolicy.Default
        elif candidate == RoutingPolicy.AlwaysAthena.value:
            return RoutingPolicy.AlwaysAthena
        elif candidate == RoutingPolicy.AlwaysAurora.value:
            return RoutingPolicy.AlwaysAurora
        elif candidate == RoutingPolicy.AlwaysRedshift.value:
            return RoutingPolicy.AlwaysRedshift
        elif candidate == RoutingPolicy.RuleBased.value:
            return RoutingPolicy.RuleBased
        elif candidate == RoutingPolicy.ForestTablePresence.value:
            return RoutingPolicy.ForestTablePresence
        elif candidate == RoutingPolicy.ForestTableSelectivity.value:
            return RoutingPolicy.ForestTableSelectivity
        else:
            raise ValueError("Unrecognized policy {}".format(candidate))

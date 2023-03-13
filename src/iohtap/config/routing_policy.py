import enum


class RoutingPolicy(str, enum.Enum):
    Default = "default"
    AlwaysAthena = "always_athena"
    AlwaysAurora = "always_aurora"
    AlwaysRedshift = "always_redshift"

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
        else:
            raise ValueError("Unrecognized DB type {}".format(candidate))

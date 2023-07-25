from brad.blueprint import Blueprint


class Filter:
    """
    Used to determine whether or not a proposed Blueprint candidate is
    considered "valid".
    """

    def is_valid(self, candidate: Blueprint) -> bool:
        raise NotImplementedError

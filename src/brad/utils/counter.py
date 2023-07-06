class Counter:
    def __init__(self, initial_value: int = 0) -> None:
        self._value = initial_value

    def bump(self, delta: int = 1) -> None:
        self._value += delta

    def value(self) -> int:
        return self._value

    def reset(self, value: int = 0) -> None:
        self._value = value

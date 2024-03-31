from typing import Any, List, Tuple


RowList = List[Tuple[Any, ...]]

# Note: pybind11 does not support the full generic std::any type
FixedRowList = List[Tuple[int]]

from typing import Dict, List, Tuple, Optional

from .query_template import QueryTemplate
from brad.blueprint import Blueprint
from brad.query_rep import QueryRep
from brad.config.engine import Engine
from brad.utils.table_sizer import TableSizer


class Workload:
    """
    A representation of the workload to be considered during the blueprint
    planning process.

    - List of analytical queries
    - List of transactions (sampled) and the sample frequency
    - Total dataset size
    - Table sizes

    The planner uses these values when comparing blueprints. The intention is
    that these values can also be _forecasted_.
    """

    def __init__(self, templates: List[QueryTemplate]) -> None:
        # NOTE: This will be removed
        self._templates = templates

        self._analytical_queries: List[QueryRep] = []
        self._transactional_queries: List[QueryRep] = []
        self._transaction_sample_fraction = 0.01
        self._dataset_size_mb = 0

        # The size of a table on an engine.
        self._table_sizes_mb: Dict[Tuple[str, Engine], int] = {}

    def templates(self) -> List[QueryTemplate]:
        return self._templates

    async def populate_table_sizes_using_blueprint(
        self, blueprint: Blueprint, table_sizer: TableSizer
    ) -> None:
        self._table_sizes_mb.clear()
        for table, locations in blueprint.tables_with_locations():
            for loc in locations:
                self._table_sizes_mb[(table, loc)] = await table_sizer.table_size_mb(
                    table, loc
                )

    def table_size_on_engine(self, table_name: str, location: Engine) -> Optional[int]:
        try:
            return self._table_sizes_mb[(table_name, location)]
        except KeyError:
            return None

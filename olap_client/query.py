
from collections import defaultdict
from enum import Enum
from typing import DefaultDict, Optional, Set, Tuple

from .exceptions import InvalidQueryError

class QueryFormat(str, Enum):
    """QueryFormat Enum

    Defines the format of the requested data.
    """
    CSV = "csv"
    JSON = "json"
    JSONRECORDS = "jsonrecords"
    XLS = "xls"

class Query:
    """Query class.

    Allows the user to build data requests and generate normalized URLs, to take
    advantage of server caching.
    """

    booleans: DefaultDict[str, bool] = defaultdict(None)
    captions: DefaultDict[str, str] = defaultdict(str)
    cuts: DefaultDict[str, set] = defaultdict(set)
    drilldowns: Set[str] = set()
    filters: DefaultDict[str, tuple] = defaultdict(tuple)
    format: QueryFormat = QueryFormat.JSONRECORDS
    locale: Optional[str] = None
    measures: Set[str] = set()
    pagination: Tuple[int, int] = (None, None)
    properties: DefaultDict[str, set] = defaultdict(set)
    sorting: Tuple[str, str] = (None, None)
    time: Optional[str] = None
    # Will be replaced by the calculations pattern from olap-client beta
    growth: Tuple[str, str] = (None, None)
    rca: Tuple[str, str, str] = (None, None, None)
    topk: Tuple[str, str, int, str] = (None, None, None, None)
    # rate
    # top_where = defaultdict(list)

    def __init__(self, cube_name: str):
        self.cube = cube_name

    def raise_on_invalid(self):
        """Raises :class: `InvalidQueryError` if the current status of the query
        object would surely return an error from the server.
        """
        if not self.cube or len(self.drilldowns) == 0 or len(self.measures) == 0:
            raise InvalidQueryError()

    def set_boolean(self, prop: str, value: bool):
        if prop in self.booleans:
            self.booleans[prop] = value
        return self

    def set_caption(self, level_name: str, property_name: str):
        self.captions[level_name] = property_name
        return self

    def add_cut(self, level_name: str, *members):
        self.cuts[level_name].update(members)
        return self

    def add_drilldown(self, *level_names):
        self.drilldowns.update(level_names)
        return self

    def set_filter(
        self,
        calc: str,
        condition1: Tuple[str, int],
        joint=None,
        condition2: Tuple[str, int] = None,
    ):
        self.filters[calc] = condition1 + (joint,) + condition2 if joint else condition1
        return self

    def set_format(self, query_format: QueryFormat):
        self.format = query_format
        return self
        
    def set_growth(self, level_name: str, calc: str):
        self.growth = (level_name, calc)
        return self

    def add_measure(self, *measure_names):
        self.measures.update(measure_names)
        return self

    def set_pagination(self, page: int, offset: int = None):
        self.pagination = (page, offset)
        return self

    def add_property(self, level_name: str, *property_names):
        self.properties[level_name].update(property_names)
        return self

    def set_rca(self):
        return self

    def set_sorting(self, sort_key, order="desc"):
        self.sorting = (sort_key, order)
        return self

    def set_time(self):
        return self

    def set_topk(self):
        return self

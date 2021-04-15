from collections import defaultdict
from typing import DefaultDict, Dict, Optional, Set, Tuple
from urllib import parse

import http3

from .exceptions import InvalidQueryError


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
    extension: str = "jsonrecords"
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


class Server:
    """Base class for OLAP servers to query.

    Must be subclassed implementing the build_query_url function, to convert a
    Query object into a query URL.
    """

    def __init__(self, base_url: str):
        self.base_url = base_url

    def build_query_url(self, query: Query):
        """Converts the Query object into an URL specific for the kind of server.

        Needs to be implemented by each server subclass.
        """
        raise NotImplementedError

    async def fetch_all_cubes(self):
        """Retrieves the schema information for all cubes in the server.

        Needs to be implemented by each server subclass.
        """
        raise NotImplementedError

    async def fetch_cube(self, cube_name: str):
        """Retrieves the schema information for a cube.

        Needs to be implemented by each server subclass.
        """
        raise NotImplementedError

    async def fetch_members(self, cube_name: str, level_name: str, ext: str):
        """Retrieves the list of members for a level in a cube.

        Needs to be implemented by each server subclass.
        """
        raise NotImplementedError

    async def query(self, query: Query):
        """Makes a request for the data specified in the Query object."""
        client = http3.AsyncClient()
        url = parse.urljoin(self.base_url, self.build_query_url(query))
        response = await client.get(url)
        response.raise_for_status()
        return response.json() if "json" in query.extension else response.content

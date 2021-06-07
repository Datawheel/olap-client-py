"""Main Query class.

The Query class holds the parameters that will later be used to execute a data
request to the server.
The final user will interact only with instances of these classes or subclasses.
"""

import dataclasses
from collections import defaultdict
from enum import Enum
from typing import (TYPE_CHECKING, Any, DefaultDict, Dict, List, Optional, Set,
                    Tuple, Union)

from typing_extensions import Literal

from .exceptions import InvalidQueryError

if TYPE_CHECKING:
    from .models import Cube, Level, Measure, Property


class DataFormat(str, Enum):
    """DataFormat Enum.

    Defines the response format of the requested data.
    """
    CSV = "csv"
    JSON = "json"
    JSONARRAYS = "jsonarrays"
    JSONRECORDS = "jsonrecords"
    XLS = "xls"


class Comparison(str, Enum):
    """Comparison Enum.

    Defines the available value comparison operations.
    """
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    EQ = "eq"
    NEQ = "neq"


@dataclasses.dataclass(eq=False, order=False)
class Calculation:
    """Calculation dataclass.

    Contains the parameters that define a request for a calculation.
    """
    kind: str
    params: Dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def name(self):
        """Adds compatibility with Measure#name for where it's an option to replace it."""
        return self.kind


@dataclasses.dataclass(eq=False, order=False)
class Cut:
    """Cut dataclass.

    Contains the parameters to define a cut for the data retrieved in the query.
    """
    level: "Level" = dataclasses.field(init=False)
    members: Set[str] = dataclasses.field(default_factory=set)
    is_exclusive: bool = False
    is_for_match: bool = False


@dataclasses.dataclass(eq=False, order=False)
class Drilldown:
    """Drilldown dataclass.

    Contains the parameters associated to a slicing operation on the cube data.
    """
    level: "Level" = dataclasses.field(init=False)
    properties: Set["Property"] = dataclasses.field(default_factory=set)
    caption: Optional["Property"] = None


@dataclasses.dataclass(eq=False, order=False)
class Filter:
    """Filter dataclass.

    Contains the parameters needed to filter the data points returned by the
    query operation from the server.
    """
    value: Union[str, "Measure"] = dataclasses.field(init=False)
    constraint1: Tuple[Comparison, int] = dataclasses.field(init=False)
    joint: Optional[str] = None
    constraint2: Optional[Tuple[Comparison, int]] = None


class Query:
    """Query class.

    Allows the user to build data requests and generate normalized URLs, to take
    advantage of server caching.
    """

    calculations: List[Calculation]
    cube: "Cube"
    cuts: DefaultDict[str, Cut]
    drilldowns: DefaultDict[str, Drilldown]
    extension: DataFormat = DataFormat.JSONRECORDS
    filters: DefaultDict[str, Filter]
    locale: Optional[str]
    measures: Set["Measure"]
    options: DefaultDict[str, bool]
    pagination: Tuple[int, int]
    sorting: Tuple[str, Literal["asc", "desc"]]
    time: Tuple[str, str]
    # top_where = defaultdict(list)

    def __init__(self, cube: "Cube"):
        self.calculations = list()
        self.cube = cube
        self.cuts = defaultdict(Cut)
        self.drilldowns = defaultdict(Drilldown)
        self.filters = defaultdict(Filter)
        self.locale = None
        self.measures = set()
        self.options = defaultdict(bool)
        self.pagination = (0, 0)
        self.sorting = (None, "desc")
        self.time = (None, None)

    def raise_on_invalid(self):
        """Raises :class: `InvalidQueryError` if the current status of the query
        object would surely return an error from the server.
        """
        from .models import Cube

        if not isinstance(self.cube, Cube):
            raise InvalidQueryError("A target Cube has not been set for this query")
        if len(self.drilldowns) == 0:
            raise InvalidQueryError("There are no drilldowns in this query")
        if len(self.measures) == 0:
            raise InvalidQueryError("There are no measures in this query")

    def add_calculation(self, calc: Union[str, Calculation], **kwargs) -> "Query":
        """Adds a calculation request to the query.

        The kind is the keyword that determines how each Server instance will
        proceed to get the calculation.
        Note there are differences on the capabilities each type of server
        provide, calculations are not always guaranteed to be returned.
        """
        if not isinstance(calc, Calculation):
            calc = Calculation(kind=calc, params=kwargs)
        self.calculations.append(calc)
        return self

    def add_measure(self, measure_name: str)  -> "Query":
        """Defines a measure in the Query.
        """
        measure = self.cube.get_measure(measure_name)
        self.measures.add(measure)
        return self

    def set_cut(self, level_name: str, members: List[str], is_exclusive: bool = None, is_for_match: bool = None)  -> "Query":
        """Defines a cut in the Query.

        Calling this function multiple times using the same level will overwrite
        the other parameters.
        Only one cut per dimension can be applied; trying to apply a cut using
        another level from the same dimension as another cut already applied
        will result in replacing the previous cut.
        """
        level = self.cube.get_level(level_name)
        cut = self.cuts[level.dimension]
        cut.level = level

        if is_exclusive is not None:
            cut.is_exclusive = is_exclusive
        if is_for_match is not None:
            cut.is_for_match = is_for_match
        if len(members) > 0:
            cut.members.update(members)
        return self

    def set_drilldown(self, level_name: str)  -> "Query":
        """Defines a drilldown in the query.
        """
        level = self.cube.get_level(level_name)
        drill = self.drilldowns[level.dimension]
        drill.level = level
        return self

    def set_caption(self, property_name: str)  -> "Query":
        """Defines a caption for its associated level.
        """
        propty = self.cube.get_property(property_name)
        drill = self.drilldowns[propty.dimension]
        drill.caption = propty
        return self

    def set_property(self, property_name: str)  -> "Query":
        """Defines a drilldown property in the query.
        """
        propty = self.cube.get_property(property_name)
        drill = self.drilldowns[propty.dimension]
        drill.properties.add(propty)
        return self

    def set_extension(self, extension: DataFormat)  -> "Query":
        """Defines the format the data will be returned as.
        """
        self.extension = extension
        return self

    def set_filter(
        self,
        value_ref: str,
        condition1: Tuple[str, int],
        joint=None,
        condition2: Tuple[str, int] = None,
    ) -> "Query":
        """Defines a measure filter in the query.
        """
        measure = next((item for item in self.cube.measures if item.name == value_ref), None)
        value_ref = measure.name if measure is not None else value_ref

        filtr = self.filters[value_ref]
        filtr.value = measure or value_ref
        filtr.constraint1 = condition1
        filtr.joint = joint
        filtr.constraint2 = condition2
        return self

    def set_locale(self, locale: str)  -> "Query":
        """Defines the language of the response data for the query.
        """
        self.locale = locale
        return self

    def set_option(self, prop: str, value: bool)  -> "Query":
        """Defines a boolean option for the query.

        The validity of each option is verified by the server. Invalid options
        are ignored silently.
        """
        self.options[prop] = bool(value)
        return self

    def set_pagination(self, page: int, offset: int = 0)  -> "Query":
        """Defines the pagination for the resulting data of the query.
        """
        self.pagination = (page, offset)
        return self

    def set_sorting(self, sort_key: str, order="desc")  -> "Query":
        """Defines the sorting for the resulting data of the query.
        """
        measure = self.cube.get_measure(sort_key)
        self.sorting = (measure.name, order)
        return self

    def set_time(self, precision: str, value: str)  -> "Query":
        """Defines the time restriction for the query.
        """
        new_time = (precision, value)
        self.time = new_time if None not in new_time else (None, None)
        return self

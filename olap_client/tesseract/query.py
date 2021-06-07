"""TesseractQuery subclass.

Contains specific modifications to work with a Tesseract OLAP server.
It should be generated through a TesseractCube instance instead of directly.
"""

import dataclasses
from collections import defaultdict
from typing import TYPE_CHECKING, DefaultDict, Optional, Set, Union
from urllib import parse

from ..exceptions import InvalidQueryError
from ..models import Level, Measure
from ..query import Calculation, Cut, Drilldown, Filter, Query
from .enum import TesseractDataFormat, TesseractEndpointType

if TYPE_CHECKING:
    from .schema import (TesseractCube, TesseractLevel, TesseractMeasure,
                         TesseractProperty)


class GrowthCalculation(Calculation):
    """Defines a Growth calculation on the server."""

    def __init__(self, period: Level, value: Measure):
        """Overrides the init definition for :class:`Calculation` class to fit Growth parameters."""
        super().__init__(kind="growth", params={
            "period": period,
            "value": value,
        })


class RcaCalculation(Calculation):
    """Defines a RCA calculation on the server."""

    def __init__(self, location: Level, category: Level, value: Measure):
        """Overrides the init definition for :class:`Calculation` class to fit Rca parameters."""
        super().__init__(kind="rca", params={
            "location": location,
            "category": category,
            "value": value,
        })


class TopkCalculation(Calculation):
    """Defines a TopK calculation on the server."""

    def __init__(self, amount: int, category: Level, value: Union[str, Measure], descendent: bool = True):
        """Overrides the init definition for :class:`Calculation` class to fit Topk parameters."""
        super().__init__(kind="topk", params={
            "amount": amount,
            "category": category,
            "value": value,
            "order": "asc" if not descendent else "desc",
        })


class TesseractCut(Cut):
    """Cut dataclass for :class:`TesseractQuery` instances."""
    level: "TesseractLevel"


class TesseractDrilldown(Drilldown):
    """Drilldown dataclass for :class:`TesseractQuery` instances."""
    level: "TesseractLevel"
    properties: Set["TesseractProperty"] = dataclasses.field(default_factory=set)
    caption: Optional["TesseractProperty"] = None


class TesseractFilter(Filter):
    """Filter dataclass for :class:`TesseractQuery` instances."""
    value: Union[str, "TesseractMeasure"]

    @property
    def value_name(self):
        """Depending on the value type, returns the measure name, or the calculation kind."""
        return self.value if isinstance(self.value, str) else self.value.name

    def serialize(self):
        """Generates a string representation of the current Filter."""
        filtr = "{0.value_name}.{0.constraint1[0]}.{0.constraint1[1]:d}".format(self)
        if self.joint is not None and self.constraint2[0] is not None:
            filtr = "{filtr}.{0.joint}.{0.constraint2[0]}.{0.constraint2[1]:d}".format(self, filtr=filtr)
        return filtr


class TesseractQuery(Query):
    """TesseractQuery class.

    :class:`Query` subclass, customized to work with Tesseract OLAP elements.
    """

    cube: "TesseractCube"
    cuts: DefaultDict[str, TesseractCut]
    drilldowns: DefaultDict[str, TesseractDrilldown]
    extension: str = TesseractDataFormat.JSONRECORDS
    filters: DefaultDict[str, TesseractFilter]
    measures: Set["TesseractMeasure"]

    def __init__(self, cube: "TesseractCube"):
        super().__init__(cube)

        self.cuts = defaultdict(TesseractCut)
        self.drilldowns = defaultdict(TesseractDrilldown)
        self.filters = defaultdict(TesseractFilter)

    def raise_on_invalid(self):
        """Raises :class: `InvalidQueryError` if the current status of the query
        object would surely return an error from the server.
        """

        super().raise_on_invalid()

        for filtr in self.filters.values():
            if not isinstance(filtr.value, str) and filtr.value not in self.measures:
                raise InvalidQueryError("Measure \"%s\" must be in measures to use in a filter.")


    def get_url(self, endpoint: TesseractEndpointType = TesseractEndpointType.LOGICLAYER):
        """Converts the parameters in the query to its URL for the specified endpoint."""
        if endpoint == TesseractEndpointType.LOGICLAYER:
            return build_logiclayer_url(self)

        # For the time being, efforts will be focused on the logiclayer endpoint
        raise KeyError("Aggregate endpoint is not yet fully supported")


def build_aggregate_url(query: "TesseractQuery") -> str:
    """Transforms a query instance into a tesseract-olap aggregate URL."""
    # For the time being, efforts will be focused on the logiclayer endpoint
    raise NotImplementedError

    # if len([measure for measure in query.measures if measure != ""]) == 0:
    #     raise InvalidQueryError()
    # if len([drill for drill in query.drilldowns if drill != ""]) == 0:
    #     raise InvalidQueryError()

    # transform_limit = lambda x: ("{0}.{1}" if x[1] else "{0}").format(*x)
    #                             if x[0] is not None or x[1] is not None else None

    # transform_sort = lambda x: "{0}.{1}".format(*x) if x[0] is not None else None

    # all_params = {
    #     "captions[]": [
    #         join_name(level, prop)
    #         for level, prop in query.captions.items()
    #     ],
    #     "cuts[]": [
    #         level + "." + ",".join(str(m) for m in members)
    #         for level, members in query.cuts.items()
    #     ],
    #     "debug": query.options.get("debug"),
    #     "drilldowns[]": query.drilldowns,
    #     "exclude_default_members": query.options.get("exclude_default_members"),
    #     "filters[]": [
    #         calc + "." + ".".join(str(c) for c in conditions)
    #         for calc, conditions in query.filters.items()
    #     ],
    #     "growth": "",
    #     "limit": transform_limit(query.pagination),
    #     "measures[]": query.measures,
    #     "parents": query.options.get("parents"),
    #     "properties[]": [
    #         f"{level}.{prop}"
    #         for level, props in query.properties.items()
    #         for prop in props
    #     ],
    #     # "rate": "",
    #     "sort": transform_sort(query.sorting),
    #     "sparse": query.options.get("sparse"),
    #     # "top_where": "",
    #     "top": "",
    # }

    # params = {k: v for k, v in all_params.items() if is_valid_value(v)}
    # search_params = parse.urlencode(params, True)
    # return f"cubes/{query.cube}/aggregate.{query.format}?{search_params}"


def build_logiclayer_url(query: "TesseractQuery") -> str:
    """Transforms a query instance into a tesseract-olap logiclayer URL."""

    params = {
        "cube": query.cube.name,
        "measures": set(item.name for item in query.measures),
        "drilldowns": [],
        "properties": [],
        "filters": [],
        "exclude": [],
    }

    for dril in query.drilldowns.values():
        params["drilldowns"].append(get_unique_name(dril.level))
        params["properties"].extend(get_unique_name(prop) for prop in dril.properties)

    if len(query.filters) > 0:
        # all_params["measures"].update(item.value_name
        #                               for item in query.filters.values()
        #                               if not isinstance(item.value, str))
        params["filters"] = [item.serialize() for item in query.filters.values()]

    if query.locale is not None:
        params["locale"] = query.locale

    for cut in query.cuts.values():
        level_name = get_unique_name(cut.level)
        params[level_name] = ",".join(cut.members)
        if cut.is_exclusive:
            params["exclude"].append(level_name)

    if query.pagination[0] > 0:
        template = "{:d},{:d}" if query.pagination[1] > 0 else "{:d}"
        params["limit"] = template.format(*query.pagination)

    if query.sorting[0] is not None:
        params["sort"] = "{}.{}".format(*query.sorting)

    if all((item is not None for item in query.time)):
        params["time"] = "{}.{}".join(*query.time),

    for calc in query.calculations:
        if calc.kind == "growth":
            params["growth"] = "{period},{value}".format(
                period=get_unique_name(calc.params["period"]),
                value=calc.params["value"].name,
            )

        elif calc.kind == "rca":
            params["rca"] = "{location},{category},{value}".format(
                location=get_unique_name(calc.params["location"]),
                category=get_unique_name(calc.params["category"]),
                value=calc.params["value"].name,
            )

        elif calc.kind == "topk":
            params["top"] = "{amount:d},{category},{value},{order}".format(
                amount=calc.params["amount"],
                category=get_unique_name(calc.params["category"]),
                value=calc.params["value"].name,
                order=calc.params["order"],
            )

    if query.options.get("debug") is True:
        params["debug"] = "true"

    if query.options.get("exclude_default_members") is True:
        params["exclude_default_members"] = "true"

    if query.options.get("parents") is True:
        params["parents"] = "true"

    if query.options.get("sparse") is True:
        params["sparse"] = "true"

    params = {key: ",".join(sorted(value)) if isinstance(value, (list, set)) else value
              for key, value in params.items()
              if is_valid_value(value)}
    return "data.{ext}?{search}".format(ext=query.extension, search=parse.urlencode(params))


def get_unique_name(item: Union["TesseractLevel", "TesseractProperty"]) -> str:
    """Returns the unique name for a item, or defaults to its name if it doesn't exist."""
    return item.unique_name or item.name


def join_name(*parts) -> str:
    """Builds a Tesseract OLAP full name according to naming specifications.

    :ref:`https://github.com/tesseract-olap/tesseract/tree/master/tesseract-server/#naming`
    """

    return ".".join(
        (f"[{part}]" for part in parts)
        if next(("." in token for token in parts), None) is not None
        else parts
    )


def is_valid_value(value) -> bool:
    """Determines if `value` is worth serializing as a parameter for the URL."""
    if isinstance(value, (list, set, dict)):
        return len(value) > 0
    elif isinstance(value, str):
        return value != ""
    return value is not None

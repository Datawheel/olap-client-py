from typing import List
from urllib import parse

import http3

from ..cube import (Cube, Dimension, Hierarchy, Level, Measure, Member,
                    NamedSet, Property)
from ..exceptions import InvalidQueryError
from ..server import Query, Server


class TesseractServer(Server):
    """Class for tesseract server requests.

    By default generates URLs using the special LogicLayer endpoint.
    """

    endpoint: str = "logiclayer"

    def build_query_url(self, query: Query):
        """Converts the Query object into an URL for Tesseract OLAP."""
        if self.endpoint == "logiclayer":
            return TesseractServer.build_logiclayer_url(query)
        elif self.endpoint == "aggregate":
            return TesseractServer.build_aggregate_url(query)
        else:
            raise KeyError()

    async def fetch_all_cubes(self):
        client = http3.AsyncClient()
        url = parse.urljoin(self.base_url, "cubes")
        request = await client.get(url)
        request.raise_for_status()
        schema = request.json()
        return list(cube_from_tesseract(cube) for cube in schema["cubes"])

    async def fetch_cube(self, cube_name: str):
        client = http3.AsyncClient()
        url = parse.urljoin(self.base_url, "/".join(["cubes", cube_name]))
        request = await client.get(url)
        request.raise_for_status()
        raw_cube = request.json()
        return cube_from_tesseract(raw_cube)

    async def fetch_members(self, cube_name: str, level_name: str, ext = "jsonrecords"):
        """Retrieves the list of members for a level in a cube."""
        client = http3.AsyncClient()
        url = parse.urljoin(self.base_url, f"members.{ext}")
        search_params = {"cube": cube_name, "level": level_name}
        request = await client.get(url, params=search_params)
        request.raise_for_status()
        return request.json() if "json" in ext else request.content

    @staticmethod
    def build_aggregate_url(query: Query):
        """Transforms a query instance into a tesseract-olap aggregate URL."""

        if len(measure for measure in query.measures if measure != "") == 0:
            raise InvalidQueryError()
        if len(drill for drill in query.drilldowns if drill != "") == 0:
            raise InvalidQueryError()

        transform_limit = (
            lambda x: ("{0}.{1}" if x[1] else "{0}").format(*x)
            if x[0] is not None or x[1] is not None
            else None
        )
        transform_sort = lambda x: "{0}.{1}".format(*x) if x[0] is not None else None

        all_params = {
            "captions[]": [
                join_name(level, prop)
                for level, prop in query.captions.items()
            ],
            "cuts[]": [
                level + "." + ",".join(str(m) for m in members)
                for level, members in query.cuts.items()
            ],
            "debug": query.booleans.get("debug"),
            "drilldowns[]": query.drilldowns,
            "exclude_default_members": query.booleans.get("exclude_default_members"),
            "filters[]": [
                calc + "." + ".".join(str(c) for c in conditions)
                for calc, conditions in query.filters.items()
            ],
            "growth": "",
            "limit": transform_limit(query.pagination),
            "measures[]": query.measures,
            "parents": query.booleans.get("parents"),
            "properties[]": [
                f"{level}.{prop}"
                for level, props in query.properties.items()
                for prop in props
            ],
            # "rate": "",
            "sort": transform_sort(query.sorting),
            "sparse": query.booleans.get("sparse"),
            # "top_where": "",
            "top": "",
        }

        params = {k: v for k, v in all_params.items() if is_valid_value(v)}
        search_params = parse.urlencode(params, True)
        return f"{query.cube}/aggregate.{query.format}?{search_params}"

    @staticmethod
    def build_logiclayer_url(query: Query):
        """Transforms a query instance into a tesseract-olap logiclayer URL."""

        all_params = {
            "cube": query.cube,
            "debug": query.booleans.get("debug"),
            "drilldowns": ",".join(query.drilldowns),
            "exclude_default_members": query.booleans.get("exclude_default_members"),
            "exclude": "",
            "filters": "",
            "growth": "",
            "limit": "",
            "locale": "",
            "measures": ",".join(query.measures),
            "parents": query.booleans.get("parents"),
            "properties": "",
            "rate": "",
            "rca": "",
            "sort": "",
            "sparse": query.booleans.get("sparse"),
            "time": "",
            "top_where": "",
            "top": "",
        }

        for level, members in query.cuts.items():
            all_params[level] = ",".join(members)

        params = {k: v for k, v in all_params.items() if is_valid_value(v)}
        return f"data.{query.format}?{parse.urlencode(params)}"


def cube_from_tesseract(cube):
    return Cube(
        annotations=cube.get("annotations", {}),
        dimensions=list(
            Dimension(
                annotations=dim.get("annotations", {}),
                default_hierarchy=dim["default_hierarchy"],
                dimension_type=dim["type"],
                full_name=join_name(dim["name"]),
                hierarchies=list(
                    Hierarchy(
                        annotations=hie.get("annotations", {}),
                        dimension=dim["name"],
                        full_name=join_name(dim["name"], hie["name"]),
                        levels=list(
                            Level(
                                annotations=lvl.get("annotations", {}),
                                caption=lvl["name"],
                                depth=depth,
                                dimension=dim["name"],
                                full_name=join_name(dim["name"], hie["name"], lvl["name"]),
                                hierarchy=hie["name"],
                                name=lvl["name"],
                                properties=list(
                                    Property(
                                        annotations=lvl.get("annotations", {}),
                                        dimension=dim["name"],
                                        hierarchy=hie["name"],
                                        level=lvl["name"],
                                        name=prop["name"]
                                    )
                                    for prop in lvl["properties"])
                                    if lvl["properties"] is not None
                                    else [],
                                unique_name=lvl["unique_name"],
                            )
                            for depth, lvl in enumerate(hie["levels"], 1)
                        ),
                        name=hie["name"],
                    )
                    for hie in dim["hierarchies"]
                ),
                name=dim["name"],
            )
            for dim in cube["dimensions"]
        ),
        measures=list(
            Measure(
                aggregator_type=mea["aggregator"]["name"],
                annotations=mea.get("annotations", {}),
                caption=mea["name"],
                name=mea["name"],
            )
            for mea in cube["measures"]
        ),
        name=cube["name"],
        namedsets=list(
            NamedSet()
            for nset in cube.get("named_sets", [])
        ),
    )


def members_from_tesseract(members: List[dict], locale: str):
    return [
        Member(
            caption=mem.get(f"{locale} Label") or mem.get("Label") or mem["ID"],
            key=mem["ID"],
            name=mem.get("Label") or mem["ID"],
        )
        for mem in members
    ]


def join_name(*parts):
    """Builds a Tesseract OLAP full name according to specifications.

    See [this page](https://github.com/tesseract-olap/tesseract/tree/master/tesseract-server/#naming) for more info.
    """
    return ".".join(
        (f"[{part}]" for part in parts)
        if next(("." in token for token in parts), None) is not None
        else parts
    )


def is_valid_value(value):
    if isinstance(value, (list, set)):
        return len(value) > 0
    elif isinstance(value, str):
        return value != ""
    return value is not None

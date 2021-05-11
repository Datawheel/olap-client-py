"""Module that implements the Tesseract variant of the Server base class"""

from typing import List
from urllib import parse

import httpx

from ..query import DataFormat
from ..server import Query, Server
from .schema import (TesseractCube, TesseractDataFormat, TesseractEndpointType,
                     TesseractSchema)


class TesseractServer(Server):
    """Class for tesseract server requests.

    By default generates URLs using the special LogicLayer endpoint.
    """

    endpoint: TesseractEndpointType = TesseractEndpointType.LOGICLAYER

    def build_query_url(self, query: Query):
        """Converts the Query object into an URL for Tesseract OLAP."""
        if self.endpoint == TesseractEndpointType.LOGICLAYER:
            return TesseractServer.build_logiclayer_url(query)
        # For the time being, efforts will be focused on the logiclayer endpoint
        # elif self.endpoint == TesseractEndpointType.AGGREGATE:
        #     return TesseractServer.build_aggregate_url(query)
        raise KeyError("Endpoint \"{}\" is not available on Tesseract Servers" % self.endpoint)

    async def fetch_all_cubes(self):
        """Retrieves the list of available cubes from the server."""
        url = parse.urljoin(self.base_url, "cubes")
        async with httpx.AsyncClient() as client:
            request = await client.get(url)
            request.raise_for_status()
            schema = request.json()
        return TesseractSchema.parse_obj(schema).cubes

    async def fetch_cube(self, cube_name: str):
        """Retrieves the information from a specific cube from the server."""
        url = parse.urljoin(self.base_url, "cubes/{}" % cube_name)
        async with httpx.AsyncClient() as client:
            request = await client.get(url)
            request.raise_for_status()
            raw_cube = request.json()
        return TesseractCube.parse_obj(raw_cube)

    async def fetch_members(self, cube_name: str, level_name: str, ext = DataFormat.JSONRECORDS):
        """Retrieves the list of members for a level in a cube."""
        if ext not in TesseractDataFormat:
            raise KeyError("Format \"{}\" is not available on Tesseract Servers" % ext)

        url = parse.urljoin(self.base_url, "members.{}" % ext)
        search_params = {"cube": cube_name, "level": level_name}
        async with httpx.AsyncClient() as client:
            request = await client.get(url, params=search_params)
            request.raise_for_status()
        return request.json() if "json" in ext else request.content

    def setEndpoint(self, endpoint_type: TesseractEndpointType):
        if endpoint_type == TesseractEndpointType.AGGREGATE:
            raise NotImplementedError("Aggregate endpoint is not yet fully supported")
        self.endpoint = endpoint_type
        return self

    @staticmethod
    def build_aggregate_url(query: Query):
        """Transforms a query instance into a tesseract-olap aggregate URL."""
        # For the time being, efforts will be focused on the logiclayer endpoint
        raise NotImplementedError

        # if len([measure for measure in query.measures if measure != ""]) == 0:
        #     raise InvalidQueryError()
        # if len([drill for drill in query.drilldowns if drill != ""]) == 0:
        #     raise InvalidQueryError()

        # all_params = {
        #     "captions[]": [
        #         join_name(level, prop)
        #         for level, prop in query.captions.items()
        #     ],
        #     "cuts[]": [
        #         level + "." + ",".join(str(m) for m in members)
        #         for level, members in query.cuts.items()
        #     ],
        #     "debug": query.booleans.get("debug"),
        #     "drilldowns[]": query.drilldowns,
        #     "exclude_default_members": query.booleans.get("exclude_default_members"),
        #     "filters[]": [
        #         calc + "." + ".".join(str(c) for c in conditions)
        #         for calc, conditions in query.filters.items()
        #     ],
        #     "growth": "",
        #     "limit": transform_limit(query.pagination),
        #     "measures[]": query.measures,
        #     "parents": query.booleans.get("parents"),
        #     "properties[]": [
        #         f"{level}.{prop}"
        #         for level, props in query.properties.items()
        #         for prop in props
        #     ],
        #     # "rate": "",
        #     "sort": transform_sort(query.sorting),
        #     "sparse": query.booleans.get("sparse"),
        #     # "top_where": "",
        #     "top": "",
        # }

        # params = {k: v for k, v in all_params.items() if is_valid_value(v)}
        # search_params = parse.urlencode(params, True)
        # return f"cubes/{query.cube}/aggregate.{query.format}?{search_params}"

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

def transform_limit(x):
    return ("{0}.{1}" if x[1] else "{0}").format(*x)
            if x[0] is not None or x[1] is not None
            else None

def transform_sort(x):
    return "{0}.{1}".format(*x) if x[0] is not None else None

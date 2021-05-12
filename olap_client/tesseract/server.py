"""TesseractServer class.

A class extending the Server class, adapted for use with Tesseract OLAP servers.
"""

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
        raise KeyError("Endpoint \"%s\" is not available on Tesseract servers" % self.endpoint)

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
        url = parse.urljoin(self.base_url, "cubes/%s" % cube_name)

        async with httpx.AsyncClient() as client:
            request = await client.get(url)
            request.raise_for_status()
            raw_cube = request.json()
        return TesseractCube.parse_obj(raw_cube)

    async def fetch_members(self, cube_name: str, level_name: str, ext = DataFormat.JSONRECORDS):
        """Retrieves the list of members for a level in a cube."""
        if ext not in TesseractDataFormat:
            raise KeyError("Format \"%s\" is not available on Tesseract Servers" % ext)

        url = parse.urljoin(self.base_url, "members.%s" % ext)
        search_params = {"cube": cube_name, "level": level_name}
        async with httpx.AsyncClient() as client:
            request = await client.get(url, params=search_params)
            request.raise_for_status()
        return request.json() if "json" in ext else request.content

    def set_endpoint(self, endpoint_type: TesseractEndpointType):
        """Sets the endpoint this Server instance will use to fetch data."""
        if endpoint_type == TesseractEndpointType.AGGREGATE:
            raise NotImplementedError("Aggregate endpoint is not yet fully supported")
        self.endpoint = endpoint_type
        return self

    @staticmethod
    def build_aggregate_url(query: Query) -> str:
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
    def build_logiclayer_url(query: Query) -> str:
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
        return "data.{ext}?{search}".format(ext=query.format, search=parse.urlencode(params))


def join_name(*parts) -> str:
    """Builds a Tesseract OLAP full name according to [specifications].

    [specifications]: https://github.com/tesseract-olap/tesseract/tree/master/tesseract-server/#naming
    """
    return ".".join(
        (f"[{part}]" for part in parts)
        if next(("." in token for token in parts), None) is not None
        else parts
    )


def is_valid_value(value) -> bool:
    """Determines if `value` is worth serializing as a parameter for the URL."""
    if isinstance(value, (list, set)):
        return len(value) > 0
    elif isinstance(value, str):
        return value != ""
    return value is not None

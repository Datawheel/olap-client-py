"""TesseractServer class.

A class extending the Server class, adapted for use with Tesseract OLAP servers.
"""

from typing import TYPE_CHECKING, Optional, Union
from urllib import parse

import httpx

from ..server import Server
from .enum import TesseractDataFormat, TesseractEndpointType
from .schema import TesseractCube, TesseractSchema

if TYPE_CHECKING:
    from .query import TesseractQuery


class TesseractServer(Server):
    """Class for tesseract server requests.

    By default generates URLs using the special LogicLayer endpoint.
    """

    endpoint: TesseractEndpointType = TesseractEndpointType.LOGICLAYER

    def build_query_url(self, query: "TesseractQuery"):
        """Converts the Query object into an URL for Tesseract OLAP."""
        return query.get_url(self.endpoint)

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

    async def fetch_members(self,
                            cube_name: str,
                            level_name: str,
                            extension: "TesseractDataFormat" = TesseractDataFormat.JSONRECORDS,
                            locale: Optional[str] = None,
                            **kwargs) -> Union[str, dict]:
        """Retrieves the list of members for a level in a cube.

        Arguments:
            cube_name : str
                The name of the cube parent to the level.
            level_name : str
                The name of the levels
            extension: str
                The format the data will be returned as. Defaults to "jsonrecords".
                Tesseract OLAP only supports "jsonrecords", "jsonarrays", and "csv".

        Returns:
            Union[str, dict]
                Depending on the extension: "csv" returns a :str:, "jsonrecords"
                returns a dict with
        """

        if extension not in TesseractDataFormat:
            raise KeyError("Format \"%s\" is not available on Tesseract Servers" % extension)

        url = parse.urljoin(self.base_url, "members.{}".format(extension))

        search_params = {"cube": cube_name, "level": level_name}
        if locale is not None:
            search_params["locale"] = locale

        async with httpx.AsyncClient() as client:
            request = await client.get(url, params=search_params)
            request.raise_for_status()

        if extension == TesseractDataFormat.CSV:
            return request.text
        else:
            return request.json()

    def set_endpoint(self, endpoint_type: "TesseractEndpointType"):
        """Sets the endpoint this Server instance will use to fetch data."""
        if endpoint_type == TesseractEndpointType.AGGREGATE:
            raise NotImplementedError("Aggregate endpoint is not yet fully supported")

        self.endpoint = endpoint_type
        return self

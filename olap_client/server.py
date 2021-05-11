"""
Module defining the Server base class.
"""

from urllib import parse

import httpx

from .query import Query

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
        url = parse.urljoin(self.base_url, self.build_query_url(query))
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        response.raise_for_status()
        return response.json() if "json" in query.format else response.content

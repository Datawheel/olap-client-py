"""Main Server base class.

The Server class is the base for all interactions with a data server. It should
not be instanciated, as their methods are only interface templates and are not
implemented.
The final user will interact only with instances of these classes or subclasses.
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
        self.base_url = base_url.rstrip("/").lower() + "/"

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

    async def fetch_members(self, *args, **kwargs):
        """Retrieves the list of members for a level in a cube.

        Needs to be implemented by each server subclass.
        """
        raise NotImplementedError

    async def exec_query(self, query: Query, timeout_sec=10):
        """Makes a request for the data specified in the Query object."""
        url = parse.urljoin(self.base_url, self.build_query_url(query))
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=timeout_sec)
        response.raise_for_status()
        return response.json() if "json" in query.extension else response.content

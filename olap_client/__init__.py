"""LogicLayer OLAP communication module.

Provides classes to generate queries and request data on Tesseract OLAP and
Mondrian REST servers.
"""

__all__ = (
    "InvalidQueryError",
    "Query",
    "Server",
)

from .exceptions import InvalidQueryError
from .query import Query
from .server import Server

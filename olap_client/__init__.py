"""LogicLayer OLAP communication module.

Provides classes to generate queries and request data on Tesseract OLAP and
Mondrian REST servers.
"""

from .exceptions import InvalidQueryError
from .query import Query
from .server import Server

__all__ = (
    "InvalidQueryError",
    "Query",
    "Server",
)
__version_info__ = ('0', '1', '0')
__version__ = '.'.join(__version_info__)

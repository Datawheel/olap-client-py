"""Olap Client exceptions module

Contains the errors and exceptions the code can raise at some point during execution.
"""

class EmptyDataException(Exception):
    """EmptyDataException

    This exception occurs when a query against a server returns an empty dataset.
    This might want to be caught and reported to the administrator.
    """


class InvalidQueryError(Exception):
    """InvalidQueryError

    This error occurs when a query is misconstructed.
    Can be raised before or after a request is made against a data server.
    """


class UpstreamInternalError(Exception):
    """UpstreamInternalError

    This error occurs when a remote server returns a valid response but the contents
    give details about an internal server error.
    This must be caught and reported to the administrator.
    """

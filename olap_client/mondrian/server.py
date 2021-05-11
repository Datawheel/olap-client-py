"""
Module that implements the Mondrian variant of the Server base class.
"""

from urllib import parse

from ..server import Query, Server


class MondrianServer(Server):
    """Class for Mondrian REST server requests."""

    def build_query_url(self, query: Query):
        """Converts the Query object into an URL for Mondrian REST."""

        str_cuts = (
            lambda level, members: f"{level}.&[{members[0]}]"
            if len(members) == 1
            else "{" + ",".join(f"{level}.&[{m}]" for m in members) + "}"
        )

        all_params = {
            "caption[]": [f"{level}.{prop}" for level, prop in query.captions.items()],
            "cut[]": [
                str_cuts(level, members) for level, members in query.cuts.items()
            ],
            "debug": query.booleans.get("debug"),
            "distinct": query.booleans.get("distinct"),
            "drilldown[]": query.drilldowns,
            "filter[]": [
                f"{calc} {conditions[0]} {conditions[1]}"
                for calc, conditions in query.filters.items()
            ],
            "limit": query.pagination[0],
            "measures[]": query.measures,
            "nonempty": query.booleans.get("nonempty"),
            "offset": query.pagination[1],
            "order_desc": query.sorting[1] in ("desc", "DESC"),
            "order": query.sorting[0],
            "parents": query.booleans.get("parents"),
            "properties[]": [
                f"{level}.{prop}" for level, prop in query.properties.items()
            ],
            "sparse": query.booleans.get("sparse"),
        }

        params = {k: v for k, v in all_params.items() if v is not None}
        search_params = parse.urlencode(params, True)
        return f"{query.cube}/aggregate.{query.format}?{search_params}"

    def fetch_members(self, cube_name: str, level_name: str):
        pass

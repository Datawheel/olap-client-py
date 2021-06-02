"""Tests for the Query class."""

import pytest

from olap_client.exceptions import InvalidQueryError
from olap_client.models import Cube
from olap_client.query import Comparison


def test_query_new_instance(cube_instance: Cube):
    """Attempts to create a new Query instance through instance initialization."""
    query = cube_instance.new_query()
    assert query.cube == cube_instance

def test_query_raise_on_invalid(cube_instance: Cube):
    """Attempts to add a few drilldowns to the query."""
    query = cube_instance.new_query()
    with pytest.raises(InvalidQueryError):
        query.raise_on_invalid()

    query.set_drilldown("Exporter Country")
    with pytest.raises(InvalidQueryError):
        query.raise_on_invalid()

def test_query_default_factories(cube_instance: Cube):
    """Checks if the default values for the properties in a Query are generated
    through factories, and are independent from each other."""

    query1 = cube_instance.new_query()
    query2 = cube_instance.new_query()

    query1.add_calculation("rate", params={})
    assert len(query2.calculations) == 0

    query1.set_cut("Year", members=[2019, 2020])
    assert len(query2.cuts) == 0

    query1.set_drilldown("Importer Continent")
    assert len(query2.drilldowns) == 0

    query1.set_filter("Quantity", (Comparison.GT, 0))
    assert len(query2.filters) == 0

    query1.add_measure("Trade Value")
    assert len(query2.measures) == 0

    query1.set_option("debug", True)
    assert len(query2.options) == 0

    query1.set_pagination(2, 7)
    assert all((item == 0 for item in query2.pagination))

    query1.set_sorting("Trade Value", "asc")
    assert query2.sorting[0] is None and query2.sorting[1] == "desc"

    query1.set_time("week", "oldest")
    assert all((item is None for item in query2.time))

"""Tests for the TesseractServer class."""

import pytest

from olap_client.query import Comparison
from olap_client.tesseract import (TesseractCube, TesseractDataFormat,
                                   TesseractServer)
from olap_client.tesseract.query import GrowthCalculation


def test_build_query_url():
    pass

@pytest.mark.asyncio
async def test_fetch_all_cubes(tesseract_server: TesseractServer):
    """Attempts to get all the cube available from the server."""
    cubes = await tesseract_server.fetch_all_cubes()
    assert len(cubes) == 80

@pytest.mark.asyncio
async def test_fetch_cube(tesseract_server: TesseractServer):
    """Attempts to get a specific cube from the server."""
    cube = await tesseract_server.fetch_cube("trade_i_baci_a_17")
    assert isinstance(cube, TesseractCube)
    assert cube.name == "trade_i_baci_a_17"

@pytest.mark.asyncio
async def test_fetch_members(tesseract_server: TesseractServer, cube_instance: TesseractCube):
    """Attempts to get the list of members for a level, from the server."""
    level = cube_instance.get_level("Year")
    members = await tesseract_server.fetch_members(cube_name=cube_instance.name,
                                                   level_name=level.name,
                                                   extension=TesseractDataFormat.JSONRECORDS)
    assert len(members["data"]) == 7

@pytest.mark.asyncio
async def test_build_logiclayer_url(cube_instance: TesseractCube):
    """Attempts to execute a query."""

    query = cube_instance.new_query()
    assert query.get_url() == "data.jsonrecords?cube=trade_i_baci_a_92"

    query.set_extension(TesseractDataFormat.CSV)
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92"

    query.set_drilldown("Year")
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&drilldowns=Year"

    query.add_measure("Trade Value")
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Trade+Value&drilldowns=Year"

    query.set_drilldown("Exporter Country")
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Trade+Value&drilldowns=Exporter+Country%2CYear"

    query.add_measure("Quantity")
    query.set_filter("Quantity", (Comparison.GT, 200), "and", (Comparison.LTE, 1000))
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&filters=Quantity.gt.200.and.lte.1000"

    query.set_property("Exporter Country ISO 2")
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&properties=Exporter+Country+ISO+2&filters=Quantity.gt.200.and.lte.1000"

    query.set_locale("es")
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&properties=Exporter+Country+ISO+2&filters=Quantity.gt.200.and.lte.1000&locale=es"

    query.set_pagination(10, 30)
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&properties=Exporter+Country+ISO+2&filters=Quantity.gt.200.and.lte.1000&locale=es&limit=10%2C30"

    query.set_option("debug", True)
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&properties=Exporter+Country+ISO+2&filters=Quantity.gt.200.and.lte.1000&locale=es&limit=10%2C30&debug=true"

    query.set_option("parents", False)
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&properties=Exporter+Country+ISO+2&filters=Quantity.gt.200.and.lte.1000&locale=es&limit=10%2C30&debug=true"

    query.set_sorting("Quantity", "asc")
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&properties=Exporter+Country+ISO+2&filters=Quantity.gt.200.and.lte.1000&locale=es&limit=10%2C30&sort=Quantity.asc&debug=true"

    growth_calc = GrowthCalculation(period=cube_instance.get_level("Year"),
                                    value=cube_instance.get_measure("Trade Value"))
    query.add_calculation(growth_calc)
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&properties=Exporter+Country+ISO+2&filters=Quantity.gt.200.and.lte.1000&locale=es&limit=10%2C30&sort=Quantity.asc&growth=Year%2CTrade+Value&debug=true"

    growth_calc = GrowthCalculation(period=cube_instance.get_level("Year"),
                                    value=cube_instance.get_measure("Quantity"))
    query.add_calculation(growth_calc)
    assert query.get_url() == "data.csv?cube=trade_i_baci_a_92&measures=Quantity%2CTrade+Value&drilldowns=Exporter+Country%2CYear&properties=Exporter+Country+ISO+2&filters=Quantity.gt.200.and.lte.1000&locale=es&limit=10%2C30&sort=Quantity.asc&growth=Year%2CQuantity&debug=true"

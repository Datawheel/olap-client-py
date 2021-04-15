import pytest
from olap_client import TesseractServer

@pytest.mark.asyncio
async def test_fetch_all_cubes():
    server = TesseractServer("https://api.datamexico.org/tesseract/")
    cubes = await server.fetch_all_cubes()
    assert len(cubes) == 75

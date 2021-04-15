import asyncio
from olap_client import TesseractServer

# async def test_fetch_all_cubes():
#     server = TesseractServer("https://api.datamexico.org/tesseract/")
#     cubes = await server.fetch_all_cubes()

# asyncio.run(test_fetch_all_cubes())
from olap_client.tesseract.schema import TesseractSchema

print(TesseractSchema.schema_json())

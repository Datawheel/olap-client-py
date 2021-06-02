"""Test fixtures module.

This module provides the test suite with some parameters to work easily.
The return value is passed to the test function that requires it based on the
fixture name. Check the documentation for pytest on fixtures for more details.

* pytest fixtures: <https://docs.pytest.org/en/6.2.x/fixture.html>
"""

import threading
from http.server import HTTPServer

import pytest

from olap_client.tesseract.server import TesseractServer

from .dummy_servers import TesseractDummyServer


@pytest.fixture(autouse=True)
def tesseract_olap_server(unused_tcp_port: int):
    """Runs a dummy Tesseract OLAP server in localhost to test HTTP requests."""
    server_address = ("localhost", unused_tcp_port)
    httpd = HTTPServer(server_address, TesseractDummyServer)
    print("Starting httpd server on %s:%d" % server_address)

    thread = threading.Thread(target=httpd.serve_forever)
    thread.daemon = True
    thread.start()

    return "http://%s:%d" % server_address

@pytest.fixture
def tesseract_server(tesseract_olap_server: str):
    """Provides a :class:`TesseractServer` instance connected to a dummy server."""
    return TesseractServer(tesseract_olap_server)

@pytest.fixture
async def cube_instance(tesseract_server: TesseractServer):
    """Provides a :class:Cube to the test."""
    cube = await tesseract_server.fetch_cube("trade_i_baci_a_92")
    return cube

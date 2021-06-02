"""Dummy server module.

These classes handles the requests for a fake server to test this package.
The servers (should) have most of the endpoints of the actual servers they
intend to disguise as.
"""

import json
from http.server import BaseHTTPRequestHandler
from pathlib import Path

cwd = Path(__file__, "..").absolute()


class TesseractDummyServer(BaseHTTPRequestHandler):
    """A handler class for a dummy Tesseract OLAP server."""

    def json_response(self, body: dict):
        """Help function to write a response."""
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode("utf8"))

    def do_GET(self):
        """Handler for GET requests."""

        if self.path.startswith("/cubes"):
            filepath = cwd.joinpath("tesseract_cubes.json")
            with filepath.open() as fileref:
                schema = json.load(fileref)

            cube_name = self.path.replace("/cubes", "").lstrip("/")
            if cube_name == "":
                self.json_response(schema)
                return

            cube = next((cube for cube in schema["cubes"] if cube["name"] == cube_name), None)
            if cube is not None:
                self.json_response(cube)
                return

        if self.path.startswith("/data"):
            pass

        if self.path.startswith("/members"):
            filepath = cwd.joinpath("tesseract_members.json")
            with filepath.open() as fileref:
                members = json.load(fileref)

            self.json_response(members)
            return

        self.send_error(404)
        self.end_headers()

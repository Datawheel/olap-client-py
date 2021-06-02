from os import path

from setuptools import find_packages, setup

from olap_client import __version__

here = path.abspath(path.dirname(__file__))
with open(path.join(here, "README.md")) as f:
    README = f.read()

setup(
    name="logiclayer",
    version=__version__,
    description="An agnostic client to interact with OLAP servers.",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Francisco Abarzua",
    author_email="francisco@datawheel.us",
    url="https://github.com/Datawheel/olap-client-py",
    install_requires=[
        "httpx",
        "pydantic",
    ],
    extras_require={
        ':python_version>="3.0"': ["requests >= 2.0.0"],
    },
    packages=find_packages(include=["olap_client", "olap_client.*"]),
    include_package_data=True,
    keywords="datawheel logiclayer olap-client tesseract-olap",
    classifiers=[
        "Development Status :: 1 - Planning",
        "Environment :: No Input/Output (Daemon)",
        "Framework :: FastAPI",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
)

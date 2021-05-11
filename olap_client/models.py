"""Olap Client data structures

These classes define standardized data structures for the data coming from a
supported OLAP server.
"""

from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, root_validator


class DimensionType(str, Enum):
    """Generic Dimension type enumeration class."""
    GEO = "geo"
    TIME = "time"
    STANDARD = "standard"


class Cube(BaseModel):
    """OLAP cube abstraction class."""
    annotations: Dict[str, str] = {}
    dimensions: List["Dimension"] = []
    measures: List["Measure"] = []
    name: str


# pylint: disable=E0213
class Dimension(BaseModel):
    """OLAP dimension abstraction class."""
    annotations: Dict[str, str] = {}
    default_hierarchy: Optional[str]
    dimension_type: DimensionType = DimensionType.STANDARD
    hierarchies: List["Hierarchy"] = []
    name: str

    @root_validator(pre=True)
    def inyect_hierarchy_tree(cls, dimension: dict):
        """Inyects the dimension name to each child hierachy."""
        for hierarchy in dimension.get("hierarchies", []):
            hierarchy["dimension"] = dimension["name"]
        return dimension


# pylint: disable=E0213
class Hierarchy(BaseModel):
    """OLAP hierarchy abstraction class."""
    annotations: Dict[str, str] = {}
    dimension: str
    levels: List["Level"] = []
    name: str

    @root_validator(pre=True)
    def inyect_level_tree(cls, hierarchy: dict):
        """Inyects the dimension and hierachy names to each child level."""
        for level in hierarchy.get("levels", []):
            level["dimension"] = hierarchy["dimension"]
            level["hierarchy"] = hierarchy["name"]
        return hierarchy


# pylint: disable=E0213
class Level(BaseModel):
    """OLAP level abstraction class."""
    annotations: Dict[str, str] = {}
    depth: int
    dimension: str
    hierarchy: str
    name: str
    properties: List["Property"] = []
    unique_name: Optional[str]

    @root_validator(pre=True)
    def inyect_property_tree(cls, level: dict):
        """Inyects the dimension, hierachy, and level names to each child property."""
        level["properties"] = level.get("properties", None) or []
        for propty in level["properties"]:
            propty["dimension"] = level["dimension"]
            propty["hierarchy"] = level["hierarchy"]
            propty["level"] = level["name"]
        return level


class Property(BaseModel):
    """OLAP level property abstraction class."""
    annotations: Dict[str, str] = {}
    dimension: str
    hierarchy: str
    level: str
    name: str


class Member(BaseModel):
    """OLAP level member abstraction class."""
    key: Union[str, int]
    name: str


class Measure(BaseModel):
    """OLAP measure abstraction class."""
    aggregator_meta: Dict[str, Union[str, int]] = {}
    aggregator_name: str
    annotations: Dict[str, str] = {}
    name: str


Cube.update_forward_refs()
Dimension.update_forward_refs()
Hierarchy.update_forward_refs()
Level.update_forward_refs()

"""Olap Client data structures

These classes define standardized data structures for the data coming from a
supported OLAP server.
"""

from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator

from .query import Query


class DimensionType(str, Enum):
    """Generic Dimension type enumeration class."""
    GEO = "geo"
    TIME = "time"
    STANDARD = "standard"


class Cube(BaseModel):
    """OLAP cube abstraction class."""
    annotations: Dict[str, str] = Field(default_factory=dict)
    dimensions: List["Dimension"] = Field(default_factory=list)
    measures: List["Measure"] = Field(default_factory=list)
    name: str

    @property
    def hierarchies(self):
        """Generates an iterator for all the child Levels in this cube."""
        for dimension in self.dimensions:
            for hierarchy in dimension.hierarchies:
                yield hierarchy

    @property
    def levels(self):
        """Generates an iterator for all the child Levels in this cube."""
        for dimension in self.dimensions:
            for hierarchy in dimension.hierarchies:
                for level in hierarchy.levels:
                    yield level

    @property
    def properties(self):
        """Generates an iterator for all the child Properties in this cube."""
        for dimension in self.dimensions:
            for hierarchy in dimension.hierarchies:
                for level in hierarchy.levels:
                    for prop in level.properties:
                        yield prop

    def new_query(self):
        """Creates a new Query instance, based on information from this cube."""
        return Query(self)

    def get_dimension(self, name: str):
        """Looks for a Dimension matching with the name provided."""
        return next((item for item in self.dimensions if item.name == name))

    def get_hierarchy(self, name: str):
        """Looks for a Hierarchy matching with the name provided."""
        return next((item for item in self.hierarchies if item.name == name))

    def get_level(self, name: str):
        """Looks for a child Level matching with the name provided."""
        return next((item for item in self.levels if item.matches(name)))

    def get_measure(self, name: str):
        """Looks for a Measure matching with the name provided."""
        return next((item for item in self.measures if item.name == name))

    def get_property(self, name: str):
        """Looks for a child Property matching with the name provided."""
        return next((item for item in self.properties if item.matches(name)))


# pylint: disable=E0213
class Dimension(BaseModel):
    """OLAP dimension abstraction class."""
    annotations: Dict[str, str] = Field(default_factory=dict)
    default_hierarchy: Optional[str]
    dimension_type: DimensionType = DimensionType.STANDARD
    hierarchies: List["Hierarchy"] = Field(default_factory=list)
    name: str

    @root_validator(pre=True)
    def inyect_parents(cls, dimension: dict):
        """Inyects the dimension name to each child hierachy."""
        for hierarchy in dimension.get("hierarchies", []):
            hierarchy["dimension"] = dimension["name"]
        return dimension

    @property
    def levels(self):
        """Generates an iterator for all the child Levels in this dimension."""
        for hierarchy in self.hierarchies:
            for level in hierarchy.levels:
                yield level

    @property
    def properties(self):
        """Generates an iterator for all the child Properties in this dimension."""
        for level in self.levels:
            for prop in level.properties:
                yield prop

    def get_hierarchy(self, name):
        """Looks for a Hierarchy matching with the name provided."""
        return next((item for item in self.hierarchies if item.matches(name)))

    def get_level(self, name: str):
        """Looks for a child Level matching with the name provided."""
        return next((item for item in self.levels if item.matches(name)))

    def get_property(self, name: str):
        """Looks for a child Property matching with the name provided."""
        return next((item for item in self.properties if item.matches(name)))


# pylint: disable=E0213
class Hierarchy(BaseModel):
    """OLAP hierarchy abstraction class."""
    annotations: Dict[str, str] = Field(default_factory=dict)
    dimension: str
    levels: List["Level"] = Field(default_factory=list)
    name: str

    @root_validator(pre=True)
    def inyect_parents(cls, hierarchy: dict):
        """Inyects the dimension and hierachy names to each child level."""
        for level in hierarchy.get("levels", []):
            level["dimension"] = hierarchy["dimension"]
            level["hierarchy"] = hierarchy["name"]
        return hierarchy

    @property
    def properties(self):
        """Generates an iterator for all the child Properties in this Hierarchy."""
        for level in self.levels:
            for prop in level.properties:
                yield prop

    def matches(self, name: str):
        """Checks if this instance matches a certain name."""
        return self.name == name

    def get_level(self, name):
        """Looks for a child Level matching with the name provided."""
        return next((item for item in self.levels if item.matches(name)))

    def get_property(self, name: str):
        """Looks for a child Property matching with the name provided."""
        return next((item for item in self.properties if item.matches(name)))


# pylint: disable=E0213
class Level(BaseModel):
    """OLAP level abstraction class."""
    annotations: Dict[str, str] = Field(default_factory=dict)
    depth: int
    dimension: str
    hierarchy: str
    name: str
    properties: List["Property"] = Field(default_factory=list)

    @root_validator(pre=True)
    def inyect_parents(cls, level: dict):
        """Inyects the dimension, hierachy, and level names to each child property."""
        level["properties"] = level.get("properties", None) or []
        for propty in level["properties"]:
            propty["dimension"] = level["dimension"]
            propty["hierarchy"] = level["hierarchy"]
            propty["level"] = level["name"]
        return level

    def matches(self, name: str):
        """Checks if this instance matches a certain name."""
        return self.name == name

    def get_property(self, name: str):
        """Looks for a Property in the children that matches a name."""
        return next((item for item in self.properties if item.matches(name)))


class Property(BaseModel):
    """OLAP level property abstraction class."""
    annotations: Dict[str, str] = Field(default_factory=dict)
    dimension: str
    hierarchy: str
    level: str
    name: str

    def __hash__(self):
        return hash((self.dimension, self.hierarchy, self.level, self.name))

    def matches(self, name: str):
        """Checks if this instance matches a certain name."""
        return self.name == name


class Member(BaseModel):
    """OLAP level member abstraction class."""
    key: Union[str, int]
    name: str


class Measure(BaseModel):
    """OLAP measure abstraction class."""
    aggregator_meta: Dict[str, Union[str, int]] = {}
    aggregator_name: str
    annotations: Dict[str, str] = Field(default_factory=dict)
    name: str

    def __hash__(self):
        return hash((self.name, self.aggregator_name))


Cube.update_forward_refs()
Dimension.update_forward_refs()
Hierarchy.update_forward_refs()
Level.update_forward_refs()

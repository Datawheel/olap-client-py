"""Olap Client data structures

These classes define standardized data structures for the data coming from a
supported OLAP server.
"""

from typing import Dict, List, Optional, Union

from pydantic import BaseModel


class Cube(BaseModel):
    annotations: Dict[str, str]
    dimensions: List["Dimension"]
    measures: List["Measure"]
    name: str
    namedsets: List["NamedSet"]


class Dimension(BaseModel):
    annotations: Dict[str, str]
    default_hierarchy: Optional[str]
    dimension_type: str
    full_name: str
    hierarchies: List["Hierarchy"]
    name: str


class Hierarchy(BaseModel):
    annotations: Dict[str, str]
    dimension: str
    full_name: str
    levels: List["Level"]
    name: str


class Level(BaseModel):
    annotations: Dict[str, str]
    caption: str
    depth: int
    dimension: str
    full_name: str
    hierarchy: str
    name: str
    properties: List["Property"]
    unique_name: Optional[str]


class Property(BaseModel):
    annotations: Dict[str, str]
    dimension: str
    hierarchy: str
    level: str
    name: str


class Member(BaseModel):
    caption: str
    key: Union[str, int]
    name: str


class Measure(BaseModel):
    aggregator_type: str
    annotations: Dict[str, str]
    caption: str
    name: str


class NamedSet(BaseModel):
    annotations: Dict[str, str]
    dimension: str
    hierarchy: str
    level: str
    name: str


Cube.update_forward_refs()
Dimension.update_forward_refs()
Hierarchy.update_forward_refs()
Level.update_forward_refs()

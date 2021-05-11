"""Tesseract OLAP's public schema data model definition."""

from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, root_validator, validator

from ..models import (Cube, Dimension, DimensionType, Hierarchy, Level,
                      Measure, Property)


class TesseractAggregatorType(str, Enum):
    """Tesseract's Measure Aggregator type enumeration class."""
    SUM = "sum"
    COUNT = "count"
    AVERAGE = "avg"
    MAX = "max"
    MIN = "min"
    BASICGROUPEDMEDIAN = "basic_grouped_median"
    WEIGHTEDAVERAGE = "weighted_average"
    WEIGHTEDSUM = "weighted_sum"
    REPLICATEWEIGHTMOE = "Replicate Weight MOE"
    MOE = "MOE"
    WEIGHTEDAVERAGEMOE = "weighted_average_moe"
    CUSTOM = "custom"
    UNKNOWN = "unknown"


class TesseractDataFormat(str, Enum):
    """Tesseract's available data format enumeration class."""
    CSV = "csv"
    JSONARRAYS = "jsonarrays"
    JSONRECORDS = "jsonrecords"


class TesseractDimensionType(str, Enum):
    """Tesseract's Dimension type enumeration class."""
    GEO = "geo"
    TIME = "time"
    STANDARD = "standard"


class TesseractEndpointType(str, Enum):
    """Tesseract's endpoint type enumeration class."""
    AGGREGATE = "aggregate"
    LOGICLAYER = "logiclayer"


class TesseractSchema(BaseModel):
    """Main parser class for Tesseract OLAP public schemas."""
    name: str
    cubes: List["TesseractCube"]
    annotations: Optional[Dict[str, str]]


class TesseractCube(Cube):
    """Model for a Cube from a Tesseract OLAP server."""
    alias: Optional[List[str]]
    dimensions: List["TesseractDimension"] = []
    measures: List["TesseractMeasure"] = []
    min_auth_level: int


# pylint: disable=E0213
class TesseractDimension(Dimension):
    """Model for a Dimension from a Tesseract OLAP server."""
    dimension_type: str = Field(..., alias="type")
    hierarchies: List["TesseractHierarchy"] = []

    @validator("dimension_type")
    def map_dimension_type(cls, value: str):
        """Remaps the values for dimension_type to the internal enum."""
        if value == TesseractDimensionType.STANDARD:
            return DimensionType.STANDARD
        if value == TesseractDimensionType.TIME:
            return DimensionType.TIME
        if value == TesseractDimensionType.GEO:
            return DimensionType.GEO
        raise ValueError("Invalid dimension type \"{}\"" % value)


# pylint: disable=E0213
class TesseractHierarchy(Hierarchy):
    """Model for a Hierarchy from a Tesseract OLAP server."""
    levels: List["TesseractLevel"]

    @validator("levels", pre=True)
    def inyect_level_depth(cls, levels: List[dict]):
        """Inyects the level depth value into a level."""
        for index, level in enumerate(levels, 1):
            level["depth"] = index
        return levels


class TesseractLevel(Level):
    """Model for a Level from a Tesseract OLAP server."""
    properties: List["TesseractProperty"] = []


class TesseractProperty(Property):
    """Model for a Level Property from a Tesseract OLAP server."""
    caption_set: Optional[str]
    unique_name: Optional[str]


class TesseractMeasureTypeStandard(BaseModel):
    """This model describes meta-info for a Standard-type measure."""
    units: Optional[str]


class TesseractMeasureTypeError(BaseModel):
    """This model describes meta-info for an Error-type measure."""
    for_measure: str
    err_type: str


# pylint: disable=E0213
class TesseractMeasure(Measure):
    """Model for a Measure from a Tesseract OLAP server."""
    name: str
    measure_type: Union[TesseractMeasureTypeStandard, TesseractMeasureTypeError]

    @root_validator(pre=True)
    def map_aggregator_type(cls, measure: dict):
        """Reformats aggregator data for the measure."""
        aggregator = measure.get("aggregator", {})
        measure["aggregator_name"] = aggregator.pop("name", TesseractAggregatorType.UNKNOWN)
        measure["aggregator_meta"] = aggregator
        return measure


TesseractSchema.update_forward_refs()
TesseractCube.update_forward_refs()
TesseractDimension.update_forward_refs()
TesseractHierarchy.update_forward_refs()
TesseractLevel.update_forward_refs()

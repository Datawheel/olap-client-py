from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel


class TesseractAggregatorType(str, Enum):
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


class TesseractDimensionType(str, Enum):
    GEO = "geo"
    TIME = "time"
    STANDARD = "std"


class TesseractMeasureTypeStandard(BaseModel):
    units: Optional[str]


class TesseractMeasureTypeError(BaseModel):
    for_measure: str
    err_type: str


class TesseractMeasureType(Enum):
    STANDARD = TesseractMeasureTypeStandard
    ERROR = TesseractMeasureTypeError


class TesseractSchema(BaseModel):
    name: str
    cubes: List["TesseractCube"]
    annotations: Optional[Dict[str, str]]


class TesseractCube(BaseModel):
    name: str
    dimensions: List["TesseractDimension"]
    measures: List["TesseractMeasure"]
    annotations: Optional[Dict[str, str]]
    alias: Optional[List[str]]
    min_auth_level: int


class TesseractDimension(BaseModel):
    name: str
    hierarchies: List["TesseractHierarchy"]
    default_hierarchy: Optional[str]
    dim_type: "TesseractDimensionType"
    annotations: Optional[Dict[str, str]]


class TesseractHierarchy(BaseModel):
    name: str
    levels: List["TesseractLevel"]
    annotations: Optional[Dict[str, str]]


class TesseractLevel(BaseModel):
    name: str
    properties: Optional[List["TesseractProperty"]]
    annotations: Optional[Dict[str, str]]
    unique_name: Optional[str]


class TesseractProperty(BaseModel):
    name: str
    caption_set: Optional[str]
    annotations: Optional[Dict[str, str]]
    unique_name: Optional[str]


class TesseractMeasure(BaseModel):
    name: str
    aggregator: "TesseractMeasureAggregator"
    measure_type: "TesseractMeasureType"
    annotations: Optional[Dict[str, str]]


class TesseractMeasureAggregator(BaseModel):
    name: "TesseractAggregatorType"


TesseractMeasureTypeStandard.update_forward_refs()
TesseractMeasureTypeError.update_forward_refs()
TesseractSchema.update_forward_refs()
TesseractCube.update_forward_refs()
TesseractDimension.update_forward_refs()
TesseractHierarchy.update_forward_refs()
TesseractLevel.update_forward_refs()
TesseractProperty.update_forward_refs()
TesseractMeasure.update_forward_refs()
TesseractMeasureAggregator.update_forward_refs()

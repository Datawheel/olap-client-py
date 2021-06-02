"""Tesseract Enums.

Enum classes that contain certain constants to work with when interacting with
Tesseract OLAP servers and its subclasses.
"""

from enum import Enum


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

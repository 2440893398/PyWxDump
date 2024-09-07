from enum import Enum


class AggregateStrategyEnum(Enum):
    """
    Enum for the different aggregation strategies
    """
    SUM = 1
    AVG = 2
    MAX = 3
    MIN = 4
    COUNT = 5
    OVERRIDE = 6
    GROUP_CONCAT = 7

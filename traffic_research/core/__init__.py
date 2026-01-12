"""Core modules for traffic research analysis."""

from .models import AccuracyScore
from .scoring import (
    calculateNumericScore,
    calculateConditionScore,
    computeTimeScore,
    computeConditionScore,
    computeFeatureScores
)
from .matching import (
    generateReferenceDataFrame,
    compareParameters,
    compareTimeDistance
)
from .utils import secondsToTimeString, enumToString

__all__ = [
    'AccuracyScore',
    'calculateNumericScore',
    'calculateConditionScore',
    'computeTimeScore',
    'computeConditionScore',
    'computeFeatureScores',
    'generateReferenceDataFrame',
    'compareParameters',
    'compareTimeDistance',
    'secondsToTimeString',
    'enumToString'
]

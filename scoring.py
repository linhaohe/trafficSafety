"""Scoring functions for row comparison."""

import math
from config import TIME_SCORE_WEIGHT, CONDITION_SCORE_WEIGHT, DEFAULT_TIME_THRESHOLD_WEIGHT


def calculateNumericScore(num1, num2, threshold):
    """Calculate numeric similarity score using exponential decay."""
    return math.exp(-abs(num1 - num2) / threshold)


def calculateConditionScore(condition1, condition2):
    """Calculate boolean condition score: 1.0 if match, 0.0 otherwise."""
    return 1.0 if condition1 == condition2 else 0.0


def computeTimeScore(row1, row2, threshold=DEFAULT_TIME_THRESHOLD_WEIGHT):
    """Compute time-based similarity score (weight: 50%)."""
    time_fields = [
        'Crossing Start Time',
        'Bus Stop Arrival Time',
        'Bus Stop Departure Time',
        'Intend to Cross Timestamp',
        'Refuge Island Start Time',
        'Refuge Island End Time',
        'Crossing End Time'
    ]
    return sum(calculateNumericScore(row1[field], row2[field], threshold) 
               for field in time_fields)


def computeConditionScore(row1, row2):
    """Compute condition-based similarity score (weight: 50%)."""
    condition_fields = [
        'User Type',
        'Estimated Gender',
        'Estimated Age Group',
        'Bus Interaction',
        'Roadway Crossing',
        'Type of Bus Interaction',
        'Crossing Interaction Notes',
        'Crossing Location Relative to Bus Stop',
        'Vehicle Traffic',
        'Group Size'
    ]
    return sum(calculateConditionScore(row1[field], row2[field]) 
               for field in condition_fields)


def computeFeatureScores(row1, row2, timeThreshold):
    """Compute weighted feature scores for row comparison."""
    timeScore = computeTimeScore(row1, row2, timeThreshold)
    conditionScore = computeConditionScore(row1, row2)
    return (timeScore * TIME_SCORE_WEIGHT + 
            conditionScore * CONDITION_SCORE_WEIGHT)

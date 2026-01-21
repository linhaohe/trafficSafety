"""Scoring functions for row comparison."""

import math
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from config import TIME_SCORE_WEIGHT, CONDITION_SCORE_WEIGHT, DEFAULT_TIME_THRESHOLD_WEIGHT


def calculateNumericScore(num1, num2, threshold):
    """Calculate numeric similarity score using exponential decay."""
    abs_diff = abs(num1 - num2)
    if threshold <= 0 or abs_diff > threshold:
        # Avoid division by zero, return 0 if threshold is invalid
        return 0.0
    if pd.isna(num1) or pd.isna(num2):
        # Handle NaN values
        return 0.0
    return math.exp(-abs_diff / threshold)


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
        'Group Size',
        'Crosswalk Crossing?',
        'Clothing Color',
        'Did User Finish Crossing During Pedestrian Phase?',
        'Bus Presence',
    ]
    return sum(calculateConditionScore(row1[field], row2[field]) 
               for field in condition_fields) * CONDITION_SCORE_WEIGHT / len(condition_fields)


def computeFeatureScores(row1, row2, timeThreshold):
    """Compute weighted feature scores for row comparison."""
    timeScore = computeTimeScore(row1, row2, timeThreshold)
    conditionScore = computeConditionScore(row1, row2)
    return (timeScore * TIME_SCORE_WEIGHT + 
            conditionScore)

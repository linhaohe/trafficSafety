"""Scoring functions for row comparison."""

import math
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from config import TIME_SCORE_WEIGHT, CONDITION_SCORE_WEIGHT, COLOR_WEIGHT


def calculateTimeScore(num1, num2, threshold):
    """Calculate numeric similarity score using exponential decay."""
    abs_diff = abs(num1 - num2)
    if threshold <= 0: 
        # Avoid division by zero, return 0 if threshold is invalid
        return 0.0
    if pd.isna(num1) or pd.isna(num2):
        # Handle NaN values
        return 0.0
    if abs_diff < threshold:
        return 1.0
    return math.exp(-abs_diff / (threshold + 10))


def calculateConditionScore(condition1, condition2):
    """Calculate boolean condition score: 1.0 if match, 0.0 otherwise."""
    return 1.0 if condition1 == condition2 else 0.0

def calculateClothingColorScore(color1, color2, decay=2.0):
    """Exponential decay similarity on brightness (values expected 1–10)."""
    if decay <= 0:
        return 0.0
    # Treat missing/invalid/unknown values as no similarity contribution
    if pd.isna(color1) or pd.isna(color2):
        return 0.0
    try:
        v1 = float(color1)
        v2 = float(color2)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(v1) or math.isnan(v2):
        return 0.0
    # If your pipeline uses -1 for "other/unknown", treat as no signal
    if v1 < 1 or v1 > 10 or v2 < 1 or v2 > 10:
        return 0.0
    abs_diff = abs(v1 - v2)
    return math.exp(-abs_diff / decay)

def computeTimeScore(row1, row2, threshold):
    """Compute time-based similarity score as an average in [0, 1].
    
    Each time field contributes a numeric similarity score in [0, 1]. We return
    the mean across all time fields so the scale is stable regardless of the
    number of fields. Fields where both values are -1 are skipped.
    """
    time_fields = [
        'Crossing Start Time',
        'Bus Stop Arrival Time',
        'Bus Stop Departure Time',
        'Intend to Cross Timestamp',
        'Refuge Island Start Time',
        'Refuge Island End Time',
        'Crossing End Time'
    ]
    
    # Only compare fields where both values are not -1
    valid_scores = []
    for field in time_fields:
        val1 = row1[field]
        val2 = row2[field]
        
        # Skip if both values are -1
        if val1 == -1 or val2 == -1:
            continue
        
        score = calculateTimeScore(val1, val2, threshold)
        valid_scores.append(score)
    
    # If no valid fields to compare, return 0
    if not valid_scores:
        return 0.0
    # Return average of valid scores (unweighted, weight applied in computeFeatureScores)
    return sum(valid_scores) / len(valid_scores)

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
        # 'Group Size',
        'Crosswalk Crossing',
        'Did User Finish Crossing During Pedestrian Phase',
        'Bus Presence',
    ]
    # Average score across all non-color condition fields
    base_condition_avg = (
        sum(calculateConditionScore(row1[field], row2[field])
            for field in condition_fields) / len(condition_fields)
        if condition_fields else 0.0
    )
    # Weighted combination: 70% other conditions, 30% clothing color
    other_weighted = base_condition_avg * (1 - COLOR_WEIGHT)
    colorScore = calculateClothingColorScore(row1['Clothing Color'], row2['Clothing Color']) * COLOR_WEIGHT
    condition_total = other_weighted + colorScore
    # Return unweighted score (weight applied in computeFeatureScores)
    return condition_total


def computeFeatureScores(row1, row2, timeThreshold):
    """Compute weighted feature scores for row comparison."""
    timeScore = computeTimeScore(row1, row2, timeThreshold)
    conditionScore = computeConditionScore(row1, row2)
    # Apply weights at the final combination level
    return (timeScore * TIME_SCORE_WEIGHT + 
            conditionScore * CONDITION_SCORE_WEIGHT)

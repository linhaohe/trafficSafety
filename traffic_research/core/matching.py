"""Functions for matching and comparing rows across dataframes."""

import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from .scoring import computeFeatureScores
from config import EXCLUDED_FROM_ACCURACY


def generateReferenceDataFrame(dflist, timeThreshold, percentageThreshold):
    """Generate reference DataFrame by matching rows across three dataframes.
    
    Compares:
    - A to B (df0 to df1): finds best match in B for each row in A
    - A to C (df0 to df2): finds best match in C for each row in A
    - B to C (df1 to df2): finds best match in C for each row in B
    
    Only considers matches above percentageThreshold. Once a row is matched, 
    it is marked as visited and cannot be used in future comparisons.
    """
    rows = []
    df0, df1, df2 = dflist[0], dflist[1], dflist[2]

    # Track which rows have been visited/used
    visited_b = set()  # Rows in df1 (B) that have been matched
    visited_c = set()  # Rows in df2 (C) that have been matched

    # Compare B to C (df1 to df2) - independent comparison for all rows in B
    bc_matches = {}
    for row1 in df1.itertuples():
        maxScore3, maxIndex3 = -1.0, -1
        for row2 in df2.itertuples():
            if row2.Index in visited_c:
                continue
            score = computeFeatureScores(df1.iloc[row1.Index], df2.iloc[row2.Index], timeThreshold)
            if score >= percentageThreshold and score > maxScore3:
                maxScore3, maxIndex3 = score, row2.Index
        bc_matches[row1.Index] = {
            "index2_bc": maxIndex3,
            "score3": maxScore3
        }

    # Compare A to B and A to C, and include B to C match
    for row in df0.itertuples():
        maxScore1, maxIndex1 = -1.0, -1
        maxScore2, maxIndex2 = -1.0, -1

        # Compare A to B (df0 to df1) - skip visited rows
        for row1 in df1.itertuples():
            if row1.Index in visited_b:
                continue
            score = computeFeatureScores(df0.iloc[row.Index], df1.iloc[row1.Index], timeThreshold)
            if score >= percentageThreshold and score > maxScore1:
                maxScore1, maxIndex1 = score, row1.Index

        # Compare A to C (df0 to df2) - skip visited rows
        for row2 in df2.itertuples():
            if row2.Index in visited_c:
                continue
            score = computeFeatureScores(df0.iloc[row.Index], df2.iloc[row2.Index], timeThreshold)
            if score >= percentageThreshold and score > maxScore2:
                maxScore2, maxIndex2 = score, row2.Index

        # Mark matched rows as visited only if score is above threshold
        if maxIndex1 != -1 and maxScore1 >= percentageThreshold:
            visited_b.add(maxIndex1)
        if maxIndex2 != -1 and maxScore2 >= percentageThreshold:
            visited_c.add(maxIndex2)

        # Get B->C match for the matched row in B
        if maxIndex1 in bc_matches:
            bc_match = bc_matches[maxIndex1]
            index2_bc = bc_match["index2_bc"]
            score3 = bc_match["score3"]
            
            if (index2_bc != -1 and index2_bc != maxIndex2 and score3 >= percentageThreshold):
                visited_c.add(index2_bc)
        else:
            index2_bc = -1
            score3 = -1.0

        rows.append({
            "index1": maxIndex1,
            "score1": maxScore1,
            "index2": maxIndex2,
            "score2": maxScore2,
            "index1_bc": maxIndex1,
            "index2_bc": index2_bc,
            "score3": score3
        })

    qualityDF = pd.DataFrame(rows)
    qualityDF = qualityDF.astype({
        "index1": "Int64", 
        "score1": "float64", 
        "index2": "Int64", 
        "score2": "float64",
        "index1_bc": "Int64",
        "index2_bc": "Int64",
        "score3": "float64"
    })
    return qualityDF


def compareParameters(row0, row1, row2, fieldName, accuracy, percentageThreshold):
    """Compare three parameter values and update accuracy tracking.
    
    Uses similarity scores to determine if values match:
    - scoreAB: similarity score between A and B
    - scoreAC: similarity score between A and C
    - scoreBC: similarity score between B and C
    
    Returns the agreed value if at least two match (with scores above threshold), 
    None otherwise.
    
    Note: Certain parameters are excluded from accuracy tracking.
    """
    A = row0['row'][fieldName]
    B = row1['row'][fieldName]
    C = row2['row'][fieldName]
    
    # Extract similarity scores
    scoreAB = row0['score'][0]  # A to B score
    scoreAC = row0['score'][1]  # A to C score
    scoreBC = row1['score'][1]  # B to C score
    
    # Check if values match
    ab_match = (A == B) and (scoreAB >= percentageThreshold)
    ac_match = (A == C) and (scoreAC >= percentageThreshold)
    bc_match = (B == C) and (scoreBC >= percentageThreshold)
    
    # Count how many pairs match
    match_count = sum([ab_match, ac_match, bc_match])
    
    # Check if this parameter should be excluded from accuracy tracking
    should_track_accuracy = fieldName not in EXCLUDED_FROM_ACCURACY
    
    # If all three match with scores above threshold
    if match_count == 3:
        if should_track_accuracy:
            accuracy.update(3, 0)
        return A
    
    # If at least one pair match
    if match_count >= 1:
        if should_track_accuracy:
            accuracy.update(3, 1)
        if ab_match or ac_match:
            return A
        elif ab_match or bc_match:
            return B
        return C
    
    # If all are different or insufficient matches
    if should_track_accuracy:
        accuracy.update(3, 3)
    return ""


def compareTimeDistance(timeA, timeB, timeC, accuracy, timeThreshold):
    """Compare three time values and return the one with minimum average distance.
    
    Returns the time value that has the smallest average distance to the other two,
    but only if at least one pair is within the time threshold.
    """
    # Calculate distances between pairs
    distAB = abs(timeA - timeB)
    distAC = abs(timeA - timeC)
    distBC = abs(timeB - timeC)
    
    # Check which pairs are within threshold
    matchAB = distAB <= timeThreshold
    matchAC = distAC <= timeThreshold
    matchBC = distBC <= timeThreshold
    
    # Calculate average distances for each time value
    avgA = (distAB + distAC) / 2
    avgB = (distAB + distBC) / 2
    avgC = (distAC + distBC) / 2
    
    # If all three are within threshold of each other
    if matchAB and matchAC and matchBC:
        accuracy.update(3, 0)
        if avgA <= avgB and avgA <= avgC:
            return timeA
        elif avgB <= avgA and avgB <= avgC:
            return timeB
        else:
            return timeC
    
    # If at least one pair matches
    if matchAB or matchAC or matchBC:
        accuracy.update(3, 1)
        
        if matchAB:
            return timeA if avgA <= avgB else timeB
        
        if matchAC:
            return timeA if avgA <= avgC else timeC
        
        if matchBC:
            return timeB if avgB <= avgC else timeC
    
    # If all are different (no pairs within threshold)
    accuracy.update(3, 3)
    return -1

"""Functions for matching and comparing rows across dataframes."""

import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from .scoring import computeFeatureScores
from config import EXCLUDED_FROM_ACCURACY

# assume range_value is user inputed value
def generateReferenceGraph(dflist, timeThreshold, percentageThreshold, timeColumn):
    """
    Generate a reference graph matching rows across three dataframes.

    Each df in dflist['df'] is assumed to be sorted by the same time column
    (passed in as timeColumn). Matching is restricted to rows in the target
    dataframe whose time lies in [targetTime - timeThreshold, targetTime + timeThreshold].
    """
    graph = {}
    used_targets = set()  # (path, index) already used as a match target

    # Default time column if not explicitly provided.
    # if timeColumn is None:
    #     timeColumn = "Crossing Start Time"

    def binarySearch(df, targetTime):
        """Return the first index in df[timeColumn] whose value is >= targetTime - timeThreshold.

        Assumes df[timeColumn] is sorted ascending. Returns -1 if no such index exists.
        """
        if df is None or df.empty:
            return -1

        target = targetTime - timeThreshold
        values = df[timeColumn].values
        left, right = 0, len(values) - 1
        best_idx = -1

        while left <= right:
            mid = (left + right) // 2
            val = values[mid]

            # Treat NaNs as very large so they are effectively at the end.
            if pd.isna(val):
                right = mid - 1
                continue

            if val >= target:
                best_idx = mid
                right = mid - 1
            else:
                left = mid + 1

        return best_idx

    def helper(fromDFTuple, toDFTuple, percentageThreshold, used_targets):
        fromDF = fromDFTuple["df"]
        toDF = toDFTuple["df"]
        fromDFName = fromDFTuple["path"]
        toDFName = toDFTuple["path"]

        # Cache target time column from toDF for faster access
        to_times = toDF[timeColumn].values if not toDF.empty else []

        for pos in range(len(fromDF)):
            from_row = fromDF.iloc[pos]  # cache row once per from-row; use position for iloc
            targetTime = from_row.get(timeColumn, -1)

            maxScore, maxIndex = 0.0, -1

            # If target time is invalid, skip time-based window and leave as no-match
            if pd.isna(targetTime) or targetTime < 0:
                key = (fromDFName, pos)
                if key not in graph:
                    graph[key] = []
                graph[key].append({"key": {"dfName": toDFName, "index": maxIndex}, "score": maxScore})
                continue

            start_idx = binarySearch(toDF, targetTime)
            if start_idx == -1:
                # No candidate in the time window on the low side; record no-match
                key = (fromDFName, pos)
                if key not in graph:
                    graph[key] = []
                graph[key].append({"key": {"dfName": toDFName, "index": maxIndex}, "score": maxScore})
                continue

            upper_bound = targetTime + timeThreshold

            i = start_idx
            while i < len(toDF):
                t = to_times[i]
                if pd.isna(t):
                    i += 1
                    continue
                if t > upper_bound:
                    break

                if (toDFName, i) in used_targets:
                    i += 1
                    continue

                score = computeFeatureScores(from_row, toDF.iloc[i], timeThreshold)
                if score >= percentageThreshold and score > maxScore:
                    maxScore, maxIndex = score, i
                    if maxScore >= 1.0:
                        break  # perfect match; no need to check rest of window

                i += 1

            if maxScore >= percentageThreshold and maxIndex >= 0:
                used_targets.add((toDFName, maxIndex))

            key = (fromDFName, pos)
            if key not in graph:
                graph[key] = []
            graph[key].append({"key": {"dfName": toDFName, "index": maxIndex}, "score": maxScore})

    helper(dflist[0], dflist[1], percentageThreshold, used_targets)
    helper(dflist[0], dflist[2], percentageThreshold, used_targets)
    helper(dflist[1], dflist[2], percentageThreshold, used_targets)
    return graph


def exportGraphToCsv(graph, csv_path):
    """Export the reference graph to a CSV with one row per node: from_dfName, from_index, then to_dfName_1, to_index_1, score_1, to_dfName_2, ... for all matches in the same row. dfName is stored as filename only (e.g. Alex.csv)."""
    def _basename(path):
        return os.path.basename(path) if path else ""
    max_matches = max(len(matches) for _, matches in graph.items()) if graph else 0
    rows = []
    for key, matches in graph.items():
        if isinstance(key, tuple):
            from_dfName = _basename(key[0])
            from_index = key[1]
        else:
            from_node = dict(key)
            from_dfName = _basename(from_node.get("dfName", ""))
            from_index = from_node.get("index", -1)
        row = {"from_dfName": from_dfName, "from_index": from_index}
        for i, m in enumerate(matches):
            to_node = m["key"]
            row[f"to_dfName_{i+1}"] = _basename(to_node.get("dfName", ""))
            row[f"to_index_{i+1}"] = to_node.get("index", -1)
            row[f"score_{i+1}"] = m["score"]
        for i in range(len(matches), max_matches):
            row[f"to_dfName_{i+1}"] = ""
            row[f"to_index_{i+1}"] = ""
            row[f"score_{i+1}"] = ""
        rows.append(row)
    df = pd.DataFrame(rows)
    # df = df.sort_values(by=["from_dfName", "from_index"])
    df.to_csv(csv_path, index=False)


def compareParameters(row0, row1, row2, fieldName, accuracy):
    """Compare three parameter values and update accuracy tracking.
    
    Compares values from three reviewers (A, B, C) and determines the consensus value
    based on matching pairs and similarity scores.
    
    Args:
        row0: Dictionary with 'row' (A's data) and 'score' [A->B, A->C]
        row1: Dictionary with 'row' (B's data) and 'score' [B->A, B->C]
        row2: Dictionary with 'row' (C's data) and 'score' [C->A, C->B]
        fieldName: Name of the field to compare
        accuracy: AccuracyScore object to update
        percentageThreshold: Minimum similarity score for a match
        
    Returns:
        The consensus value if matches found, empty string otherwise.
        
    Note:
        Certain parameters are excluded from accuracy tracking.
    """
    value_a = row0[fieldName] if row0 is not None else ""
    value_b = row1[fieldName]  if row1 is not None else ""
    value_c = row2[fieldName] if row2 is not None else ""
    
    # Extract similarity scores
    # score_ab = row0['score'][0]  # A to B similarity score
    # score_ac = row0['score'][1]  # A to C similarity score
    # score_bc = row1['score'][1]  # B to C similarity score
    
    # Check which pairs match (both value and score must match)
    ab_matches = (value_a == value_b) 
    ac_matches = (value_a == value_c) 
    bc_matches = (value_b == value_c) 
    
    match_count = sum([ab_matches, ac_matches, bc_matches])
    should_track_accuracy = fieldName not in EXCLUDED_FROM_ACCURACY
    # All three agree
    if match_count == 3:
        if should_track_accuracy:
            accuracy.update(3, 0)
        return value_a
    
    # At least one pair matches - determine consensus
    if match_count >= 1:
        if should_track_accuracy:
            accuracy.update(3, 1)
        
        # A matches both B and C -> A is consensus
        if ab_matches and ac_matches:
            return value_a
        
        # A matches B, and B matches C -> A is consensus (through B)
        if ab_matches and bc_matches:
            return value_a
        
        # A matches C, and B matches C -> C is consensus
        if ac_matches and bc_matches:
            return value_c
        
        # Only one pair matches
        if ab_matches :
            return value_a
        if ac_matches:
            return value_a
        if bc_matches:
            return value_b  # B is middle value when only B and C match
    
    # No matches found
    if should_track_accuracy:
        accuracy.update(3, 3)
    return ""


def compareTimeDistance(timeA, timeB, timeC, accuracy, timeThreshold):
    """Compare three time values and return the one with minimum average distance.
    
    Returns the time value that has the smallest average distance to the other two,
    but only if at least one pair is within the time threshold.
    """
    # Handle invalid time values (-1 indicates invalid/missing)
    if timeA == -1 and timeB == -1 and timeC == -1:
        accuracy.update(3, 3)
        return -1
    if timeA == -1:
        timeA = timeB if timeB != -1 else timeC
    if timeB == -1:
        timeB = timeA if timeA != -1 else timeC
    if timeC == -1:
        timeC = timeA if timeA != -1 else timeB
    
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

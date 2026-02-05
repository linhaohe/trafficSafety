"""Functions for matching and comparing rows across dataframes."""

import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from .scoring import computeFeatureScores
from config import EXCLUDED_FROM_ACCURACY

#assume range_value is user inputed value
def generateReferenceGraph(dflist, timeThreshold, percentageThreshold, range_value):
    """Build a graph of best matches between dflist[0], dflist[1], dflist[2].
    Assumes dflist is sorted by length (shortest first) and each df has default index 0..n-1.
    A (dfName, index) used as a target in one edge is never reused as a target elsewhere."""
    graph = {}
    used_targets = set()  # (path, index) already used as a match target

    def helper(fromDFTuple, toDFTuple, timeThreshold, percentageThreshold, range_value, used_targets):
        fromDF = fromDFTuple["df"]
        toDF = toDFTuple["df"]
        fromDFName = fromDFTuple["path"]
        toDFName = toDFTuple["path"]
        shockWave = len(toDF) - len(fromDF) + range_value

        for row in fromDF.itertuples():
            start_idx = max(0, row.Index - shockWave)
            end_idx = min(len(toDF), row.Index + shockWave + 1)
            maxScore, maxIndex = 0, -1
            for i in range(start_idx, end_idx):
                if (toDFName, i) in used_targets:
                    continue
                score = computeFeatureScores(fromDF.iloc[row.Index], toDF.iloc[i], timeThreshold)
                if score >= percentageThreshold and score > maxScore:
                    maxScore, maxIndex = score, i
            if maxScore >= percentageThreshold and maxIndex >= 0:
                used_targets.add((toDFName, maxIndex))
                dict_as_key = {'dfName': fromDFName, 'index': row.Index}
                immutable_key = frozenset(dict_as_key.items())
                dict_as_key_to_add = {'dfName': toDFName, 'index': maxIndex}
                # immutable_key_to_add = frozenset(dict_as_key_to_add.items())
                if immutable_key not in graph:
                    graph[immutable_key] = []
                # if immutable_key_to_add not in graph:
                #     graph[immutable_key_to_add] = []
                graph[immutable_key].append({"key": dict_as_key_to_add, "score": maxScore})
                # graph[immutable_key_to_add].append({"key": dict_as_key, "score": maxScore})

    helper(dflist[0], dflist[1], timeThreshold, percentageThreshold, range_value, used_targets)
    helper(dflist[0], dflist[2], timeThreshold, percentageThreshold, range_value, used_targets)
    helper(dflist[1], dflist[2], timeThreshold, percentageThreshold, range_value, used_targets)
    return graph


def exportGraphToCsv(graph, csv_path):
    """Export the reference graph to a CSV with one row per node: from_dfName, from_index, then to_dfName_1, to_index_1, score_1, to_dfName_2, ... for all matches in the same row. dfName is stored as filename only (e.g. Alex.csv)."""
    def _basename(path):
        return os.path.basename(path) if path else ""
    max_matches = max(len(matches) for _, matches in graph.items()) if graph else 0
    rows = []
    for key, matches in graph.items():
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
    df = df.sort_values(by=["from_dfName", "from_index"])
    df.to_csv(csv_path, index=False)


def generateReferenceDataFrame(dflist, timeThreshold, percentageThreshold, range_value):
    """Generate reference DataFrame by matching rows across three dataframes.
    
    Compares:
    - A to B (df0 to df1): for row i in A, compares with rows in B in range [i-range, i+range]
    - A to C (df0 to df2): for row i in A, compares with rows in C in range [i-range, i+range]
    - B to C (df1 to df2): for row i in B, compares with rows in C in range [i-range, i+range]
    
    For each row at index i, only compares with rows in other dataframes within 
    the range [i-range, i+range]. Only considers matches above percentageThreshold. 
    Once a row is matched, it is marked as visited and cannot be used in future comparisons.
    
    Args:
        dflist: List of three dataframes [df0, df1, df2]
        timeThreshold: Time threshold for matching
        percentageThreshold: Minimum similarity score for a match
        range_value: Range value for index comparison (default 0, meaning only same index)
    """
    rows = []
    df0, df1, df2 = dflist[0], dflist[1], dflist[2]

    # Track which rows have been visited/used
    visited_b = set()  # Rows in df1 (B) that have been matched
    visited_c = set()  # Rows in df2 (C) that have been matched

    # Compare B to C (df1 to df2) - for each row i in B, compare with rows in C in range [i-range, i+range]
    bc_matches = {}
    for row1 in df1.itertuples():
        i = row1.Index
        maxScore3, maxIndex3 = -1.0, -1
        
        # Calculate the range of indices to compare in df2
        start_idx = max(0, i - range_value)
        end_idx = min(len(df2), i + range_value + 1)
        
        for j in range(start_idx, end_idx):
            if j in visited_c:
                continue
            score = computeFeatureScores(df1.iloc[i], df2.iloc[j], timeThreshold)
            if score >= percentageThreshold and score > maxScore3:
                maxScore3, maxIndex3 = score, j
        
        bc_matches[i] = {
            "index2_bc": maxIndex3,
            "score3": maxScore3
        }

    # Compare A to B and A to C, and include B to C match
    for row in df0.itertuples():
        i = row.Index
        maxScore1, maxIndex1 = -1.0, -1
        maxScore2, maxIndex2 = -1.0, -1

        # Compare A to B (df0 to df1) - compare row i in A with rows in B in range [i-range, i+range]
        start_idx_b = max(0, i - range_value)
        end_idx_b = min(len(df1), i + range_value + 1)
        for j in range(start_idx_b, end_idx_b):
            if j in visited_b:
                continue
            score = computeFeatureScores(df0.iloc[i], df1.iloc[j], timeThreshold)
            if score >= percentageThreshold and score > maxScore1:
                maxScore1, maxIndex1 = score, j

        # Compare A to C (df0 to df2) - compare row i in A with rows in C in range [i-range, i+range]
        start_idx_c = max(0, i - range_value)
        end_idx_c = min(len(df2), i + range_value + 1)
        for j in range(start_idx_c, end_idx_c):
            if j in visited_c:
                continue
            score = computeFeatureScores(df0.iloc[i], df2.iloc[j], timeThreshold)
            if score >= percentageThreshold and score > maxScore2:
                maxScore2, maxIndex2 = score, j

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

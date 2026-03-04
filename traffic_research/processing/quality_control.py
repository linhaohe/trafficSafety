"""Quality control functions for generating and testing data quality."""

from typing import Any
import pandas as pd
import sys
import os
from enum import Enum
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from traffic_research.core.data_engineering import DataEngining, float_cols
from traffic_research.core.matching import compareParameters, compareTimeDistance
from traffic_research.core.utils import enumToString, secondsToTimeString
from config import EXCLUDED_FROM_ACCURACY, DEFAULT_TIME_THRESHOLD

def parseEnumObjectRow(rowObject):
    
    def safeStr(value, default=""):
        """Convert value to string, using default if value is -1 or empty."""
        if value == -1:
            return default
        return str(value) if value != default else default
    
    def enumToStr(field, enumType, default=""):
        """Convert enum value to string with optional default for empty values."""  
        result = enumToString(field, enumType)
        return result if result else default
    
    
    videoTitle = safeStr(rowObject['Video Title'])
    locationName = safeStr(rowObject['Location Name'])
    busStopIDs = safeStr(rowObject['Bus Stop IDs/Addresses'])
    busStopRouteCount = rowObject['Count of Bus Stop Routes']
    crosswalkLocationRelativeToBusStop = rowObject['Crosswalk Location Relative to Bus Stop']
    crossingTreatment = rowObject['Crossing Treatment']
    refugeIsland = enumToStr(rowObject['Refuge Island'], DataEngining.boolean)
    
    userType = enumToStr(rowObject['User Type'], DataEngining.userType)
    groupSize = safeStr(rowObject['Group Size'])
    estimatedGender = enumToStr(rowObject['Estimated Gender'], DataEngining.gender, default="hard to tell")
    estimatedAgeGroup = enumToStr(rowObject['Estimated Age Group'], DataEngining.ageGroup, default="hard to tell")
    clothingColor = enumToStr(rowObject['Clothing Color'], DataEngining.clothingColor, default="hard to tell")
    visibilityScale = rowObject['Visibility Scale']
    estimatedVisibleDistraction = enumToStr(rowObject['Estimated Visible Distrction'], DataEngining.boolean)
    userNotes = rowObject['User Notes']
    
    busInteraction = enumToStr(rowObject['Bus Interaction'], DataEngining.boolean)
    roadwayCrossing = enumToStr(rowObject['Roadway Crossing'], DataEngining.boolean)
    typeOfBusInteraction = enumToStr(rowObject['Type of Bus Interaction'], DataEngining.busInteractions)
    busArrivalTime = secondsToTimeString(rowObject['Bus Stop Arrival Time'])
    busDepartureTime = secondsToTimeString(rowObject['Bus Stop Departure Time'])
    busPresence = enumToStr(rowObject['Bus Presence'], DataEngining.boolean)
    
    intendToCrossTimestamp = secondsToTimeString(rowObject['Intend to Cross Timestamp'])
    crossingStartTime = secondsToTimeString(rowObject['Crossing Start Time'])
    refugeIslandStartTime = secondsToTimeString(rowObject['Refuge Island Start Time'])
    refugeIslandEndTime = secondsToTimeString(rowObject['Refuge Island End Time'])
    crossingEndTime = secondsToTimeString(rowObject['Crossing End Time'])
    
    crosswalkCrossing = enumToStr(rowObject['Crosswalk Crossing'], DataEngining.boolean)
    pedestrianPhaseCrossing = enumToStr(rowObject['Pedestrian Phase Crossing'], DataEngining.boolean)
    finishedDuringPedsPhase = enumToStr(rowObject['Did User Finish Crossing During Pedestrian Phase'], DataEngining.boolean)
    walkingInteraction = enumToStr(rowObject['Crossing Interaction Notes'], DataEngining.walkInteractions)
    crossingLocationToBus = enumToStr(rowObject['Crossing Location Relative to Bus'], DataEngining.crossingLocationRelativeToBus)
    crossingLocationRelativeToBusStop = enumToStr(rowObject['Crossing Location Relative to Bus Stop'], DataEngining.crossingLocationRelativeToBusStop)
    trafficCondition = enumToStr(rowObject['Vehicle Traffic'], DataEngining.trafficVolume)
    initials = rowObject['Initials']
    userCount = rowObject['User Count']
    noteworthyEvents = rowObject['Noteworthy Events']
    busNoteworthyEvents = rowObject['Bus Noteworthy Events']
    generalReviewerNotes = rowObject['General Reviewer Notes']

    result = {
        "Video Title": videoTitle,
        'Initials': initials,
        "Location Name": locationName,
        "Bus Stop IDs/Addresses": busStopIDs,
        "Count of Bus Stop Routes": busStopRouteCount,
        "Crosswalk Location Relative to Bus Stop": crosswalkLocationRelativeToBusStop,
        "Crossing Treatment": crossingTreatment,
        "Refuge Island": refugeIsland,
        "User Count": userCount,
        "User Type": userType,
        "Group Size": groupSize,
        "Estimated Gender": estimatedGender,
        "Estimated Age Group": estimatedAgeGroup,
        "Clothing Color": clothingColor,
        "Visibility Scale": visibilityScale,
        "Estimated Visible Distrction": estimatedVisibleDistraction,
        "User Notes": userNotes,
        "Bus Interaction": busInteraction,
        "Roadway Crossing": roadwayCrossing,
        "Type of Bus Interaction": typeOfBusInteraction,
        "Bus Stop Arrival Time": busArrivalTime,
        "Bus Stop Departure Time": busDepartureTime,
        "Bus Noteworthy Events": busNoteworthyEvents,
        "Crosswalk Crossing": crosswalkCrossing,
        "Pedestrian Phase Crossing": pedestrianPhaseCrossing,
        "Intend to Cross Timestamp": intendToCrossTimestamp,
        "Crossing Start Time": crossingStartTime,
        "Refuge Island Start Time": refugeIslandStartTime,
        "Refuge Island End Time": refugeIslandEndTime,
        "Did User Finish Crossing During Pedestrian Phase": finishedDuringPedsPhase,
        "Crossing End Time": crossingEndTime,
        "Crossing Interaction Notes": walkingInteraction,
        "Bus Presence": busPresence,
        "Crossing Location Relative to Bus": crossingLocationToBus,
        "Crossing Location Relative to Bus Stop": crossingLocationRelativeToBusStop,
        "Vehicle Traffic": trafficCondition,
        "Noteworthy Events": noteworthyEvents,
        "General Reviewer Notes": generalReviewerNotes
    }
    if "CrossingDuration" in rowObject:
        result["CrossingDuration"] = rowObject['CrossingDuration']
    return result
    
def constructRowDict(row0, row1, row2, index, accuracy, timeThreshold):
    """Construct a row dictionary by comparing three reviewer rows."""
    
    def compare(field):
        return compareParameters(row0, row1, row2, field, accuracy)
    
    def compareTime(field):
        time0 = row0[field] if row0 is not None else -1
        time1 = row1[field] if row1 is not None else -1
        time2 = row2[field] if row2 is not None else -1
        
        return compareTimeDistance(time0, time1, time2, accuracy, timeThreshold)
    
        
    def combineNotes(field):
        list = []
        if row0 is not None and row0[field] != 'nan' and row0[field] !='None':
            list.append(row0[field])
        if row1 is not None and row1[field] != 'nan' and row1[field] !='None':
            list.append(row1[field])
        if row2 is not None and row2[field] != 'nan' and row2[field] !='None':
            list.append(row2[field])
        return list
    # Location and infrastructure fields
    videoTitle = compare('Video Title')
    locationName = compare('Location Name')
    busStopIDs = compare('Bus Stop IDs/Addresses')
    busStopRouteCount = compare('Count of Bus Stop Routes')
    crosswalkLocationRelativeToBusStop = compare('Crosswalk Location Relative to Bus Stop')
    crossingTreatment = compare('Crossing Treatment')
    refugeIsland = compare('Refuge Island')
    
    # User information fields
    userCount = index + 1
    userType = compare('User Type')
    groupSize = compare('Group Size')
    estimatedGender = compare('Estimated Gender')
    estimatedAgeGroup = compare('Estimated Age Group')
    clothingColor = compare('Clothing Color')
    visibilityScale = compare('Visibility Scale')
    estimatedVisibleDistraction = compare('Estimated Visible Distrction')
    
    # Bus interaction fields
    busInteraction = compare('Bus Interaction')
    roadwayCrossing = compare('Roadway Crossing')
    typeOfBusInteraction = compare('Type of Bus Interaction')
    busArrivalTime = compareTime('Bus Stop Arrival Time')
    busDepartureTime = compareTime('Bus Stop Departure Time')
    busPresence = compare('Bus Presence')
    
    # Crossing timing fields
    intendToCrossTimestamp = compareTime('Intend to Cross Timestamp')
    crossingStartTime = compareTime('Crossing Start Time')
    refugeIslandStartTime = compareTime('Refuge Island Start Time')
    refugeIslandEndTime = compareTime('Refuge Island End Time')
    crossingEndTime = compareTime('Crossing End Time')
    
    # Crossing behavior fields
    crosswalkCrossing = compare('Crosswalk Crossing')
    pedestrianPhaseCrossing = compare('Pedestrian Phase Crossing')
    finishedDuringPedsPhase = compare('Did User Finish Crossing During Pedestrian Phase')
    walkingInteraction = compare('Crossing Interaction Notes')
    crossingLocationToBus = compare('Crossing Location Relative to Bus')
    crossingLocationRelativeToBusStop = compare('Crossing Location Relative to Bus Stop')
    
    # Traffic and notes fields
    trafficCondition = compare('Vehicle Traffic')
    times_for_min = [t for t in (
        compareTime('Bus Stop Arrival Time'),
        compareTime('Intend to Cross Timestamp'),
        compareTime('Crossing Start Time'),
    ) if t > 0]
    minTime = min(times_for_min) if times_for_min else -1
    
    noteworthyEvents = combineNotes('Noteworthy Events')
    busNoteworthyEvents = combineNotes('Bus Noteworthy Events')
    generalReviewerNotes = combineNotes('General Reviewer Notes')
    userNotes = combineNotes('User Notes')
    result = parseEnumObjectRow({
        "Video Title": videoTitle,
        'Initials': '',
        "Location Name": locationName,
        "Bus Stop IDs/Addresses": busStopIDs,
        "Count of Bus Stop Routes": busStopRouteCount,
        "Crosswalk Location Relative to Bus Stop": crosswalkLocationRelativeToBusStop,
        "Crossing Treatment": crossingTreatment,
        "Refuge Island": refugeIsland,
        "User Count": userCount,
        "User Type": userType,
        "Group Size": groupSize,
        "Estimated Gender": estimatedGender,
        "Estimated Age Group": estimatedAgeGroup,
        "Clothing Color": clothingColor,
        "Visibility Scale": visibilityScale,
        "Estimated Visible Distrction": estimatedVisibleDistraction,
        "User Notes": userNotes,
        "Bus Interaction": busInteraction,
        "Roadway Crossing": roadwayCrossing,
        "Type of Bus Interaction": typeOfBusInteraction,
        "Bus Stop Arrival Time": busArrivalTime,
        "Bus Stop Departure Time": busDepartureTime,
        "Noteworthy Events": noteworthyEvents,
        "Crosswalk Crossing": crosswalkCrossing,
        "Pedestrian Phase Crossing": pedestrianPhaseCrossing,
        "Intend to Cross Timestamp": intendToCrossTimestamp,
        "Crossing Start Time": crossingStartTime,
        "Refuge Island Start Time": refugeIslandStartTime,
        "Refuge Island End Time": refugeIslandEndTime,
        "Did User Finish Crossing During Pedestrian Phase": finishedDuringPedsPhase,
        "Crossing End Time": crossingEndTime,
        "Crossing Interaction Notes": walkingInteraction,
        "Bus Presence": busPresence,
        "Crossing Location Relative to Bus": crossingLocationToBus,
        "Crossing Location Relative to Bus Stop": crossingLocationRelativeToBusStop,
        "Bus Noteworthy Events": busNoteworthyEvents,
        "Vehicle Traffic": trafficCondition,
        "General Reviewer Notes": generalReviewerNotes
    })
    result["sort_key"] = minTime
    result["CrossingDuration"] = crossingEndTime - crossingStartTime
    result["IntendCrossingDuration"] = crossingEndTime - intendToCrossTimestamp
    return result

def generateQualityControlDataFramebyGraph(refGraph, dflist, accuracy, timeThreshold):
    """Build QC rows from refGraph by resolving each node to (row0, row1?, row2?) and calling constructRowDict."""
    paths = [dflist[i]["path"] for i in range(3)]
    dfs = [dflist[i]["df"] for i in range(3)]
    path_to_idx = {p: i for i, p in enumerate(paths)}
    visited = [set(), set(), set()]
    rows = []

    for key, matches in refGraph.items():
        if isinstance(key, tuple):
            from_dfName, from_index = key[0], key[1]
        else:
            from_dict = dict(key)
            from_dfName = from_dict["dfName"]
            from_index = from_dict["index"]
        from_idx = path_to_idx[from_dfName]
        if from_index in visited[from_idx] or len(matches) == 0:
            continue
        visited[from_idx].add(from_index)

        # Unpack match keys once; -1 index means no match
        m0_key = matches[0]["key"] if len(matches) > 0 else None
        m0_score = matches[0]["score"] if len(matches) > 0 else -1
        m1_key = matches[1]["key"] if len(matches) > 1 else None
        m1_score = matches[1]["score"] if len(matches) > 1 else -1            
        valid_0 = m0_key is not None and m0_score > -1 and m0_key["index"] >= 0 and m0_key["index"] not in visited[path_to_idx[m0_key["dfName"]]]
        valid_1 = m1_key is not None and m1_score > -1 and m1_key["index"] >= 0 and m1_key["index"] not in visited[path_to_idx[m1_key["dfName"]]]
        
        if not valid_1 and valid_0 and len(matches) > 1:
            # print(f"valid_1 is None and valid_0 is not None: {valid_1} {valid_0}")
            # print(f"m0_key: {m0_key}")
            m1_key = refGraph[(m0_key["dfName"], m0_key["index"])][0]["key"] if refGraph[(m0_key["dfName"], m0_key["index"])] else None
            m1_score = refGraph[(m0_key["dfName"], m0_key["index"])][0]["score"] if refGraph[(m0_key["dfName"], m0_key["index"])] else -1
            valid_1 = m1_key is not None and m1_score > -1 and m1_key["index"] >= 0 and m1_key["index"] not in visited[path_to_idx[m1_key["dfName"]]]

        if not (valid_0 or valid_1):
            continue

        row0 = dfs[from_idx].iloc[from_index]
        row1 = None
        row2 = None
        if valid_0:
            idx0 = path_to_idx[m0_key["dfName"]]
            row1 = dfs[idx0].iloc[m0_key["index"]]
            visited[idx0].add(m0_key["index"])
        if valid_1:
            idx1 = path_to_idx[m1_key["dfName"]]
            row2 = dfs[idx1].iloc[m1_key["index"]]
            visited[idx1].add(m1_key["index"])
        rows.append(constructRowDict(row0, row1, row2, from_index, accuracy, timeThreshold))
    return pd.DataFrame(rows)


def accuracyTest(humanQualityDF, computedQualityDF):
    """Test accuracy by comparing human quality control with computed results.
    
    Handles cases where:
    - Human quality DF is missing columns (only compares common columns)
    - Computed quality DF is missing columns (treats as mismatch for those columns)
    
    Note: Certain parameters are excluded from accuracy tracking.
    """
    correctCount = 0
    indexCount = 0
    rowCount = 0
    
    # Get intersection of columns that exist in both DataFrames
    common_columns = humanQualityDF.columns.intersection(computedQualityDF.columns)
    # Filter out excluded columns from accuracy tracking
    columns_to_compare = [col for col in common_columns if col not in EXCLUDED_FROM_ACCURACY]
    
    # Columns in human DF but not in computed DF (treated as mismatches)
    missing_in_computed = humanQualityDF.columns.difference(computedQualityDF.columns)
    # Filter out excluded columns
    missing_in_computed = [col for col in missing_in_computed if col not in EXCLUDED_FROM_ACCURACY]
    
    for row in humanQualityDF.itertuples():
        if len(computedQualityDF) > rowCount:
            humanRow = humanQualityDF.iloc[row.Index]
            computedRow = computedQualityDF.iloc[row.Index]
            
            # Compare columns that exist in both DataFrames (excluding specified parameters)
            for col in columns_to_compare:
                humanVal = humanRow[col]
                computedVal = computedRow[col]
                
                if (humanVal == computedVal or 
                    ((pd.isna(humanVal) or humanVal == '0') and 
                     (pd.isna(computedVal) or computedVal == '0'))):
                    correctCount += 1
                elif col in float_cols:
                    try:
                        if abs(float(humanVal) - float(computedVal)) < DEFAULT_TIME_THRESHOLD:
                            correctCount += 1
                    except (ValueError, TypeError):
                        pass
                indexCount += 1
            
            # Count missing columns in computed DF as mismatches
            indexCount += len(missing_in_computed)
            rowCount += 1
        else:
            break
    
    # Account for remaining rows
    if len(humanQualityDF) - rowCount > 0:
        indexCount += (len(humanQualityDF) - rowCount) * (len(columns_to_compare) + len(missing_in_computed))
    elif len(computedQualityDF) - rowCount > 0:
        indexCount += (len(computedQualityDF) - rowCount) * len(columns_to_compare)
    
    return correctCount / indexCount if indexCount > 0 else 0.0

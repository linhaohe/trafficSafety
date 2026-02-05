"""Quality control functions for generating and testing data quality."""

from typing import Any
import heapq
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from traffic_research.core.data_engineering import DataEngining, float_cols
from traffic_research.core.matching import compareParameters, compareTimeDistance
from traffic_research.core.utils import enumToString, secondsToTimeString
from config import EXCLUDED_FROM_ACCURACY, DEFAULT_TIME_THRESHOLD


def constructRowDict(row0, row1, row2, index, accuracy, timeThreshold):
    """Construct a row dictionary by comparing three reviewer rows."""
    def compare(field):
        return compareParameters(row0, row1, row2, field, accuracy)
    
    def compareTime(field):
        time0 = row0[field] if row0 is not None else -1
        time1 = row1[field] if row1 is not None else -1
        time2 = row2[field] if row2 is not None else -1
        return compareTimeDistance(time0, time1, time2, accuracy, timeThreshold)
    
    def enumToStr(field, enumType, default=""):
        """Convert enum value to string with optional default for empty values."""  
        result = enumToString(compare(field), enumType)
        return result if result else default
    
    def safeStr(value, default=""):
        """Convert value to string, using default if value is -1 or empty."""
        if value == -1:
            return default
        return str(value) if value != default else default
    
    # Location and infrastructure fields
    videoTitle = safeStr(compare('Video Title'))
    locationName = safeStr(compare('Location Name'))
    busStopIDs = safeStr(compare('Bus Stop IDs/Addresses'))
    busStopRouteCount = compare('Count of Bus Stop Routes')
    crosswalkLocationRelativeToBusStop = compare('Crosswalk Location Relative to Bus Stop')
    crossingTreatment = compare('Crossing Treatment')
    refugeIsland = enumToStr('Refuge Island', DataEngining.boolean)
    
    # User information fields
    userCount = index + 1
    userType = enumToStr('User Type', DataEngining.userType)
    groupSize = safeStr(compare('Group Size'))
    estimatedGender = enumToStr('Estimated Gender', DataEngining.gender, default="hard to tell")
    estimatedAgeGroup = enumToStr('Estimated Age Group', DataEngining.ageGroup, default="hard to tell")
    clothingColor = enumToStr('Clothing Color', DataEngining.clothingColor, default="hard to tell")
    visibilityScale = compare('Visibility Scale')
    estimatedVisibleDistraction = enumToStr('Estimated Visible Distrction', DataEngining.boolean)
    userNotes = compare('User Notes')
    
    # Bus interaction fields
    busInteraction = enumToStr('Bus Interaction', DataEngining.boolean)
    roadwayCrossing = enumToStr('Roadway Crossing', DataEngining.boolean)
    typeOfBusInteraction = enumToStr('Type of Bus Interaction', DataEngining.busInteractions)
    busArrivalTime = secondsToTimeString(compareTime('Bus Stop Arrival Time'))
    busDepartureTime = secondsToTimeString(compareTime('Bus Stop Departure Time'))
    busPresence = enumToStr('Bus Presence', DataEngining.boolean)
    
    # Crossing timing fields
    intendToCrossTimestamp = secondsToTimeString(compareTime('Intend to Cross Timestamp'))
    crossingStartTime = secondsToTimeString(compareTime('Crossing Start Time'))
    refugeIslandStartTime = secondsToTimeString(compareTime('Refuge Island Start Time'))
    refugeIslandEndTime = secondsToTimeString(compareTime('Refuge Island End Time'))
    crossingEndTime = secondsToTimeString(compareTime('Crossing End Time'))
    
    # Crossing behavior fields
    crosswalkCrossing = enumToStr('Crosswalk Crossing', DataEngining.boolean)
    # print(f"crosswalkCrossing: {crosswalkCrossing}")
    pedestrianPhaseCrossing = enumToStr('Pedestrian Phase Crossing', DataEngining.boolean)
    finishedDuringPedsPhase = enumToStr('Did User Finish Crossing During Pedestrian Phase', DataEngining.boolean)
    walkingInteraction = enumToStr('Crossing Interaction Notes', DataEngining.walkInteractions)
    crossingLocationToBus = enumToStr('Crossing Location Relative to Bus', DataEngining.crossingLocationRelativeToBus)
    crossingLocationRelativeToBusStop = enumToStr('Crossing Location Relative to Bus Stop', DataEngining.crossingLocationRelativeToBusStop)
    
    # Traffic and notes fields
    trafficCondition = enumToStr('Vehicle Traffic', DataEngining.trafficVolume)
    
    return {
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
        "Noteworthy Events": '0',
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
        "Bus Noteworthy Events": '0',
        "Vehicle Traffic": trafficCondition,
        "General Reviewer Notes": '0'
    }

def generateQualityControlDataFramebyGraph(refGraph, dflist, accuracy, timeThreshold):
    # for key, matches in refGraph.items():
    df_dict = {}
    rows = []
    df_dict[dflist[0]['path']] = {'df': dflist[0]['df'], 'visted': set()}
    df_dict[dflist[1]['path']] = {'df': dflist[1]['df'], 'visted': set()}
    df_dict[dflist[2]['path']] = {'df': dflist[2]['df'], 'visted': set()}
    for key,matches in refGraph.items():
        from_dict = dict(key)
        from_dfName = from_dict['dfName']
        from_index = from_dict['index']   
        if from_index in df_dict[from_dfName]['visted'] or len(matches) == 0:
            continue
        df_dict[from_dfName]['visted'].add(from_index)
        row0 = df_dict[from_dfName]['df'].iloc[from_index]
        row1 = None
        row2 = None
        
        if len(matches) > 0:
            row1 = df_dict[matches[0]['key']['dfName']]['df'].iloc[matches[0]['key']['index']]
            df_dict[matches[0]['key']['dfName']]['visted'].add(matches[0]['key']['index'])
        if len(matches) > 1:
            row2 = df_dict[matches[1]['key']['dfName']]['df'].iloc[matches[1]['key']['index']]
            df_dict[matches[1]['key']['dfName']]['visted'].add(matches[1]['key']['index'])
        rows.append(constructRowDict(row0, row1, row2, from_index, accuracy, timeThreshold))
    return pd.DataFrame(rows)

def generateQualityControlDataFrame(refDF, dflist, accuracy, timeThreshold):
    """Generate quality control DataFrame from reference DataFrame and dataframes."""
    rows = []
    df0, df1, df2 = dflist[0], dflist[1], dflist[2]
    
    for index in refDF.itertuples():
        # A to B and A to C
        row0 = df0.iloc[index.Index]
        # B to A and B to C
        index1 = int(refDF.iloc[index.Index].index1)
        index2 = int(refDF.iloc[index.Index].index2)
        if index1 == -1 and index.score3 != -1:
          index1 = int(refDF.iloc[index.Index].index1_bc)
        if index2 == -1 and index.score3 != -1:
          index2 = int(refDF.iloc[index.Index].index2_bc)
        
        if index1 == -1 and index2 == -1:
            continue
        if index1 == -1 or index1 >= len(df1):
            # No match found or invalid index, use first row as fallback (will be handled in comparison)
            index1 = 0 if len(df1) > 0 else -1
        row1 = df1.iloc[index1] if index1 != -1 else df0.iloc[index.Index]
        # C to A and C to B
        if index2 == -1 or index2 >= len(df2):
            # No match found or invalid index, use first row as fallback (will be handled in comparison)
            index2 = 0 if len(df2) > 0 else -1
        row2 = df2.iloc[index2] if index2 != -1 else df0.iloc[index.Index]

        rows.append(constructRowDict(row0, row1, row2, index.Index, accuracy, timeThreshold))
    
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

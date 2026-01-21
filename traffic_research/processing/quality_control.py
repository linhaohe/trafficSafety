"""Quality control functions for generating and testing data quality."""

import pandas as pd
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from traffic_research.core.data_engineering import DataEngining, float_cols
from traffic_research.core.matching import compareParameters, compareTimeDistance
from traffic_research.core.utils import enumToString, secondsToTimeString
from config import EXCLUDED_FROM_ACCURACY, DEFAULT_TIME_THRESHOLD


def constructRowDict(row0, row1, row2, index, accuracy, percentageThreshold, timeThreshold):
    """Construct a row dictionary by comparing three reviewer rows."""
    def compare(field):
        return compareParameters(row0, row1, row2, field, accuracy, percentageThreshold)
    
    def compareTime(field):
        return compareTimeDistance(row0['row'][field], row1['row'][field], row2['row'][field], accuracy, timeThreshold)
    
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
    crosswalkCrossing = enumToStr('Crosswalk Crossing?', DataEngining.boolean)
    pedestrianPhaseCrossing = enumToStr('Pedestrian Phase Crossing?', DataEngining.boolean)
    finishedDuringPedsPhase = enumToStr('Did User Finish Crossing During Pedestrian Phase?', DataEngining.boolean)
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
        "Crosswalk Crossing?": crosswalkCrossing,
        "Pedestrian Phase Crossing?": pedestrianPhaseCrossing,
        "Intend to Cross Timestamp": intendToCrossTimestamp,
        "Crossing Start Time": crossingStartTime,
        "Refuge Island Start Time": refugeIslandStartTime,
        "Refuge Island End Time": refugeIslandEndTime,
        "Did User Finish Crossing During Pedestrian Phase?": finishedDuringPedsPhase,
        "Crossing End Time": crossingEndTime,
        "Crossing Interaction Notes": walkingInteraction,
        "Bus Presence": busPresence,
        "Crossing Location Relative to Bus": crossingLocationToBus,
        "Crossing Location Relative to Bus Stop": crossingLocationRelativeToBusStop,
        "Bus Noteworthy Events": '0',
        "Vehicle Traffic": trafficCondition,
        "General Reviewer Notes": '0'
    }


def generateQualityControlDataFrame(refDF, dflist, accuracy, percentageThreshold, timeThreshold):
    """Generate quality control DataFrame from reference DataFrame and dataframes."""
    rows = []
    df0, df1, df2 = dflist[0], dflist[1], dflist[2]
    
    for index in refDF.itertuples():
        # A to B and A to C
        row0 = {
            "row": df0.iloc[index.Index],
            "score": [index.score1, index.score2]
        }
        # B to A and B to C
        index1 = int(refDF.iloc[index.Index].index1)
        index2 = int(refDF.iloc[index.Index].index2)
        # index1_bc = int(refDF.iloc[index.Index].index1_bc)
        # index2_bc = int(refDF.iloc[index.Index].index2_bc)
        # if index1 == -1 and index2 == -1 and index1_bc == -1 and index2_bc == -1:
        #     continue
        
        if index1 == -1 or index1 >= len(df1):
            # No match found or invalid index, use first row as fallback (will be handled in comparison)
            index1 = 0 if len(df1) > 0 else -1
        row1 = {
            "row": df1.iloc[index1] if index1 != -1 else df0.iloc[index.Index],  # Fallback to row0 if no match
            "score": [index.score1, index.score3]
        }
        # C to A and C to B
        if index2 == -1 or index2 >= len(df2):
            # No match found or invalid index, use first row as fallback (will be handled in comparison)
            index2 = 0 if len(df2) > 0 else -1
        row2 = {
            "row": df2.iloc[index2] if index2 != -1 else df0.iloc[index.Index],  # Fallback to row0 if no match
            "score": [index.score2, index.score3]
        }
        rows.append(constructRowDict(row0, row1, row2, index.Index, accuracy, percentageThreshold, timeThreshold))
    
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

import pandas as pd
import math
import os
from dataEngining import generateDateFrameList, generateDateFrame, float_cols
from dataEngining import DataEngining

class AccuracyScore:
    def __init__(self, nofVisitedCell=0, nofDifferent=0):
        self.nofVisitedCell = nofVisitedCell
        self.nofDifferent = nofDifferent
        self.filesAccuracy = []
    
    def update(self, visitedCells, differentCells):
        self.nofVisitedCell += visitedCells
        self.nofDifferent += differentCells

    def getAccuracy(self):
        if self.nofVisitedCell == 0:
            return 0.0
        return 1.0 - (self.nofDifferent / self.nofVisitedCell)
    
    def getFilesAccuracy(self):
        return self.filesAccuracy
    
    def appendFileAccuracy(self, fileName, accuracy):
        self.filesAccuracy.append({
            'Location': fileName,
            'Accuracy': accuracy
        })
    
    def reset(self):
        self.nofVisitedCell = 0
        self.nofDifferent = 0
    
def calculateNumericScore(num1, num2, threshold):
    return math.exp(-abs(num1 - num2) / threshold)


def calculateConditionScore(condition1, condition2):
    """Calculate boolean condition score: 1.0 if match, 0.0 otherwise."""
    return 1.0 if condition1 == condition2 else 0.0

# Scoring weights
TIME_SCORE_WEIGHT = 0.0714
CONDITION_SCORE_WEIGHT = 0.05556
DEFAULT_PERCENTAGE_THRESHOLD = 0.8
DEFAULT_TIME_THRESHOLD = 3


def computeTimeScore(row1, row2, threshold=DEFAULT_TIME_THRESHOLD):
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

def generateReferenceDataFrame(dflist, timeThreshold):
    """Generate reference DataFrame by matching rows across three dataframes.
    
    Compares:
    - A to B (df0 to df1): finds best match in B for each row in A
    - A to C (df0 to df2): finds best match in C for each row in A
    - B to C (df1 to df2): finds best match in C for each row in B
    """
    rows = []
    df0, df1, df2 = dflist[0], dflist[1], dflist[2]

    # Compare B to C (df1 to df2) - independent comparison for all rows in B
    bc_matches = {}
    for row1 in df1.itertuples():
        maxScore3, maxIndex3 = -1.0, -1
        for row2 in df2.itertuples():
            score = computeFeatureScores(df1.iloc[row1.Index], df2.iloc[row2.Index], timeThreshold)
            if score > maxScore3:
                maxScore3, maxIndex3 = score, row2.Index
        bc_matches[row1.Index] = {
            "index2_bc": maxIndex3,
            "score3": maxScore3
        }

    # Compare A to B and A to C, and include B to C match
    for row in df0.itertuples():
        maxScore1, maxIndex1 = -1.0, -1
        maxScore2, maxIndex2 = -1.0, -1

        # Compare A to B (df0 to df1)
        for row1 in df1.itertuples():
            score = computeFeatureScores(df0.iloc[row.Index], df1.iloc[row1.Index], timeThreshold)
            if score > maxScore1:
                maxScore1, maxIndex1 = score, row1.Index

        # Compare A to C (df0 to df2)
        for row2 in df2.itertuples():
            score = computeFeatureScores(df0.iloc[row.Index], df2.iloc[row2.Index], timeThreshold)
            if score > maxScore2:
                maxScore2, maxIndex2 = score, row2.Index

        # Get B->C match for the matched row in B
        if maxIndex1 in bc_matches:
            bc_match = bc_matches[maxIndex1]
            index2_bc = bc_match["index2_bc"]
            score3 = bc_match["score3"]
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
def compareParameters(A, B, C, accuracy, percentageThreshold=0):
    """Compare three parameter values and update accuracy tracking.
    
    Returns the agreed value if at least two match, None otherwise.
    """
    if A == B or A == C or B == C:
        # If all are same
        if A == B and A == C:
            accuracy.update(3, 0)
            return A
        # If two are same
        accuracy.update(3, 1)
        return A
    # If all are different
    accuracy.update(3, 3)
    return None

def compareTimeDistance(timeA, timeB, timeC, accuracy, threshold=DEFAULT_TIME_THRESHOLD):
    """Compare three time values and return the one with minimum average distance."""
    distAB = abs(timeA - timeB)
    distAC = abs(timeA - timeC)
    distBC = abs(timeB - timeC)
    avgA = (distAB + distAC) / 2
    avgB = (distAB + distBC) / 2
    avgC = (distAC + distBC) / 2
    
    if distAB <= threshold or distAC <= threshold or distBC <= threshold:
        if distAB <= threshold and distAC <= threshold and distBC <= threshold:
            # If all are same
            accuracy.update(3, 0)
        else:
            # If two are same
            accuracy.update(3, 1)
    else:
        # If all are different
        accuracy.update(3, 3)

    if avgA <= avgB and avgA <= avgC:
        return timeA
    elif avgB <= avgA and avgB <= avgC:
        return timeB
    return timeC

def secondsToTimeString(seconds):
    """Convert seconds to HH:MM:SS format."""
    if seconds is None or seconds < 0:
        return "N/A"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{int(hours):02}:{int(minutes):02}:{int(secs):02}"


def enumToString(enumVal, enumList):
    """Convert enum value to string representation."""
    if enumVal is None or enumVal == -1:
        return ""
    try:
        return enumList(enumVal).name
    except:
        return ""
    
def constructRowDict(row0, row1, row2, index, accuracy):
    """Construct a row dictionary by comparing three reviewer rows."""
    def compare(field):
        return compareParameters(row0[field], row1[field], row2[field], accuracy)
    
    def compareTime(field):
        return compareTimeDistance(row0[field], row1[field], row2[field], accuracy)
    
    def enumToStr(field, enumType):
        return enumToString(compare(field), enumType)
    
    videoTitle = compare('Video Title')
    locationName = compare('Location Name')
    busStopIDs = compare('Bus Stop IDs/Addresses')
    busStopRouteCount = compare('Count of Bus Stop Routes')
    crosswalkLocationRelativeToBusStop = compare('Crosswalk Location Relative to Bus Stop')
    crossingTreatment = compare('Crossing Treatment')
    refugeIsland = enumToStr('Refuge Island', DataEngining.boolean)
    userCount = index + 1
    userType = enumToStr('User Type', DataEngining.userType)
    groupSize = compare('Group Size')
    estimatedGender = enumToStr('Estimated Gender', DataEngining.gender)
    estimatedAgeGroup = compare('Estimated Age Group')
    clothingColor = enumToStr('Clothing Color', DataEngining.clothingColor)
    visibilityScale = compare('Visibility Scale')
    estimatedVisibleDistraction = enumToStr('Estimated Visible Distrction', DataEngining.boolean)
    userNotes = compare('User Notes')
    busInteraction = enumToStr('Bus Interaction', DataEngining.boolean)
    roadwayCrossing = enumToStr('Roadway Crossing', DataEngining.boolean)
    typeOfBusInteraction = enumToStr('Type of Bus Interaction', DataEngining.busInteractions)
    busArrivalTime = secondsToTimeString(compareTime('Bus Stop Arrival Time'))
    busDepartureTime = secondsToTimeString(compareTime('Bus Stop Departure Time'))
    crosswalkCrossing = enumToStr('Crosswalk Crossing?', DataEngining.boolean)
    pedestrianPhaseCrossing = enumToStr('Pedestrian Phase Crossing?', DataEngining.boolean)
    intendToCrossTimestamp = secondsToTimeString(compareTime('Intend to Cross Timestamp'))
    crossingStartTime = secondsToTimeString(compareTime('Crossing Start Time'))
    refugeIslandStartTime = secondsToTimeString(compareTime('Refuge Island Start Time'))
    refugeIslandEndTime = secondsToTimeString(compareTime('Refuge Island End Time'))
    finishedDuringPedsPhase = enumToStr('Did User Finish Crossing During Pedestrian Phase?', DataEngining.boolean)
    crossingEndTime = secondsToTimeString(compareTime('Crossing End Time'))
    walkingInteraction = enumToStr('Crossing Interaction Notes', DataEngining.walkInteractions)
    busPresence = enumToStr('Bus Presence', DataEngining.boolean)
    crossingLocationToBus = enumToStr('Crossing Location Relative to Bus', DataEngining.crossingLocationRelativeToBus)
    crossingLocationRelativeToBusStop = enumToStr('Crossing Location Relative to Bus Stop', DataEngining.crossingLocationRelativeToBusStop)
    trafficCondition = enumToStr('Vehicle Traffic', DataEngining.trafficVolume)
    return {
        "Video Title": "" if videoTitle == -1 else videoTitle,
        'Initials': '',
        "Location Name": "" if locationName == -1 else locationName,
        "Bus Stop IDs/Addresses": "" if busStopIDs == -1 else busStopIDs,
        "Count of Bus Stop Routes": busStopRouteCount,
        "Crosswalk Location Relative to Bus Stop": crosswalkLocationRelativeToBusStop,
        "Crossing Treatment": crossingTreatment,
        "Refuge Island": refugeIsland,
        "User Count": "" if userCount == -1 else userCount,
        "User Type": userType,
        "Group Size": "" if groupSize == -1 else str(groupSize),
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

def generateQualityControlDataFrame(refDF, dflist, accuracy):
    """Generate quality control DataFrame from reference DataFrame and dataframes."""
    rows = []
    df0, df1, df2 = dflist[0], dflist[1], dflist[2]
    
    for index in refDF.itertuples():
        row0 = df0.iloc[index.Index]
        row1 = df1.iloc[int(refDF.iloc[index.Index].index1)]
        row2 = df2.iloc[int(refDF.iloc[index.Index].index2)]
        rows.append(constructRowDict(row0, row1, row2, index.Index, accuracy))
    
    return pd.DataFrame(rows)

def accuracyTest(humanQualityDF, computedQualityDF):
    """Test accuracy by comparing human quality control with computed results."""
    correctCount = 0
    indexCount = 0
    rowCount = 0
    
    for row in humanQualityDF.itertuples():
        if len(computedQualityDF) > rowCount:
            humanRow = humanQualityDF.iloc[row.Index]
            computedRow = computedQualityDF.iloc[row.Index]
            
            for col in humanQualityDF.columns:
                # Check for exact match or both are null/zero
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
            rowCount += 1
        else:
            break
    
    # Account for remaining rows
    if len(humanQualityDF) - rowCount > 0:
        indexCount += (len(humanQualityDF) - rowCount) * humanQualityDF.shape[1]
    elif len(computedQualityDF) - rowCount > 0:
        indexCount += (len(computedQualityDF) - rowCount) * computedQualityDF.shape[1]
    
    return correctCount / indexCount if indexCount > 0 else 0.0

def computeTrafficData(fileList, accuracy, timeThreshold):
    """Compute traffic data from file list and generate quality control DataFrame."""
    dflist = generateDateFrameList(fileList)
    refDF = generateReferenceDataFrame(dflist, timeThreshold)
    dfQualityControl = generateQualityControlDataFrame(refDF, dflist, accuracy)
    dfQualityControl = dfQualityControl.transpose()
    return dfQualityControl, refDF

def computeDataFolderToCSV(resourceFolderPath, outputFolderPath, timeThreshold=DEFAULT_TIME_THRESHOLD,percentageThreshold=DEFAULT_PERCENTAGE_THRESHOLD):
    """Process all folders in resource path and generate CSV outputs."""
    accuracy = AccuracyScore()
    
    for fileFolder in os.listdir(resourceFolderPath):
        filePath = os.path.join(resourceFolderPath, fileFolder)
        if os.path.isdir(filePath):
            fileList = [
                os.path.join(filePath, filename)
                for filename in os.listdir(filePath)
                if filename.endswith(".csv")
            ]
            
            dfQualityControl, refDF = computeTrafficData(fileList, accuracy, timeThreshold)
            accuracy.appendFileAccuracy(fileFolder, accuracy.getAccuracy())
            accuracy.reset()
            
            dfQualityControl.to_csv(
                os.path.join(outputFolderPath, fileFolder) + '.csv', 
                index=True, 
                header=False
            )
            refDF.to_csv(
                os.path.join(outputFolderPath, fileFolder) + '+refDF.csv', 
                index=True, 
                header=True
            )
    
    accuracyDF = pd.DataFrame(accuracy.getFilesAccuracy(), columns=['Location', 'Accuracy'])
    accuracyDF.to_csv(os.path.join(outputFolderPath, 'accuracy_summary.csv'), header=True)


def performAccuracyTest(outputFile, humanQualityFile):
    """Perform accuracy test comparing computed output with human quality control."""
    dfCompute = generateDateFrame(outputFile).dropna(how='all')
    dfHuman = generateDateFrame(humanQualityFile).dropna(how='all')
    accuracy = accuracyTest(dfHuman, dfCompute)
    print(f"Accuracy: {accuracy*100:.2f}%")
    
if __name__ == "__main__":
    # print(DataEngining.parseTimeObject('15:20:20PM'))
    computeDataFolderToCSV('./resource/inputData', './output')
    performAccuracyTest('./output/Northampton_Court_House_V43.csv', 
                        './resource/human_quality_control/Norhampton_Court_House.csv')


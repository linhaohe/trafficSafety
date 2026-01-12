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
OVERALL_TIME_SCORE_WEIGHT = 0.55
TIME_SCORE_WEIGHT = OVERALL_TIME_SCORE_WEIGHT/7
DEFAULT_TIME_THRESHOLD_Weight = 10
CONDITION_SCORE_WEIGHT = (1-OVERALL_TIME_SCORE_WEIGHT)/10
DEFAULT_PERCENTAGE_THRESHOLD = 0.8
DEFAULT_TIME_THRESHOLD = 3


def computeTimeScore(row1, row2, threshold=DEFAULT_TIME_THRESHOLD_Weight):
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
    # Note: This is done first but rows are only marked as visited when used in A->B->C chain
    bc_matches = {}
    for row1 in df1.itertuples():
        maxScore3, maxIndex3 = -1.0, -1
        for row2 in df2.itertuples():
            # Skip if row2 is already visited
            if row2.Index in visited_c:
                continue
            score = computeFeatureScores(df1.iloc[row1.Index], df2.iloc[row2.Index], timeThreshold)
            # Only consider if score is above threshold
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
            # Only consider if score is above threshold
            if score >= percentageThreshold and score > maxScore1:
                maxScore1, maxIndex1 = score, row1.Index

        # Compare A to C (df0 to df2) - skip visited rows
        for row2 in df2.itertuples():
            if row2.Index in visited_c:
                continue
            score = computeFeatureScores(df0.iloc[row.Index], df2.iloc[row2.Index], timeThreshold)
            # Only consider if score is above threshold
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
            
            # Mark the B->C matched row in C as visited if it's different from A->C match
            # and score is above threshold
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
    
    Note: Certain parameters are excluded from accuracy tracking:
    - Video Title
    - Initials
    - Location Name
    - Count of Bus Stop Routes
    """
    # Parameters excluded from accuracy tracking
    EXCLUDED_FROM_ACCURACY = {
        'Video Title',
        'Initials',
        'Location Name',
        'Count of Bus Stop Routes'
    }
    
    A = row0['row'][fieldName]
    B = row1['row'][fieldName]
    C = row2['row'][fieldName]
    
    # Extract similarity scores
    scoreAB = row0['score'][0]  # A to B score
    scoreAC = row0['score'][1]  # A to C score
    scoreBC = row1['score'][1]  # B to C score (fixed: was row1['score'][0])
    
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
        # Return the value that appears in at least two matches
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
        # Return the one with minimum average distance
        if avgA <= avgB and avgA <= avgC:
            return timeA
        elif avgB <= avgA and avgB <= avgC:
            return timeB
        else:
            return timeC
    
    # If at least one pair matches
    if matchAB or matchAC or matchBC:
        accuracy.update(3, 1)
        
        # If A-B match, return the one with smaller average distance
        if matchAB:
            return timeA if avgA <= avgB else timeB
        
        # If A-C match, return the one with smaller average distance
        if matchAC:
            return timeA if avgA <= avgC else timeC
        
        # If B-C match, return the one with smaller average distance
        if matchBC:
            return timeB if avgB <= avgC else timeC
    
    # If all are different (no pairs within threshold)
    accuracy.update(3, 3)
    return -1

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
        # Special handling for AgeGroup to return readable format
        if enumList == DataEngining.ageGroup or enumList == DataEngining.AgeGroup:
            age_mapping = {
                0: "0-20",   # age_0_20
                1: "21-35",  # age_21_35
                2: "36-50",  # age_36_50
                3: ">50",    # age_50_plus
                -1: ""       # other
            }
            return age_mapping.get(enumVal, "")
        
        # Standard enum conversion
        enum_member = enumList(enumVal)
        return enum_member.name
    except:
        return ""
    
def constructRowDict(row0, row1, row2, index, accuracy, percentageThreshold, timeThreshold):
    """Construct a row dictionary by comparing three reviewer rows."""
    def compare(field):
        return compareParameters(row0, row1, row2, field, accuracy, percentageThreshold)
    
    def compareTime(field):
        return compareTimeDistance(row0['row'][field], row1['row'][field], row2['row'][field], accuracy,timeThreshold)
    
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
    estimatedAgeGroup = enumToStr('Estimated Age Group',DataEngining.ageGroup)
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

def generateQualityControlDataFrame(refDF, dflist, accuracy, percentageThreshold, timeThreshold):
    """Generate quality control DataFrame from reference DataFrame and dataframes."""
    rows = []
    df0, df1, df2 = dflist[0], dflist[1], dflist[2]
    
    for index in refDF.itertuples():
        #A to B and A to C
        row0 = {
            "row":df0.iloc[index.Index],
            "score":[index.score1,index.score2]
            }
        #B to A and B to C
        row1 = {
            "row":df1.iloc[int(refDF.iloc[index.Index].index1)],
            "score":[index.score1,index.score3]
        }
        #C to A and C to B
        row2 = {
            "row":df2.iloc[int(refDF.iloc[index.Index].index2)],
            "score":[index.score2,index.score3]
        }
        rows.append(constructRowDict(row0, row1, row2, index.Index, accuracy, percentageThreshold, timeThreshold))
    
    return pd.DataFrame(rows)

def accuracyTest(humanQualityDF, computedQualityDF):
    """Test accuracy by comparing human quality control with computed results.
    
    Handles cases where:
    - Human quality DF is missing columns (only compares common columns)
    - Computed quality DF is missing columns (treats as mismatch for those columns)
    
    Note: Certain parameters are excluded from accuracy tracking:
    - Video Title
    - Initials
    - Location Name
    - Count of Bus Stop Routes
    """
    # Parameters excluded from accuracy tracking
    EXCLUDED_FROM_ACCURACY = {
        'Video Title',
        'Initials',
        'Location Name',
        'Count of Bus Stop Routes'
    }
    
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
            
            # Count missing columns in computed DF as mismatches (not correct, but counted)
            # Only count non-excluded missing columns
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

def computeTrafficData(fileList, accuracy, percentageThreshold, timeThreshold):
    """Compute traffic data from file list and generate quality control DataFrame."""
    dflist = generateDateFrameList(fileList)
    refDF = generateReferenceDataFrame(dflist, timeThreshold, percentageThreshold)
    dfQualityControl = generateQualityControlDataFrame(refDF, dflist, accuracy, percentageThreshold, timeThreshold)
    dfQualityControl = dfQualityControl.transpose()
    return dfQualityControl, refDF
def _processFolder(filePath, outputFolderPath, accuracy, percentageThreshold, timeThreshold):
    """Helper function to process a single folder and generate CSV outputs."""
    fileList = [
        os.path.join(filePath, filename)
        for filename in os.listdir(filePath)
        if filename.endswith(".csv")
    ]
    
    dfQualityControl, refDF = computeTrafficData(fileList, accuracy, percentageThreshold, timeThreshold)
    accuracy.appendFileAccuracy(os.path.basename(filePath), accuracy.getAccuracy())
    accuracy.reset()
    
    folderName = os.path.basename(filePath)
    dfQualityControl.to_csv(
        os.path.join(outputFolderPath, folderName) + '.csv', 
        index=True, 
        header=False
    )
    refDF.to_csv(
        os.path.join(outputFolderPath, folderName) + '+refDF.csv', 
        index=True, 
        header=True
    )


def computeDataFolderToCSV(resourceFolderPath, outputFolderPath, percentageThreshold, timeThreshold):
    """Process all folders in resource path and generate CSV outputs."""
    accuracy = AccuracyScore()
    
    for fileFolder in os.listdir(resourceFolderPath):
        filePath = os.path.join(resourceFolderPath, fileFolder)
        if os.path.isdir(filePath):
            _processFolder(filePath, outputFolderPath, accuracy, percentageThreshold, timeThreshold)
    
    accuracyDF = pd.DataFrame(accuracy.getFilesAccuracy(), columns=['Location', 'Accuracy'])
    accuracyDF.to_csv(os.path.join(outputFolderPath, 'accuracy_summary.csv'), header=True)


def computeDataFolderToCSVWithIndex(resourceFolderPath, outputFolderPath, percentageThreshold, timeThreshold, threshold_name, index):
    """Process all folders in resource path and generate CSV outputs with index for batch processing.
    
    Args:
        resourceFolderPath: Path to input data folders
        outputFolderPath: Path to output directory
        percentageThreshold: Percentage threshold value
        timeThreshold: Time threshold value
        threshold_name: Name identifier for the threshold type (e.g., 'percentage', 'time')
        index: Index value for batch processing
    """
    accuracy = AccuracyScore()
    
    for fileFolder in os.listdir(resourceFolderPath):
        filePath = os.path.join(resourceFolderPath, fileFolder)
        if os.path.isdir(filePath):
            _processFolder(filePath, outputFolderPath, accuracy, percentageThreshold, timeThreshold)
    
    # Ensure accuracy_summary directory exists
    accuracySummaryDir = os.path.join(outputFolderPath, 'accuracy_summary')
    os.makedirs(accuracySummaryDir, exist_ok=True)
    
    accuracyDF = pd.DataFrame(accuracy.getFilesAccuracy(), columns=['Location', 'Accuracy'])
    accuracyDF.to_csv(
        os.path.join(accuracySummaryDir, f'accuracy_summary_{threshold_name}_{index}.csv'),
        header=True
    )
    
def performAccuracyTest(outputFile, humanQualityFile):
    """Perform accuracy test comparing computed output with human quality control."""
    dfCompute = generateDateFrame(outputFile).dropna(how='all')
    dfHuman = generateDateFrame(humanQualityFile).dropna(how='all')
    accuracy = accuracyTest(dfHuman, dfCompute)
    print(f"Accuracy: {accuracy*100:.2f}%")
    return accuracy

# Constants for graph data generation
INPUT_DATA_PATH = './resource/inputData'
OUTPUT_PATH = './output'
ACCURACY_SUMMARY_DIR = os.path.join(OUTPUT_PATH, 'accuracy_summary')
HUMAN_QC_PATH = './resource/human_quality_control'

NORTHAMPTON_OUTPUT = os.path.join(OUTPUT_PATH, 'Northampton_Court_House_V43.csv')
NORTHAMPTON_HUMAN_QC = os.path.join(HUMAN_QC_PATH, 'Norhampton_Court_House.csv')
BELMONT_OUTPUT = os.path.join(OUTPUT_PATH, 'Belmont+Edward_St_V38.csv')
BELMONT_HUMAN_QC = os.path.join(HUMAN_QC_PATH, 'Belmont_St+Edward_St.csv')


def _generateGraphDataHelper(
    start_index, 
    end_index, 
    percentage_threshold, 
    time_threshold, 
    threshold_name,
    get_threshold_value
):
    """Helper function to generate graph data by testing different threshold values.
    
    Args:
        start_index: Starting index for iteration
        end_index: Ending index for iteration (exclusive)
        percentage_threshold: Percentage threshold value (constant or None if varying)
        time_threshold: Time threshold value (constant or None if varying)
        threshold_name: Name identifier for the threshold being tested
        get_threshold_value: Function that takes index and returns (pct_threshold, time_threshold) tuple
    
    Returns:
        Tuple of (accuracy_rows1, accuracy_rows2) DataFrames
    """
    accuracy_rows1 = []
    accuracy_rows2 = []
    
    # Ensure output directory exists
    os.makedirs(ACCURACY_SUMMARY_DIR, exist_ok=True)
    
    for i in range(start_index, end_index):
        pct_thresh, time_thresh = get_threshold_value(i)
        
        computeDataFolderToCSVWithIndex(
            INPUT_DATA_PATH,
            OUTPUT_PATH,
            percentageThreshold=pct_thresh,
            timeThreshold=time_thresh,
            threshold_name=threshold_name,
            index=i
        )
        
        accuracy1 = performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
        accuracy2 = performAccuracyTest(BELMONT_OUTPUT, BELMONT_HUMAN_QC)
        
        accuracy_rows1.append({
            'Percentage Threshold': pct_thresh,
            'Time Threshold': time_thresh,
            'Data Set': 'Northampton_Court_House',
            'Accuracy': accuracy1
        })
        accuracy_rows2.append({
            'Percentage Threshold': pct_thresh,
            'Time Threshold': time_thresh,
            'Data Set': 'Belmont_St+Edward_St',
            'Accuracy': accuracy2
        })
    
    # Save results
    df1 = pd.DataFrame(accuracy_rows1)
    df2 = pd.DataFrame(accuracy_rows2)
    
    df1.to_csv(
        os.path.join(ACCURACY_SUMMARY_DIR, f'accuracy_summary_{threshold_name}_Northampton_Court_House.csv'),
        header=True
    )
    df2.to_csv(
        os.path.join(ACCURACY_SUMMARY_DIR, f'accuracy_summary_{threshold_name}_Belmont_St+Edward_St.csv'),
        header=True
    )
    
    return df1, df2


def generateGraphDataPercentage(start_percent=1, end_percent=2, time_threshold=3, threshold_name='percentage'):
    """Generate graph data by testing different percentage thresholds.
    
    Args:
        start_percent: Starting percentage threshold (multiplied by 0.01)
        end_percent: Ending percentage threshold (multiplied by 0.01, exclusive)
        time_threshold: Time threshold to use for all tests (constant)
        threshold_name: Name identifier for output files
    """
    def get_threshold_value(i):
        return (0.01 * i, time_threshold)
    
    return _generateGraphDataHelper(
        start_percent,
        end_percent,
        None,  # percentage varies
        time_threshold,
        threshold_name,
        get_threshold_value
    )


def generateGraphDataTime(start_time=1, end_time=2, percentage_threshold=0.64, threshold_name='time'):
    """Generate graph data by testing different time thresholds.
    
    Args:
        start_time: Starting time threshold
        end_time: Ending time threshold (exclusive)
        percentage_threshold: Percentage threshold to use for all tests (constant)
        threshold_name: Name identifier for output files
    """
    def get_threshold_value(i):
        return (percentage_threshold, i)
    
    return _generateGraphDataHelper(
        start_time,
        end_time,
        percentage_threshold,
        None,  # time varies
        threshold_name,
        get_threshold_value
    )
def graphData():
    """Generate all accuracy comparison graphs."""
    import matplotlib.pyplot as plt
        
    def _loadIteratedAccuracyData(start, end, file_pattern, x_col_name, x_multiplier=1):
        """Helper to load iterated accuracy data from CSV files."""
        rows1 = []
        rows2 = []
        for i in range(start, end):
            file_path = os.path.join(ACCURACY_SUMMARY_DIR, file_pattern.format(i))
            if not os.path.exists(file_path):
                continue
            df_data = pd.read_csv(file_path).dropna(how='all')
            if len(df_data) >= 2:
                rows1.append({
                    'Data Set': 'Northampton_Court_House',
                    'Accuracy': df_data.iloc[0]['Accuracy'],
                    x_col_name: i * x_multiplier
                })
                rows2.append({
                    'Data Set': 'Belmont_St+Edward_St',
                    'Accuracy': df_data.iloc[1]['Accuracy'],
                    x_col_name: i * x_multiplier
                })
        return pd.DataFrame(rows1), pd.DataFrame(rows2)
    
    def _plotAccuracyComparison(df1, df2, x_col, title, xlabel, ylabel, output_file):
        """Helper to plot accuracy comparison between two datasets."""
        plt.plot(df1[x_col], df1['Accuracy'], label='Northampton_Court_House')
        plt.plot(df2[x_col], df2['Accuracy'], label='Belmont_St+Edward_St')
        plt.legend()
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.savefig(output_file)
        plt.clf()
    
    def graphIteratedAccuracyPercentageThreshold():
        """Graph consistency test of percentage threshold."""
        df1, df2 = _loadIteratedAccuracyData(1, 101, 'accuracy_summary_percentage_{}.csv', 'Percentage Threshold', 0.01)
        _plotAccuracyComparison(
            df1, df2, 'Percentage Threshold',
            "Consistency test of percentage threshold, t = 3s",
            "Percentage Threshold(%)",
            "Iterated Accuracy(%)",
            os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_percentage_threshold.png')
        )
    
    def graphPercentageThresholdVsHumanAccuracy():
        """Graph percentage threshold vs human accuracy."""
        df1 = pd.read_csv(os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_percentage_Northampton_Court_House.csv'))
        df2 = pd.read_csv(os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_percentage_Belmont_St+Edward_St.csv'))
        _plotAccuracyComparison(
            df1, df2, 'Percentage Threshold',
            "Accuracy test of percentage threshold vs human accuracy, t = 3s",
            "Percentage Threshold(%)",
            "Similarity(%)",
            os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_percentage_threshold_vs_human_accuracy.png')
        )
    
    def graphIteratedAccuracyTimeThreshold():
        """Graph consistency test of time threshold."""
        rows1 = []
        rows2 = []
        for i in range(1, 20):
            file_path = os.path.join(ACCURACY_SUMMARY_DIR, f'accuracy_summary_time_{i}.csv')
            if not os.path.exists(file_path):
                continue
            df_data = pd.read_csv(file_path).dropna(how='all')
            if len(df_data) >= 2:
                rows1.append({
                    'Data Set': 'Northampton_Court_House',
                    'Accuracy': df_data.iloc[0]['Accuracy'],
                    'Time Threshold': i
                })
                rows2.append({
                    'Data Set': 'Belmont_St+Edward_St',
                    'Accuracy': df_data.iloc[1]['Accuracy'],
                    'Time Threshold': i
                })
        df1 = pd.DataFrame(rows1)
        df2 = pd.DataFrame(rows2)
        _plotAccuracyComparison(
            df1, df2, 'Time Threshold',
            "Consistency test of time threshold, % = 0.64",
            "Time Threshold(seconds)",
            "Iterated Accuracy(%)",
            os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_time_summary_time_threshold.png')
        )
    
    def graphTimeThresholdVsHumanAccuracy():
        """Graph time threshold vs human accuracy."""
        df1 = pd.read_csv(os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_time_Northampton_Court_House.csv'))
        df2 = pd.read_csv(os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_time_Belmont_St+Edward_St.csv'))
        _plotAccuracyComparison(
            df1, df2, 'Time Threshold',
            "Accuracy test of time threshold vs human accuracy, % = 0.64",
            "Time Threshold(seconds)",
            "Similarity(%)",
            os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_time_summary_time_threshold_vs_human_accuracy.png')
        )
    
    graphIteratedAccuracyPercentageThreshold()
    graphPercentageThresholdVsHumanAccuracy()
    graphIteratedAccuracyTimeThreshold()
    graphTimeThresholdVsHumanAccuracy()
if __name__ == "__main__":
    # Generate graph data for testing different percentage thresholds
    # generateGraphDataPercentage(start_percent=1, end_percent= 101, time_threshold=3)
    # generateGraphDataTime(start_time=1, end_time=20, percentage_threshold=0.64)

    # Uncomment to generate graphs from existing data
    graphData()
    
    # Uncomment to run single computation and accuracy tests
    # computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH, percentageThreshold=0.64, timeThreshold=3)
    # performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
    # performAccuracyTest(BELMONT_OUTPUT, BELMONT_HUMAN_QC)
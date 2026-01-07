import pandas as pd
import math
import os
from dataEngining import generateDateFrameList
from dataEngining import DataEngining

class accuracyScore:
    def __init__(self,nofVistedCell = 0,nofDifferent = 0):
        self.nofVistedCell = nofVistedCell
        self.nofDifferent = nofDifferent
    
    def update(self,vistedCells, differentCells):
        self.nofVistedCell += vistedCells
        self.nofDifferent += differentCells

    def getAccuarcy(self):
        if self.nofVistedCell == 0:
            return 0.0
        return 1.0 - (self.nofDifferent / self.nofVistedCell)
    
def calculateNumericScore(num1, num2,threshold):
    numScore = math.exp(-abs(num1 - num2) / threshold)
    return numScore

#boolean types
def calculateConditionScore(condition1, condition2):
    if condition1 == condition2:
        return 1.0
    else:
        return 0.0

#weight 50%
def computeTimeScore(row1, row2):
    score = 0.0
    score += calculateNumericScore(row1['Crossing Start Time'], row2['Crossing Start Time'], 10)
    score += calculateNumericScore(row1['Bus Stop Arrival Time'], row2['Bus Stop Arrival Time'], 10)
    score += calculateNumericScore(row1['Bus Stop Departure Time'], row2['Bus Stop Departure Time'], 10)
    score += calculateNumericScore(row1['Intend to Cross Timestamp'], row2['Intend to Cross Timestamp'], 10)
    score += calculateNumericScore(row1['Refuge Island Start Time'], row2['Refuge Island Start Time'], 10)
    score += calculateNumericScore(row1['Refuge Island End Time'], row2['Refuge Island End Time'], 10)
    score += calculateNumericScore(row1['Crossing End Time'], row2['Crossing End Time'], 10)
    return score

#weight 25%
def computeConditionScore(row1, row2):
    score = 0.0
    score += calculateConditionScore(row1['User Type'], row2['User Type'])
    score += calculateConditionScore(row1['Estimated Gender'], row2['Estimated Gender'])
    score += calculateConditionScore(row1['Bus Interaction'], row2['Bus Interaction'])
    score += calculateConditionScore(row1['Roadway Crossing'], row2['Roadway Crossing'])
    score += calculateConditionScore(row1['Type of Bus Interaction'], row2['Type of Bus Interaction'])
    score += calculateConditionScore(row1['Crossing Interaction Notes'], row2['Crossing Interaction Notes'])
    score += calculateConditionScore(row1['Crossing Location Relative to Bus Stop'], row2['Crossing Location Relative to Bus Stop'])
    score += calculateConditionScore(row1['Vehicle Traffic'], row2['Vehicle Traffic'])
    return score

def computeNumericScore(row1, row2):
    score = 0.0
    score += calculateNumericScore(row1['Group Size'], row2['Group Size'], 2)
    return score

def computeFeatureScores(row1, row2):
    timeScore = computeTimeScore(row1, row2)
    conditionScore = computeConditionScore(row1, row2)
    score = timeScore * 0.0714 + conditionScore * 0.03125 + computeNumericScore(row1, row2) * 0.25
    return score

def generateReferenceDataFrame(dflist):
    # Initialize empty list to collect rows
    rows = []
    df0 = dflist[0]
    df1 = dflist[1]
    df2 = dflist[2]

    for row in df0.itertuples():
        maxScore1 = -1.0
        maxIndex1 = -1
        maxScore2 = -1.0
        maxIndex2 = -1

        # Compare with reviewer 1
        for row1 in df1.itertuples():
            score = computeFeatureScores(df0.iloc[row.Index], df1.iloc[row1.Index])
            if score > maxScore1:
                maxScore1 = score
                maxIndex1 = row1.Index

        # Compare with reviewer 2
        for row2 in df2.itertuples():
            score = computeFeatureScores(df0.iloc[row.Index], df2.iloc[row2.Index])
            if score > maxScore2:
                maxScore2 = score
                maxIndex2 = row2.Index

        # Append a dict (much cleaner than concat)
        rows.append({
            "index1": maxIndex1,
            "score1": maxScore1,
            "index2": maxIndex2,
            "score2": maxScore2
        })

    # Build DataFrame once at the end
    qualityDF = pd.DataFrame(rows)
    qualityDF = qualityDF.astype({"index1": "Int64", "score1": "float64", "index2": "Int64", "score2": "float64"})

    return qualityDF
def compareParamets(A, B, C, accuracy, precentageThreshold = 0): 
    if A == B or A == C or B == C:
        #if all are same
        if(A == B and A == C):
            accuracy.update(3,0)
            return A
        #if two are same
        accuracy.update(3,1)
        return A
    #if all are different
    accuracy.update(3,3)
    return None

def compareTimeDistance(timeA,timeB,timeC,accuracy, threshold = 3):
    distAB = abs(timeA - timeB)
    distAC = abs(timeA - timeC)
    distBC = abs(timeB - timeC)
    avgA = (distAB + distAC) / 2
    avgB = (distAB + distBC) / 2
    avgC = (distAC + distBC) / 2
    if(distAB <= threshold or distAC <= threshold or distBC <= threshold):
        if(distAB <= threshold and distAC <= threshold and distBC <= threshold):
            #if all are same
            accuracy.update(3,0)
        #if two are same
        accuracy.update(3,1)
    else:
        #if all are different
        accuracy.update(3,3)

    if(avgA <= avgB and avgA <= avgC):
        return timeA
    elif(avgB <= avgA and avgB <= avgC):
        return timeB
    return timeC

def secondsToTimeString(seconds):
    if seconds is None or seconds < 0:
        return "N/A"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{int(hours):02}:{int(minutes):02}:{int(secs):02}"

def enmumToString(enumVal, enumList): 
    if enumVal is None or enumVal == -1: 
        return "" 
    try: 
        return enumList(enumVal).name 
    except: 
        return ""
    
def constructRowDict(row0, row1, row2,index,accuracy):
    videoTitle = compareParamets(row0['Video Title'], row1['Video Title'], row2['Video Title'],accuracy)
    locationName = compareParamets(row0['Location Name'], row1['Location Name'], row2['Location Name'],accuracy)
    busStopIDs = compareParamets(row0['Bus Stop IDs/Addresses'], row1['Bus Stop IDs/Addresses'], row2['Bus Stop IDs/Addresses'],accuracy)
    busStopRouteCount = compareParamets(row0['Count of Bus Stop Routes'], row1['Count of Bus Stop Routes'], row2['Count of Bus Stop Routes'],accuracy)
    crosswalkLocationRelativeToBusStop = compareParamets(row0['Crosswalk Location Relative to Bus Stop'], row1['Crosswalk Location Relative to Bus Stop'], row2['Crosswalk Location Relative to Bus Stop'],accuracy)
    crossingTreatment = compareParamets(row0['Crossing Treatment'], row1['Crossing Treatment'], row2['Crossing Treatment'],accuracy)
    refugeIsland = enmumToString(compareParamets(row0['Refuge Island'], row1['Refuge Island'], row2['Refuge Island'],accuracy),DataEngining.boolean)
    userCount = index + 1
    userType = enmumToString(compareParamets(row0['User Type'], row1['User Type'], row2['User Type'],accuracy),DataEngining.userType)
    groupSize = compareParamets(row0['Group Size'], row1['Group Size'], row2['Group Size'],accuracy)
    estimatedGender = enmumToString(compareParamets(row0['Estimated Gender'], row1['Estimated Gender'], row2['Estimated Gender'],accuracy),DataEngining.gender)
    estimatedAgeGroup = compareParamets(row0['Estimated Age Group'], row1['Estimated Age Group'], row2['Estimated Age Group'],accuracy)
    clothingColor = enmumToString(compareParamets(row0['Clothing Color'], row1['Clothing Color'], row2['Clothing Color'],accuracy),DataEngining.clothingColor)
    visibilityScale = compareParamets(row0['Visibility Scale'], row1['Visibility Scale'], row2['Visibility Scale'],accuracy)
    estimatedvisibleDistrction = enmumToString(compareParamets(row0['Estimated Visible Distrction'], row1['Estimated Visible Distrction'], row2['Estimated Visible Distrction'],accuracy),DataEngining.boolean)
    userNotes = compareParamets(row0['User Notes'], row1['User Notes'], row2['User Notes'],accuracy)
    busInteraction = enmumToString(compareParamets(row0['Bus Interaction'], row1['Bus Interaction'], row2['Bus Interaction'],accuracy),DataEngining.boolean)
    roadwayCrossing = enmumToString(compareParamets(row0['Roadway Crossing'], row1['Roadway Crossing'], row2['Roadway Crossing'],accuracy),DataEngining.boolean)
    typeOfBusInteraction = enmumToString(compareParamets(row0['Type of Bus Interaction'], row1['Type of Bus Interaction'], row2['Type of Bus Interaction'],accuracy),DataEngining.busInteractions)
    busArrivalTime = secondsToTimeString(compareTimeDistance(row0['Bus Stop Arrival Time'], row1['Bus Stop Arrival Time'], row2['Bus Stop Arrival Time'],accuracy))
    busDepartureTime = secondsToTimeString(compareTimeDistance(row0['Bus Stop Departure Time'], row1['Bus Stop Departure Time'], row2['Bus Stop Departure Time'],accuracy))
    crosswalkCrossing = enmumToString(compareParamets(row0['Crosswalk Crossing?'], row1['Crosswalk Crossing?'], row2['Crosswalk Crossing?'],accuracy),DataEngining.boolean)
    pedsestrianPhaseCrossing = enmumToString(compareParamets(row0['Pedestrian Phase Crossing?'], row1['Pedestrian Phase Crossing?'], row2['Pedestrian Phase Crossing?'],accuracy),DataEngining.boolean)
    intendToCrossTimestamp = secondsToTimeString(compareTimeDistance(row0['Intend to Cross Timestamp'], row1['Intend to Cross Timestamp'], row2['Intend to Cross Timestamp'],accuracy))
    crossingStartTime = secondsToTimeString(compareTimeDistance(row0['Crossing Start Time'], row1['Crossing Start Time'], row2['Crossing Start Time'],accuracy))
    refugeIslandStartTime = secondsToTimeString(compareTimeDistance(row0['Refuge Island Start Time'], row1['Refuge Island Start Time'], row2['Refuge Island Start Time'],accuracy))
    refugeIslandEndTime = secondsToTimeString(compareTimeDistance(row0['Refuge Island End Time'], row1['Refuge Island End Time'], row2['Refuge Island End Time'],accuracy))
    finshedDuringPedsPhase = enmumToString(compareParamets(row0['Did User Finish Crossing During Pedestrian Phase?'], row1['Did User Finish Crossing During Pedestrian Phase?'], row2['Did User Finish Crossing During Pedestrian Phase?'],accuracy),DataEngining.boolean)
    crossingEndTime = secondsToTimeString(compareTimeDistance(row0['Crossing End Time'], row1['Crossing End Time'], row2['Crossing End Time'],accuracy))
    walkingInteraction = enmumToString(compareParamets(row0['Crossing Interaction Notes'], row1['Crossing Interaction Notes'], row2['Crossing Interaction Notes'],accuracy),DataEngining.walkInteractions)
    busPresence = enmumToString(compareParamets(row0['Bus Presence'], row1['Bus Presence'], row2['Bus Presence'],accuracy),DataEngining.boolean)
    crossingLocationToBus = enmumToString(compareParamets(row0['Crossing Location Relative to Bus'], row1['Crossing Location Relative to Bus'], row2['Crossing Location Relative to Bus'],accuracy),DataEngining.crossingLocationRelativeToBus)
    crossingLocationRelativeToBusStop = enmumToString(compareParamets(row0['Crossing Location Relative to Bus Stop'], row1['Crossing Location Relative to Bus Stop'], row2['Crossing Location Relative to Bus Stop'],accuracy),DataEngining.crossingLocationRelativeToBusStop)
    trafficCondition = enmumToString(compareParamets(row0['Vehicle Traffic'], row1['Vehicle Traffic'], row2['Vehicle Traffic'],accuracy),DataEngining.trafficVolume)
    return {
            "Video Title": "" if videoTitle == -1 else videoTitle,
            'Initials':'',
            "Location Name": "" if locationName == -1 else locationName,
            "Bus Stop IDs/Addresses":"" if busStopIDs == -1 else busStopIDs,
            "Count of Bus Stop Routes":busStopRouteCount,
            "Crosswalk Location Relative to Bus Stop":crosswalkLocationRelativeToBusStop,
            "Crossing Treatment":crossingTreatment,
            "Refuge Island":refugeIsland,
            "User Count":"" if userCount == -1 else userCount,
            "User Type":userType,
            "Group Size":"" if groupSize == -1 else str(groupSize),
            "Estimated Gender":estimatedGender,
            "Estimated Age Group":estimatedAgeGroup,
            "Clothing Color":clothingColor,
            "Visibility Scale":visibilityScale,
            "Estimated Visible Distrction":estimatedvisibleDistrction,
            "User Notes":userNotes,
            "Bus Interaction":busInteraction,
            "Roadway Crossing":roadwayCrossing,
            "Type of Bus Interaction":typeOfBusInteraction,
            "Bus Stop Arrival Time":busArrivalTime,
            "Bus Stop Departure Time":busDepartureTime,
            "Noteworthy Events":'0',
            "Crosswalk Crossing?":crosswalkCrossing,
            "Pedestrian Phase Crossing?":pedsestrianPhaseCrossing,
            "Intend to Cross Timestamp":intendToCrossTimestamp,
            "Crossing Start Time":crossingStartTime,
            "Refuge Island Start Time":refugeIslandStartTime,
            "Refuge Island End Time":refugeIslandEndTime,
            "Did User Finish Crossing During Pedestrian Phase?":finshedDuringPedsPhase,
            "Crossing End Time":crossingEndTime,
            "Crossing Interaction Notes":walkingInteraction,
            "Bus Presence":busPresence,
            "Crossing Location Relative to Bus":crossingLocationToBus,
            "Crossing Location Relative to Bus Stop":crossingLocationRelativeToBusStop,
            "Bus Noteworthy Events":'0',
            "Vehicle Traffic":trafficCondition,
            "General Reviewer Notes":'0'
        }

def generateQualityControllDataFrame(refDF, dflist):
    rows = []
    df0 = dflist[0]
    df1 = dflist[1]
    df2 = dflist[2]
    accuracy = accuracyScore()
    for index in refDF.itertuples():
        row0 = df0.iloc[index.Index]
        row1 = df1.iloc[int(refDF.iloc[index.Index].index1)]
        row2 = df2.iloc[int(refDF.iloc[index.Index].index2)]
        rows.append(constructRowDict(row0, row1, row2,index.Index,accuracy)) 
    return pd.DataFrame(rows),accuracy

def accuarcyTest(humanQuailityDF, computedQualityDF):
    correctCount = 0
    totalCount = len(computedQualityDF)
    for i in range(totalCount):
        humanRow = humanQuailityDF.iloc[i]
        computedRow = computedQualityDF.iloc[i]
        for col in humanQuailityDF.columns:
            if(humanRow[col] == computedRow[col]):
                correctCount += 1
    return correctCount/(totalCount * len(computedQualityDF.columns))

def computeTrafficData(fileList):
    dflist = generateDateFrameList(fileList)
    refDF = generateReferenceDataFrame(dflist)
    refDF.to_csv('output_refDF_data.csv', index=True,header=True)
    dfQualityControl,accuracy = generateQualityControllDataFrame(refDF,dflist)
    print(f"Overall Accuracy: {accuracy.getAccuarcy()*100:.2f}%")
    dfQualityControl = dfQualityControl.transpose()
    return dfQualityControl

def computeDataFolderToCSV(resourceFolderPath,outputFolderPath):
    for fileFolder in os.listdir(resourceFolderPath):
        filePath = os.path.join(resourceFolderPath, fileFolder)
        if(os.path.isdir(filePath)):
            fileList = []
            for filename in os.listdir(filePath):
                if filename.endswith(".csv"):
                    fileList.append(os.path.join(filePath,filename))
            print(f"Processing folder: {fileFolder} with files: {fileList}")
            dfQualityControl = computeTrafficData(fileList)
            dfQualityControl.to_csv(os.path.join(outputFolderPath,fileFolder)+'.csv', index=True,header=False)
    

def performAccuarcyTest(outPutFile,humanQualityFile):
    dfCompute = DataEngining.load_csv(outPutFile)
    dfHuman = DataEngining.load_csv(humanQualityFile)
    accuracy = accuarcyTest(dfHuman,dfCompute)
    print(f"Accuracy: {accuracy*100:.2f}%")
    
if __name__ == "__main__":
    computeDataFolderToCSV('./resource','./output')


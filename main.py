import pandas as pd
import math
from dataEngining import generateDateFrameList
from dataEngining import DataEngining
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

def generateReferenceDataFrame(pathUrl,dflist):
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
    return qualityDF
def compareParamets(A, B, C): 
    if A == B or A == C: 
        return A 
    if B == C: 
        return B 
    return None

def compareTimeDistance(timeA,timeB,timeC):
    distAB = abs(timeA - timeB)
    distAC = abs(timeA - timeC)
    distBC = abs(timeB - timeC)
    avgA = (distAB + distAC) / 2
    avgB = (distAB + distBC) / 2
    avgC = (distAC + distBC) / 2
    if(avgA <= avgB and avgA <= avgC):
        return timeA
    elif(avgB <= avgA and avgB <= avgC):
        return timeB
    else:
        return timeC

def secondsToTimeString(seconds):
    if seconds is None or seconds < 0:
        return ""
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

def generateQualityControllDataFrame(refDF, dflist):
    rows = []
    df0 = dflist[0]
    df1 = dflist[1]
    df2 = dflist[2]
    for index in refDF.itertuples():
        row0 = df0.iloc[index.Index]
        row1 = df1.iloc[index.Index]
        row2 = df2.iloc[index.Index]
        videoTitle = compareParamets(row0['Video Title'], row1['Video Title'], row2['Video Title'])
        locationName = compareParamets(row0['Location Name'], row1['Location Name'], row2['Location Name'])
        busStopIDs = compareParamets(row0['Bus Stop IDs/Addresses'], row1['Bus Stop IDs/Addresses'], row2['Bus Stop IDs/Addresses'])
        busStopRouteCount = compareParamets(row0['Count of Bus Stop Routes'], row1['Count of Bus Stop Routes'], row2['Count of Bus Stop Routes'])
        crosswalkLocationRelativeToBusStop = compareParamets(row0['Crosswalk Location Relative to Bus Stop'], row1['Crosswalk Location Relative to Bus Stop'], row2['Crosswalk Location Relative to Bus Stop'])
        crossingTreatment = compareParamets(row0['Crossing Treatment'], row1['Crossing Treatment'], row2['Crossing Treatment'])
        refugeIsland = enmumToString(compareParamets(row0['Refuge Island'], row1['Refuge Island'], row2['Refuge Island']),DataEngining.boolean)
        userCount = index.Index+1
        userType = enmumToString(compareParamets(row0['User Type'], row1['User Type'], row2['User Type']),DataEngining.userType)
        groupSize = compareParamets(row0['Group Size'], row1['Group Size'], row2['Group Size'])
        estimatedGender = enmumToString(compareParamets(row0['Estimated Gender'], row1['Estimated Gender'], row2['Estimated Gender']),DataEngining.gender)
        estimatedAgeGroup = compareParamets(row0['Estimated Age Group'], row1['Estimated Age Group'], row2['Estimated Age Group'])
        clothingColor = enmumToString(compareParamets(row0['Clothing Color'], row1['Clothing Color'], row2['Clothing Color']),DataEngining.clothingColor)
        visibilityScale = compareParamets(row0['Visibility Scale'], row1['Visibility Scale'], row2['Visibility Scale'])
        estimatedvisibleDistrction = enmumToString(compareParamets(row0['Estimated Visible Distrction'], row1['Estimated Visible Distrction'], row2['Estimated Visible Distrction']),DataEngining.boolean)
        userNotes = compareParamets(row0['User Notes'], row1['User Notes'], row2['User Notes'])
        busInteraction = enmumToString(compareParamets(row0['Bus Interaction'], row1['Bus Interaction'], row2['Bus Interaction']),DataEngining.boolean)
        roadwayCrossing = enmumToString(compareParamets(row0['Roadway Crossing'], row1['Roadway Crossing'], row2['Roadway Crossing']),DataEngining.boolean)
        typeOfBusInteraction = enmumToString(compareParamets(row0['Type of Bus Interaction'], row1['Type of Bus Interaction'], row2['Type of Bus Interaction']),DataEngining.busInteractions)
        busArrivalTime = secondsToTimeString(compareTimeDistance(row0['Bus Stop Arrival Time'], row1['Bus Stop Arrival Time'], row2['Bus Stop Arrival Time']))
        busDepartureTime = secondsToTimeString(compareTimeDistance(row0['Bus Stop Departure Time'], row1['Bus Stop Departure Time'], row2['Bus Stop Departure Time']))
        crosswalkCrossing = enmumToString(compareParamets(row0['Crosswalk Crossing?'], row1['Crosswalk Crossing?'], row2['Crosswalk Crossing?']),DataEngining.boolean)
        pedsestrianPhaseCrossing = enmumToString(compareParamets(row0['Pedestrian Phase Crossing?'], row1['Pedestrian Phase Crossing?'], row2['Pedestrian Phase Crossing?']),DataEngining.boolean)
        intendToCrossTimestamp = secondsToTimeString(compareTimeDistance(row0['Intend to Cross Timestamp'], row1['Intend to Cross Timestamp'], row2['Intend to Cross Timestamp']))
        crossingStartTime = secondsToTimeString(compareTimeDistance(row0['Crossing Start Time'], row1['Crossing Start Time'], row2['Crossing Start Time']))
        refugeIslandStartTime = secondsToTimeString(compareTimeDistance(row0['Refuge Island Start Time'], row1['Refuge Island Start Time'], row2['Refuge Island Start Time']))
        refugeIslandEndTime = secondsToTimeString(compareTimeDistance(row0['Refuge Island End Time'], row1['Refuge Island End Time'], row2['Refuge Island End Time']))
        finshedDuringPedsPhase = enmumToString(compareParamets(row0['Did User Finish Crossing During Pedestrian Phase?'], row1['Did User Finish Crossing During Pedestrian Phase?'], row2['Did User Finish Crossing During Pedestrian Phase?']),DataEngining.boolean)
        crossingEndTime = secondsToTimeString(compareTimeDistance(row0['Crossing End Time'], row1['Crossing End Time'], row2['Crossing End Time']))
        walkingInteraction = enmumToString(compareParamets(row0['Crossing Interaction Notes'], row1['Crossing Interaction Notes'], row2['Crossing Interaction Notes']),DataEngining.walkInteractions)
        busPresence = enmumToString(compareParamets(row0['Bus Presence'], row1['Bus Presence'], row2['Bus Presence']),DataEngining.boolean)
        crossingLocationToBus = enmumToString(compareParamets(row0['Crossing Location Relative to Bus'], row1['Crossing Location Relative to Bus'], row2['Crossing Location Relative to Bus']),DataEngining.crossingLocationRelativeToBus)
        crossingLocationRelativeToBusStop = enmumToString(compareParamets(row0['Crossing Location Relative to Bus Stop'], row1['Crossing Location Relative to Bus Stop'], row2['Crossing Location Relative to Bus Stop']),DataEngining.crossingLocationRelativeToBusStop)
        trafficCondition = enmumToString(compareParamets(row0['Vehicle Traffic'], row1['Vehicle Traffic'], row2['Vehicle Traffic']),DataEngining.trafficVolume)
        rows.append({
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
        }) 
    return pd.DataFrame(rows)

def accuarcyTest(humanQuailityDF, computedQualityDF):
    correctCount = 0
    totalCount = len(computedQualityDF)
    for i in range(totalCount):
        humanRow = humanQuailityDF.iloc[i]
        computedRow = computedQualityDF.iloc[i]
        for col in humanQuailityDF.columns:
            if(humanRow[col] == computedRow[col]):
                correctCount += 1
    # accuracy = correctCount / len(humanQuailityDF)
    return correctCount/(totalCount * len(computedQualityDF.columns))
    

def main():
    files = ["./resource/Belmont+Edward_St/Neva.csv","./resource/Belmont+Edward_St/Primah.csv","./resource/Belmont+Edward_St/Gareth.csv"]
    dflist = generateDateFrameList(files)
    refDF = generateReferenceDataFrame(files,dflist)
    dfQualityControl = generateQualityControllDataFrame(refDF,dflist)
    dfQualityControl = dfQualityControl.transpose()
    dfQualityControl.to_csv('output_data.csv', index=True)

def performAccuarcyTest(outPutFile,humanQualityFile):
    dfCompute = DataEngining.load_csv(outPutFile)
    dfHuman = DataEngining.load_csv(humanQualityFile)
    accuracy = accuarcyTest(dfHuman,dfCompute)
    print(f"Accuracy: {accuracy*100:.2f}%")
    
if __name__ == "__main__":
    main()
    # performAccuarcyTest('output_data.csv','./resource/Belmont+Edward_St/QualityControl.csv')
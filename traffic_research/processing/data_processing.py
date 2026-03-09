"""Data processing functions for computing and generating CSV outputs."""

import os
import pandas as pd
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from traffic_research.core.data_engineering import generateDateFrameList, generateDateFrame
from traffic_research.core.matching import exportGraphToCsv, generateReferenceGraph
from traffic_research.processing.quality_control import accuracyTest, generateQualityControlDataFramebyGraph
from traffic_research.core.models import AccuracyScore

def mergeCharacteristicWithQualityDataFrame(qualityDataFrame, characteristics):
    characteristic_columns = {
        # 'Location Name': characteristics['GTFSSTOP_NAME'],
        # 'Bus Stop IDs/Addresses': characteristics['STOP_ID'],
        # 'Count of Bus Stop Routes': characteristics['Num Bus Routes'],
        # 'Crossing Treatment': characteristics['SignalizedIntersection'],
    }
    qualityDataFrame['Location Name'] = characteristics['GTFSSTOP_NAME']
    qualityDataFrame['Bus Stop IDs/Addresses'] = characteristics['STOP_ID']
    qualityDataFrame['Count of Bus Stop Routes'] = characteristics['Num Bus Routes']
    qualityDataFrame['Crossing Treatment'] = characteristics['Crossing Treatment']
    qualityDataFrame['Crosswalk Location Relative to Bus Stop'] = characteristics['Crosswalk location relative to bus stop']
    qualityDataFrame['Refuge Island'] = characteristics['Refuge Island/Median']
    fieldToExclude = [
        'STOP_ID',
        'GTFSSTOP_NAME',
        'Crossing Treatment',
        'Num Bus Routes',
        'Crosswalk location relative to bus stop',
        'Refuge Island/Median'
    ]
    # Preserve the original order of fields from the characteristics row
    # and drop the last one so it is not added to characteristic_columns.
    fieldToAdd = [
        field for field in characteristics.keys().tolist()
        if field not in fieldToExclude
    ]
    if fieldToAdd:
        fieldToAdd = fieldToAdd[:-1]

    for field in fieldToAdd:
        characteristic_columns[field] = characteristics[field]

    # Add all characteristic columns in one operation to avoid DataFrame fragmentation.
    characteristic_block = pd.DataFrame(
        {column: [value] * len(qualityDataFrame) for column, value in characteristic_columns.items()},
        index=qualityDataFrame.index,
    )
    return pd.concat([qualityDataFrame, characteristic_block], axis=1)
    

def _processFolder(filePath, outputFolderPath, characteristics, accuracy, percentageThreshold, timeThreshold):
    """Helper function to process a single folder and generate CSV outputs."""
    
    def generateQCDataFrame(graph,dflist):
        return generateQualityControlDataFramebyGraph(graph, dflist, accuracy, timeThreshold)
    
    fileList = [
        os.path.join(filePath, filename)
        for filename in os.listdir(filePath)
        if filename.endswith(".csv")
    ]
    
    folderName = os.path.basename(filePath)
    dflist = generateDateFrameList(fileList)
    dfNoneBusUserCrossing = []
    dfBusUserCrossing = []
    dfBusNotCrossing = []
    for df in dflist:
        dfNoneBusUserCrossingRow = {
            'path': df['path'],
            'df': df['df'][df['df']['Bus Interaction'] == 0],
        }
        dfBusUserCrossingRow = {
            'path': df['path'],
            'df': df['df'][(df['df']['Bus Interaction'] == 1) & (df['df']['Roadway Crossing'] == 1)],
        }
        dfBusNotCrossingRow = {
            'path': df['path'],
            'df': df['df'][(df['df']['Bus Interaction'] == 1) & (df['df']['Roadway Crossing'] == 0)],
        }
        dfNoneBusUserCrossing.append(dfNoneBusUserCrossingRow)
        dfBusUserCrossing.append(dfBusUserCrossingRow)
        dfBusNotCrossing.append(dfBusNotCrossingRow)
    for df in dfNoneBusUserCrossing:
        df['df'] = df['df'].sort_values(by=['Crossing Start Time'], inplace=False)
        
    for df in dfBusUserCrossing:
        df['df'] = df['df'].sort_values(by=['Crossing Start Time'], inplace=False)
    for df in dfBusNotCrossing:
        df['df'] = df['df'].sort_values(by=['Bus Stop Arrival Time'], inplace=False)
    
    
    dfNoneBusUserCrossing = sorted(dfNoneBusUserCrossing, key=lambda x: x["df"].shape[0])
    dfBusUserCrossing = sorted(dfBusUserCrossing, key=lambda x: x["df"].shape[0])
    dfBusNotCrossing = sorted(dfBusNotCrossing, key=lambda x: x["df"].shape[0])
    
    dfNoneBusUserCrossingGraph = generateReferenceGraph(
        dfNoneBusUserCrossing,
        timeThreshold=timeThreshold,
        percentageThreshold=percentageThreshold,
        timeColumn="Crossing Start Time",
    )
    dfNoneBusUserCrossingGraphQC = generateQCDataFrame(dfNoneBusUserCrossingGraph, dfNoneBusUserCrossing)
    
    dfBusUserCrossingGraph = generateReferenceGraph(
        dfBusUserCrossing,
        timeThreshold=timeThreshold,
        percentageThreshold=percentageThreshold,
        timeColumn="Crossing Start Time",
    )
    dfBusUserCrossingGraphQC = generateQCDataFrame(dfBusUserCrossingGraph, dfBusUserCrossing)
    
    dfBusNotCrossingGraph = generateReferenceGraph(
        dfBusNotCrossing,
        timeThreshold=timeThreshold,
        percentageThreshold=percentageThreshold,
        timeColumn="Bus Stop Arrival Time",
    )
    dfBusNotCrossingGraphQC = generateQCDataFrame(dfBusNotCrossingGraph, dfBusNotCrossing)
    
    dfQualityControl = pd.concat(
        [dfNoneBusUserCrossingGraphQC, dfBusUserCrossingGraphQC, dfBusNotCrossingGraphQC],
        ignore_index=False)
    dfQualityControl = dfQualityControl.sort_values(by=['sort_key'], inplace=False).drop('sort_key', axis=1)
    
    # dfQualityControl = dfQualityControl.transpose()
    outputGraphFolderPath = os.path.join(outputFolderPath, 'graph')
    exportGraphToCsv(dfNoneBusUserCrossingGraph, os.path.join(outputGraphFolderPath, folderName) + 'NoneBusUserCrossing_graph.csv')
    exportGraphToCsv(dfBusUserCrossingGraph, os.path.join(outputGraphFolderPath, folderName) + 'BusUserCrossing_graph.csv')
    exportGraphToCsv(dfBusNotCrossingGraph, os.path.join(outputGraphFolderPath, folderName) + 'BusNotCrossing_graph.csv')
    accuracy.appendFileAccuracy(os.path.basename(filePath), accuracy.getAccuracy())
    accuracy.reset()
    dfQualityControl = mergeCharacteristicWithQualityDataFrame(dfQualityControl, characteristics)
    dfQualityControl.transpose().to_csv(
        os.path.join(outputFolderPath, characteristics['GTFSSTOP_NAME'] + '.csv'), 
        index=True, 
        header=False
    )
    names = ['dfNoneBusUserCrossingGraphQC', 'dfBusUserCrossingGraphQC', 'dfBusNotCrossingGraphQC']
    for index, df in enumerate([dfNoneBusUserCrossingGraphQC, dfBusUserCrossingGraphQC, dfBusNotCrossingGraphQC]):
        os.makedirs(os.path.join(outputFolderPath, folderName), exist_ok=True)
        df.transpose().to_csv(
            os.path.join(os.path.join(outputFolderPath, folderName), names[index] + '.csv'), 
            index=True, 
            header=False
        )
    
    return dfQualityControl


def loadCharacteristics(characteristicsPath):
    characteristics = pd.read_csv(characteristicsPath)
    characteristics = characteristics.set_index('fid')
    return characteristics

def computeDataFolderToCSV(resourceFolderPath, outputFolderPath, characteristicsPath, percentageThreshold, timeThreshold):
    """Process all folders in resource path and generate CSV outputs."""
    allComputedRows = pd.DataFrame(columns=[])
    accuracy = AccuracyScore()
    characteristics = loadCharacteristics(characteristicsPath)
    for fileFolder in os.listdir(resourceFolderPath):
        filePath = os.path.join(resourceFolderPath, fileFolder)
        if os.path.isdir(filePath):
           allComputedRows=pd.concat([allComputedRows, _processFolder(filePath, outputFolderPath, characteristics.loc[int(os.path.basename(filePath))], accuracy, percentageThreshold, timeThreshold)], ignore_index=False)
        
    accuracyDF = pd.DataFrame(accuracy.getFilesAccuracy(), columns=['Location', 'Accuracy'])
    accuracyDF.to_csv(os.path.join(outputFolderPath, 'interated_summary.csv'), header=True)
    allComputedRows.transpose().to_csv(
        os.path.join(outputFolderPath, 'allComputedRows.csv'), 
        index=True, 
        header=False
    )


def performAccuracyTest(outputFile, humanQualityFile):
    """Perform accuracy test comparing computed output with human quality control."""
    dfCompute = generateDateFrame(outputFile).dropna(how='all')
    dfHuman = generateDateFrame(humanQualityFile).dropna(how='all')
    
    accuracy = accuracyTest(dfHuman, dfCompute)
    print(f"Accuracy: {accuracy*100:.2f}%")
    return accuracy

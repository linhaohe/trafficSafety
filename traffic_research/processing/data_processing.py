"""Data processing functions for computing and generating CSV outputs."""

import os
import pandas as pd
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from traffic_research.core.data_engineering import generateDateFrameList, generateDateFrame
from traffic_research.core.matching import exportGraphToCsv, generateReferenceDataFrame, generateReferenceGraph
from traffic_research.processing.quality_control import generateQualityControlDataFrame, accuracyTest, generateQualityControlDataFramebyGraph
from traffic_research.core.models import AccuracyScore


def computeTrafficData(fileList, accuracy, percentageThreshold, timeThreshold):
    """Compute traffic data from file list and generate quality control DataFrame."""
    dflist = generateDateFrameList(fileList)
    dflist = sorted(dflist, key=len)
    # Calculate range value based on dataframe length differences
    range_value = dflist[-1].shape[0] - dflist[0].shape[0] + 2
    refDF = generateReferenceDataFrame(dflist, timeThreshold, percentageThreshold, range_value)
    dfQualityControl = generateQualityControlDataFrame(refDF, dflist, accuracy, timeThreshold)
    dfQualityControl = dfQualityControl.transpose()
    return dfQualityControl, refDF


def _processFolder(filePath, outputFolderPath, accuracy, percentageThreshold, timeThreshold):
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
    #     df['df'].transpose().to_csv(
    #     os.path.join(outputFolderPath, os.path.basename(df['path'])) + '.csv', 
    #     index=True, 
    #     header=False
    # )
        
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
    dfQualityControl = dfQualityControl.transpose()
    
    exportGraphToCsv(dfNoneBusUserCrossingGraph, os.path.join(outputFolderPath, folderName) + 'NoneBusUserCrossing_graph.csv')
    exportGraphToCsv(dfBusUserCrossingGraph, os.path.join(outputFolderPath, folderName) + 'BusUserCrossing_graph.csv')
    exportGraphToCsv(dfBusNotCrossingGraph, os.path.join(outputFolderPath, folderName) + 'BusNotCrossing_graph.csv')
    accuracy.appendFileAccuracy(os.path.basename(filePath), accuracy.getAccuracy())
    accuracy.reset()
    dfQualityControl.to_csv(
        os.path.join(outputFolderPath, folderName) + '.csv', 
        index=True, 
        header=False
    )



def computeDataFolderToCSV(resourceFolderPath, outputFolderPath, percentageThreshold, timeThreshold):
    """Process all folders in resource path and generate CSV outputs."""
    accuracy = AccuracyScore()
    
    for fileFolder in os.listdir(resourceFolderPath):
        filePath = os.path.join(resourceFolderPath, fileFolder)
        if os.path.isdir(filePath):
            _processFolder(filePath, outputFolderPath, accuracy, percentageThreshold, timeThreshold)
    
    accuracyDF = pd.DataFrame(accuracy.getFilesAccuracy(), columns=['Location', 'Accuracy'])
    accuracyDF.to_csv(os.path.join(outputFolderPath, 'interated_summary.csv'), header=True)


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

"""Main entry point for traffic research analysis."""

from config import (
    INPUT_DATA_PATH, OUTPUT_PATH, 
    NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC,
    BELMONT_OUTPUT, BELMONT_HUMAN_QC
)
from traffic_research.processing.data_processing import computeDataFolderToCSV, performAccuracyTest
from traffic_research.graphing.graphing import generateGraphDataPercentage, generateGraphDataTime, graphData
from traffic_research.core import AccuracyScore, calculateTimeScore, computeTimeScore, generateDateFrameList
from traffic_research.core.matching import compareParameters, generateReferenceGraph, exportGraphToCsv
import pandas as pd

from traffic_research.processing.quality_control import generateQualityControlDataFramebyGraph

def makeDummyTimeData(intend_to_cross_timestamp,crossing_start_time, crossing_end_time, bus_stop_arrival_time, bus_stop_departure_time):
    time = {
        'Intend to Cross Timestamp': intend_to_cross_timestamp,
        'Crossing Start Time': crossing_start_time,
        'Crossing End Time': crossing_end_time,
        'Bus Stop Arrival Time': bus_stop_arrival_time,
        'Bus Stop Departure Time': bus_stop_departure_time,
        'Refuge Island Start Time': -1,
        'Refuge Island End Time': -1,
    }
    return pd.DataFrame(time, index=[0])

def printGraph(graph):
    print("\n" + "="*80)
    print("REFERENCE GRAPH")
    print("="*80)
    for key, matches in graph.items():
        if isinstance(key, tuple):
            df_name, idx = key[0], key[1]
        else:
            key_dict = dict(key)
            df_name = key_dict.get('dfName', 'Unknown')
            idx = key_dict.get('index', 'Unknown')
        print(f"\nNode: {df_name} - Index {idx}")
        print(f"  Matches ({len(matches)}):")
        for match in matches:
            print(f"    -> {match['key']} (score: {match['score']:.4f})")
    print("\n" + "="*80)
    print(f"Total nodes in graph: {len(graph)}")
    print("="*80 + "\n")
    
if __name__ == "__main__":
    # compareParameters(row0, row1, row2, fieldName, accuracy)
    # row0 = {'Crosswalk Crossing': 'Yes'}
    # row1 = {'Crosswalk Crossing': 'No'}
    # row2 = {'Crosswalk Crossing': 'Yes'}
    # fieldName = 'Crosswalk Crossing'
    # accuracy = AccuracyScore()
    # print(compareParameters(row0, row1, row2, fieldName, accuracy))
    computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH, percentageThreshold=0.65, timeThreshold=6)
    performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
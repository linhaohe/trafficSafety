"""Main entry point for traffic research analysis."""

from config import (
    INPUT_DATA_PATH, OUTPUT_PATH, 
    NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC,
    BELMONT_OUTPUT, BELMONT_HUMAN_QC
)
from traffic_research.processing.data_processing import computeDataFolderToCSV, performAccuracyTest
from traffic_research.graphing.graphing import generateGraphDataPercentage, generateGraphDataTime, graphData
from traffic_research.core import calculateTimeScore, computeTimeScore
import pandas as pd

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

if __name__ == "__main__":
    # Generate graph data for testing different percentage thresholds
    # generateGraphDataPercentage(start_percent=1, end_percent=101, time_threshold=6)
    # generateGraphDataTime(start_time=1, end_time=20, percentage_threshold=0.6)

    # Uncomment to generate graphs from existing data
    # graphData()
    
    # Uncomment to run single computation and accuracy tests
    computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH, percentageThreshold=0.65, timeThreshold=6)
    # performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
    # dummy1 = makeDummyTimeData(55215.0,55216.0,55243.0,-1,-1)
    # dummy2 = makeDummyTimeData(-1,55290.0,55308.0,-1,-1)
    # dummy3 = makeDummyTimeData(-1,55222.0,55236.0,-1,-1)
    # testTimeThreshold = 6
    # test = computeTimeScore(dummy1.iloc[0], dummy2.iloc[0], testTimeThreshold)
    # test2 = computeTimeScore(dummy1.iloc[0], dummy3.iloc[0], testTimeThreshold)
    # print(f"Test: {test}")
    # print(f"Test2: {test2}")
    # performAccuracyTest(BELMONT_OUTPUT, BELMONT_HUMAN_QC) 
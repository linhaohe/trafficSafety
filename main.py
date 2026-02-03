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
    computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH, percentageThreshold=0.65, timeThreshold=6)
    # performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
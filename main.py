"""Main entry point for traffic research analysis."""

from config import (
    INPUT_DATA_PATH, OUTPUT_PATH, 
    NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC,
    BELMONT_OUTPUT, BELMONT_HUMAN_QC
)
from traffic_research.processing.data_processing import computeDataFolderToCSV, performAccuracyTest
from traffic_research.graphing.graphing import generateGraphDataPercentage, generateGraphDataTime, graphData


if __name__ == "__main__":
    # Generate graph data for testing different percentage thresholds
    # generateGraphDataPercentage(start_percent=1, end_percent=101, time_threshold=3)
    # generateGraphDataTime(start_time=1, end_time=20, percentage_threshold=0.64)

    # Uncomment to generate graphs from existing data
    # graphData()
    
    # Uncomment to run single computation and accuracy tests
    computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH, percentageThreshold=0.64, timeThreshold=7)
    performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
    performAccuracyTest(BELMONT_OUTPUT, BELMONT_HUMAN_QC)

"""Main entry point for traffic research analysis."""

from config import (
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    NORTHAMPTON_OUTPUT,
    NORTHAMPTON_HUMAN_QC,
)
from traffic_research.processing.data_processing import computeDataFolderToCSV, performAccuracyTest

if __name__ == "__main__":
    computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH, percentageThreshold=0.65, timeThreshold=10)
    performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
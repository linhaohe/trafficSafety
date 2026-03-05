"""Main entry point for traffic research analysis."""

from config import (
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    NORTHAMPTON_OUTPUT,
    NORTHAMPTON_HUMAN_QC,
)
from traffic_research.processing.data_processing import computeDataFolderToCSV, performAccuracyTest
from traffic_research.core.clustering import plotAverageSilhouetteScore, runMode
from traffic_research.core.data_engineering import generateDateFrame
import matplotlib.pyplot as plt

import os

if __name__ == "__main__":
    # computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH, percentageThreshold=0.65, timeThreshold=10)
    # performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
    allComputedRows = generateDateFrame(os.path.join(OUTPUT_PATH, 'allComputedRows.csv'))
    plotAverageSilhouetteScore(allComputedRows, numberOfIterations=50, maxNumberOfClusters=14)
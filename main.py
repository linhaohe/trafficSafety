"""Main entry point for traffic research analysis."""

from config import (
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    CHARACTERISTICS_PATH
)
from traffic_research.core.clustering import runMode,plotAverageSilhouetteScore
from traffic_research.core.data_engineering import generateDateFrame
from traffic_research.processing.data_processing import computeDataFolderToCSV, performAccuracyTest
import os
import pandas as pd
if __name__ == "__main__":
    # characteristics = pd.read_csv(CHARACTERISTICS_PATH)
    # characteristics = characteristics.set_index('fid')
    # print(characteristics.iloc[0].keys().tolist())
    computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH,CHARACTERISTICS_PATH,percentageThreshold=0.65, timeThreshold=10)
    # allComputedRows = generateDateFrame(os.path.join(OUTPUT_PATH, 'allComputedRows.csv'))
    # runMode(allComputedRows, n_clusters=3)
    # plotAverageSilhouetteScore(allComputedRows, numberOfIterations=50, maxNumberOfClusters=14)
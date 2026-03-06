"""Main entry point for traffic research analysis."""

from config import (
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    CHARACTERISTICS_PATH
)
from traffic_research.processing.data_processing import computeDataFolderToCSV, performAccuracyTest

if __name__ == "__main__":
    computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH,CHARACTERISTICS_PATH,percentageThreshold=0.65, timeThreshold=10)
    # allComputedRows = generateDateFrame(os.path.join(OUTPUT_PATH, 'allComputedRows.csv'))
    # runMode(allComputedRows, n_clusters=3)
    # plotAverageSilhouetteScore(allComputedRows, numberOfIterations=50, maxNumberOfClusters=14)
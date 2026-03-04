"""Main entry point for traffic research analysis."""

from config import (
    INPUT_DATA_PATH,
    OUTPUT_PATH,
    NORTHAMPTON_OUTPUT,
    NORTHAMPTON_HUMAN_QC,
)
from traffic_research.processing.data_processing import computeDataFolderToCSV, performAccuracyTest
from traffic_research.core.clustering import runMode
from traffic_research.core.data_engineering import generateDateFrame
import matplotlib.pyplot as plt

import os

if __name__ == "__main__":
    computeDataFolderToCSV(INPUT_DATA_PATH, OUTPUT_PATH, percentageThreshold=0.65, timeThreshold=10)
    performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
    # allComputedRows = generateDateFrame(os.path.join(OUTPUT_PATH, 'allComputedRows.csv'))
    # scores = []
    # print(allComputedRows.columns)
    # for n_clusters in range(2, 14):
    #     silhouette_score = runMode(allComputedRows, n_clusters=n_clusters)
    #     scores.append(silhouette_score)
    # plt.plot(range(2, 14), scores)
    # plt.xlabel('Number of clusters')
    # plt.ylabel('Silhouette score')
    # plt.title('Silhouette score vs number of clusters')
    # plt.savefig(os.path.join(OUTPUT_PATH, 'silhouette_score_vs_number_of_clusters.png'))
    # plt.close()
"""Configuration constants for traffic research analysis."""

import os

# Scoring weights
OVERALL_TIME_SCORE_WEIGHT = 0.5 
TIME_SCORE_WEIGHT = OVERALL_TIME_SCORE_WEIGHT / 7
DEFAULT_TIME_THRESHOLD_WEIGHT = 10
CONDITION_SCORE_WEIGHT = (1 - OVERALL_TIME_SCORE_WEIGHT) / 10
DEFAULT_PERCENTAGE_THRESHOLD = 0.8
DEFAULT_TIME_THRESHOLD = 3

# File paths
INPUT_DATA_PATH = './resource/inputData'
OUTPUT_PATH = './output'
ACCURACY_SUMMARY_DIR = os.path.join(OUTPUT_PATH, 'accuracy_summary')
HUMAN_QC_PATH = './resource/human_quality_control'

NORTHAMPTON_OUTPUT = os.path.join(OUTPUT_PATH, 'Northampton_Court_House_V43.csv')
NORTHAMPTON_HUMAN_QC = os.path.join(HUMAN_QC_PATH, 'Norhampton_Court_House.csv')
BELMONT_OUTPUT = os.path.join(OUTPUT_PATH, 'Belmont+Edward_St_V38.csv')
BELMONT_HUMAN_QC = os.path.join(HUMAN_QC_PATH, 'Belmont_St+Edward_St.csv')

# Parameters excluded from accuracy tracking
EXCLUDED_FROM_ACCURACY = {
    'Video Title',
    'Initials',
    'Location Name',
    'Count of Bus Stop Routes'
}

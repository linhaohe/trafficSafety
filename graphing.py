"""Graph generation functions for visualizing accuracy data."""

import os
import pandas as pd
from config import (
    ACCURACY_SUMMARY_DIR, NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC, 
    BELMONT_OUTPUT, BELMONT_HUMAN_QC, INPUT_DATA_PATH, OUTPUT_PATH
)
from data_processing import computeDataFolderToCSVWithIndex, performAccuracyTest


def _generateGraphDataHelper(
    start_index, 
    end_index, 
    percentage_threshold, 
    time_threshold, 
    threshold_name,
    get_threshold_value
):
    """Helper function to generate graph data by testing different threshold values.
    
    Args:
        start_index: Starting index for iteration
        end_index: Ending index for iteration (exclusive)
        percentage_threshold: Percentage threshold value (constant or None if varying)
        time_threshold: Time threshold value (constant or None if varying)
        threshold_name: Name identifier for the threshold being tested
        get_threshold_value: Function that takes index and returns (pct_threshold, time_threshold) tuple
    
    Returns:
        Tuple of (accuracy_rows1, accuracy_rows2) DataFrames
    """
    accuracy_rows1 = []
    accuracy_rows2 = []
    
    # Ensure output directory exists
    os.makedirs(ACCURACY_SUMMARY_DIR, exist_ok=True)
    
    for i in range(start_index, end_index):
        pct_thresh, time_thresh = get_threshold_value(i)
        
        computeDataFolderToCSVWithIndex(
            INPUT_DATA_PATH,
            OUTPUT_PATH,
            percentageThreshold=pct_thresh,
            timeThreshold=time_thresh,
            threshold_name=threshold_name,
            index=i
        )
        
        accuracy1 = performAccuracyTest(NORTHAMPTON_OUTPUT, NORTHAMPTON_HUMAN_QC)
        accuracy2 = performAccuracyTest(BELMONT_OUTPUT, BELMONT_HUMAN_QC)
        
        accuracy_rows1.append({
            'Percentage Threshold': pct_thresh,
            'Time Threshold': time_thresh,
            'Data Set': 'Northampton_Court_House',
            'Accuracy': accuracy1
        })
        accuracy_rows2.append({
            'Percentage Threshold': pct_thresh,
            'Time Threshold': time_thresh,
            'Data Set': 'Belmont_St+Edward_St',
            'Accuracy': accuracy2
        })
    
    # Save results
    df1 = pd.DataFrame(accuracy_rows1)
    df2 = pd.DataFrame(accuracy_rows2)
    
    df1.to_csv(
        os.path.join(ACCURACY_SUMMARY_DIR, f'accuracy_summary_{threshold_name}_Northampton_Court_House.csv'),
        header=True
    )
    df2.to_csv(
        os.path.join(ACCURACY_SUMMARY_DIR, f'accuracy_summary_{threshold_name}_Belmont_St+Edward_St.csv'),
        header=True
    )
    
    return df1, df2


def generateGraphDataPercentage(start_percent=1, end_percent=2, time_threshold=3, threshold_name='percentage'):
    """Generate graph data by testing different percentage thresholds.
    
    Args:
        start_percent: Starting percentage threshold (multiplied by 0.01)
        end_percent: Ending percentage threshold (multiplied by 0.01, exclusive)
        time_threshold: Time threshold to use for all tests (constant)
        threshold_name: Name identifier for output files
    """
    def get_threshold_value(i):
        return (0.01 * i, time_threshold)
    
    return _generateGraphDataHelper(
        start_percent,
        end_percent,
        None,  # percentage varies
        time_threshold,
        threshold_name,
        get_threshold_value
    )


def generateGraphDataTime(start_time=1, end_time=2, percentage_threshold=0.64, threshold_name='time'):
    """Generate graph data by testing different time thresholds.
    
    Args:
        start_time: Starting time threshold
        end_time: Ending time threshold (exclusive)
        percentage_threshold: Percentage threshold to use for all tests (constant)
        threshold_name: Name identifier for output files
    """
    def get_threshold_value(i):
        return (percentage_threshold, i)
    
    return _generateGraphDataHelper(
        start_time,
        end_time,
        percentage_threshold,
        None,  # time varies
        threshold_name,
        get_threshold_value
    )


def graphData():
    """Generate all accuracy comparison graphs."""
    import matplotlib.pyplot as plt
    
    def _loadIteratedAccuracyData(start, end, file_pattern, x_col_name, x_multiplier=1):
        """Helper to load iterated accuracy data from CSV files."""
        rows1 = []
        rows2 = []
        for i in range(start, end):
            file_path = os.path.join(ACCURACY_SUMMARY_DIR, file_pattern.format(i))
            if not os.path.exists(file_path):
                continue
            df_data = pd.read_csv(file_path).dropna(how='all')
            if len(df_data) >= 2:
                rows1.append({
                    'Data Set': 'Northampton_Court_House',
                    'Accuracy': df_data.iloc[0]['Accuracy'],
                    x_col_name: i * x_multiplier
                })
                rows2.append({
                    'Data Set': 'Belmont_St+Edward_St',
                    'Accuracy': df_data.iloc[1]['Accuracy'],
                    x_col_name: i * x_multiplier
                })
        return pd.DataFrame(rows1), pd.DataFrame(rows2)
    
    def _plotAccuracyComparison(df1, df2, x_col, title, xlabel, ylabel, output_file):
        """Helper to plot accuracy comparison between two datasets."""
        plt.plot(df1[x_col], df1['Accuracy'], label='Northampton_Court_House')
        plt.plot(df2[x_col], df2['Accuracy'], label='Belmont_St+Edward_St')
        plt.legend()
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.savefig(output_file)
        plt.clf()
    
    def graphIteratedAccuracyPercentageThreshold():
        """Graph consistency test of percentage threshold."""
        df1, df2 = _loadIteratedAccuracyData(1, 101, 'accuracy_summary_percentage_{}.csv', 'Percentage Threshold', 0.01)
        _plotAccuracyComparison(
            df1, df2, 'Percentage Threshold',
            "Consistency test of percentage threshold, t = 3s",
            "Percentage Threshold(%)",
            "Iterated Accuracy(%)",
            os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_percentage_threshold.png')
        )
    
    def graphPercentageThresholdVsHumanAccuracy():
        """Graph percentage threshold vs human accuracy."""
        df1 = pd.read_csv(os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_percentage_Northampton_Court_House.csv'))
        df2 = pd.read_csv(os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_percentage_Belmont_St+Edward_St.csv'))
        _plotAccuracyComparison(
            df1, df2, 'Percentage Threshold',
            "Accuracy test of percentage threshold vs human accuracy, t = 3s",
            "Percentage Threshold(%)",
            "Similarity(%)",
            os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_percentage_threshold_vs_human_accuracy.png')
        )
    
    def graphIteratedAccuracyTimeThreshold():
        """Graph consistency test of time threshold."""
        rows1 = []
        rows2 = []
        for i in range(1, 20):
            file_path = os.path.join(ACCURACY_SUMMARY_DIR, f'accuracy_summary_time_{i}.csv')
            if not os.path.exists(file_path):
                continue
            df_data = pd.read_csv(file_path).dropna(how='all')
            if len(df_data) >= 2:
                rows1.append({
                    'Data Set': 'Northampton_Court_House',
                    'Accuracy': df_data.iloc[0]['Accuracy'],
                    'Time Threshold': i
                })
                rows2.append({
                    'Data Set': 'Belmont_St+Edward_St',
                    'Accuracy': df_data.iloc[1]['Accuracy'],
                    'Time Threshold': i
                })
        df1 = pd.DataFrame(rows1)
        df2 = pd.DataFrame(rows2)
        _plotAccuracyComparison(
            df1, df2, 'Time Threshold',
            "Consistency test of time threshold, % = 0.64",
            "Time Threshold(seconds)",
            "Iterated Accuracy(%)",
            os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_time_summary_time_threshold.png')
        )
    
    def graphTimeThresholdVsHumanAccuracy():
        """Graph time threshold vs human accuracy."""
        df1 = pd.read_csv(os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_time_Northampton_Court_House.csv'))
        df2 = pd.read_csv(os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_summary_time_Belmont_St+Edward_St.csv'))
        _plotAccuracyComparison(
            df1, df2, 'Time Threshold',
            "Accuracy test of time threshold vs human accuracy, % = 0.64",
            "Time Threshold(seconds)",
            "Similarity(%)",
            os.path.join(ACCURACY_SUMMARY_DIR, 'accuracy_time_summary_time_threshold_vs_human_accuracy.png')
        )
    
    graphIteratedAccuracyPercentageThreshold()
    graphPercentageThresholdVsHumanAccuracy()
    graphIteratedAccuracyTimeThreshold()
    graphTimeThresholdVsHumanAccuracy()

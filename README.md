# Traffic Research Analysis

A Python package for analyzing traffic research data, including data processing, quality control, accuracy testing, and visualization.

## Project Structure

```
./
├── config.py                    # Configuration constants
├── main.py                      # Main entry point
├── requirements.txt             # Python dependencies
└── traffic_research/            # Main package
    ├── __init__.py
    ├── core/                    # Core functionality
    │   ├── models.py           # Data models (AccuracyScore)
    │   ├── scoring.py          # Scoring functions
    │   ├── matching.py         # Matching and comparison functions
    │   ├── utils.py            # Utility functions
    │   └── data_engineering.py # Data engineering utilities
    ├── processing/              # Data processing
    │   ├── data_processing.py  # Data processing functions
    │   └── quality_control.py # Quality control functions
    └── graphing/                # Visualization
        └── graphing.py         # Graph generation functions
```

## Requirements

- Python 3.8+ (recommended)
- Dependencies listed in `requirements.txt`:
  - `pandas>=1.5.0` - Data processing and DataFrame operations
  - `matplotlib>=3.5.0` - Visualization and plotting

## Setup

### macOS / Linux

1. Open a terminal and navigate to the project directory:
   ```bash
   cd path/to/Traffic\ research
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Windows

1. Open a terminal and navigate to the project directory:
   ```bash
   cd path\to\Traffic research
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Running the Main Script

From the project root (with virtual environment active):

```bash
python main.py
```

The main script performs the following operations:
- Processes traffic data from input folders
- Generates quality control DataFrames
- Performs accuracy tests comparing computed results with human quality control data

### Configuration

Edit `config.py` to adjust:
- Scoring weights (`TIME_SCORE_WEIGHT`, `CONDITION_SCORE_WEIGHT`)
- Default thresholds (`DEFAULT_PERCENTAGE_THRESHOLD`, `DEFAULT_TIME_THRESHOLD`)
- File paths (`INPUT_DATA_PATH`, `OUTPUT_PATH`, etc.)

### Main Functions

#### Data Processing
- `computeDataFolderToCSV()` - Process all folders in input data path and generate CSV outputs
- `performAccuracyTest()` - Compare computed output with human quality control data

#### Graph Generation
- `generateGraphDataPercentage()` - Generate graph data for different percentage thresholds
- `generateGraphDataTime()` - Generate graph data for different time thresholds
- `graphData()` - Generate all accuracy comparison graphs

## Package Modules

### Core (`traffic_research.core`)
- **models**: `AccuracyScore` class for tracking accuracy metrics
- **scoring**: Functions for calculating numeric and condition scores
- **matching**: Functions for matching rows across dataframes and comparing parameters
- **utils**: Utility functions for time conversion and enum handling

### Processing (`traffic_research.processing`)
- **data_processing**: Functions for computing traffic data and processing folders
- **quality_control**: Functions for generating quality control DataFrames and accuracy testing

### Graphing (`traffic_research.graphing`)
- **graphing**: Functions for generating accuracy comparison graphs

## Input/Output

### Input Data
- Input data should be placed in `./resource/inputData/` (configurable in `config.py`)
- Each folder should contain CSV files from different reviewers

### Output
- Processed data is saved to `./output/` (configurable in `config.py`)
- Quality control DataFrames are saved as CSV files
- Accuracy summaries are saved to `./output/accuracy_summary/`
- Graphs are saved as PNG files in the accuracy summary directory

## Troubleshooting

- **ModuleNotFoundError**: Ensure virtual environment is active and dependencies are installed
  ```bash
  pip install -r requirements.txt
  ```

- **Import errors**: Make sure you're running from the project root directory

- **File not found errors**: Check that input data paths in `config.py` are correct

- **Permission errors**: Use a virtual environment or ensure file permissions are correct

## Notes

- The package uses pandas DataFrames for data manipulation
- Time values are stored as seconds since midnight
- Certain parameters (Video Title, Initials, Location Name, Count of Bus Stop Routes) are excluded from accuracy calculations
- The matching algorithm compares rows across three dataframes (A, B, C) and marks visited rows to prevent reuse

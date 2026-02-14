# Traffic Research Analysis

A Python package for analyzing traffic research data: processing reviewer CSVs, building consensus via graph-based time-window matching, quality control, accuracy testing, and optional visualization.

## Project Structure

```
./
├── config.py                    # Configuration constants (paths, weights, thresholds)
├── main.py                      # Main entry point
├── requirements.txt             # Python dependencies
├── ALGORITHM_SUMMARY.md         # Detailed algorithm and matching logic
└── traffic_research/            # Main package
    ├── __init__.py
    ├── core/                    # Core functionality
    │   ├── models.py            # Data models (AccuracyScore)
    │   ├── scoring.py            # Similarity scoring (time + condition)
    │   ├── matching.py          # Reference graph and export
    │   ├── utils.py              # Time/enum utilities
    │   └── data_engineering.py  # CSV load, parse, logic rules
    ├── processing/               # Data processing
    │   ├── data_processing.py   # Folder processing, graph + QC pipeline
    │   └── quality_control.py   # QC from graph, consensus, accuracy test
    └── graphing/                 # Visualization
        └── graphing.py          # Accuracy comparison graphs
```

## Requirements

- Python 3.8+
- Dependencies in `requirements.txt`:
  - `pandas>=1.5.0` — DataFrames and CSV I/O
  - `matplotlib>=3.5.0` — Optional; used by graphing module

## Setup

### macOS / Linux

```bash
cd path/to/Traffic\ research
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Windows

```bash
cd path\to\Traffic research
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

### Running the main script

From the project root (with the virtual environment active):

```bash
python main.py
```

This will:

1. **Process all folders** under `INPUT_DATA_PATH`: each folder should contain 3 reviewer CSVs.
2. **Split** data by Bus Interaction and Roadway Crossing into three subsets (NoneBusUserCrossing, BusUserCrossing, BusNotCrossing), build a **reference graph** per subset using time-window matching, then generate **quality-control DataFrames** and export graphs and QC to CSV.
3. **Write** an accuracy summary to `output/interated_summary.csv`.
4. **Run** an accuracy test comparing computed output to human QC for the Northampton location (see `config.py`).

Default thresholds used in `main.py`: `percentageThreshold=0.65`, `timeThreshold=10` (seconds).

### Configuration

Edit `config.py` to change:

- **Paths**: `INPUT_DATA_PATH`, `OUTPUT_PATH`, `HUMAN_QC_PATH`, `ACCURACY_SUMMARY_DIR`, and per-dataset paths (e.g. `NORTHAMPTON_OUTPUT`, `NORTHAMPTON_HUMAN_QC`, `BELMONT_*`).
- **Scoring**: `TIME_SCORE_WEIGHT`, `CONDITION_SCORE_WEIGHT`, `COLOR_WEIGHT`.
- **Defaults**: `DEFAULT_PERCENTAGE_THRESHOLD`, `DEFAULT_TIME_THRESHOLD`.
- **Accuracy**: `EXCLUDED_FROM_ACCURACY` — field names excluded from accuracy calculations.

### Main functions

#### Data processing

- **`computeDataFolderToCSV(resourceFolderPath, outputFolderPath, percentageThreshold, timeThreshold)`** — Process all subfolders; produce one QC CSV and three graph CSVs per folder, plus `interated_summary.csv`.
- **`performAccuracyTest(outputFile, humanQualityFile)`** — Compare a computed QC CSV to a human QC CSV and print accuracy.

#### Graphing (optional)

- **`generateGraphDataPercentage(...)`** — Sweep percentage threshold; run processing and accuracy tests.
- **`generateGraphDataTime(...)`** — Sweep time threshold; run processing and accuracy tests.
- **`graphData()`** — Generate accuracy comparison plots (reads from `accuracy_summary` CSVs).

## Package modules

### Core (`traffic_research.core`)

- **models**: `AccuracyScore` — Tracks per-folder and overall accuracy.
- **scoring**: Time and condition similarity (`computeTimeScore`, `computeConditionScore`, `computeFeatureScores`).
- **matching**: `generateReferenceGraph`, `exportGraphToCsv`, `compareParameters`, `compareTimeDistance`.
- **utils**: `secondsToTimeString`, `enumToString`.
- **data_engineering**: `DataEngining` (load, parse, logic rules), `generateDateFrameList`, `generateDateFrame`.

### Processing (`traffic_research.processing`)

- **data_processing**: `computeDataFolderToCSV`, `computeDataFolderToCSVWithIndex`, `performAccuracyTest`.
- **quality_control**: `constructRowDict`, `generateQualityControlDataFramebyGraph`, `accuracyTest`.

### Graphing (`traffic_research.graphing`)

- **graphing**: `generateGraphDataPercentage`, `generateGraphDataTime`, `graphData`.

## Input / output

### Input

- **Location**: `./resource/inputData/` (override in `config.py`).
- **Layout**: One subfolder per location; each subfolder contains **3 CSV files** (e.g. from three reviewers).

### Output

- **Location**: `./output/` (override in `config.py`).
- **Per folder**:
  - `{folderName}.csv` — Combined quality-control DataFrame (consensus rows).
  - `{folderName}NoneBusUserCrossing_graph.csv`, `{folderName}BusUserCrossing_graph.csv`, `{folderName}BusNotCrossing_graph.csv` — Reference match graphs.
- **Summary**: `output/interated_summary.csv` — Location and accuracy per folder.
- **Graphing**: If using the graphing module, CSVs and PNGs go under `output/accuracy_summary/`.

## Troubleshooting

- **ModuleNotFoundError**: Activate the virtual environment and run `pip install -r requirements.txt`.
- **Import errors**: Run scripts from the project root.
- **File not found**: Check paths in `config.py` and that input folders contain 3 CSVs each.
- **Permission errors**: Use a virtual environment and ensure write access to `OUTPUT_PATH`.

## Notes

- Data is processed as pandas DataFrames; times are stored as seconds since midnight.
- Matching is **graph-based** and uses a **time window** (see `ALGORITHM_SUMMARY.md` for details).
- Fields in `EXCLUDED_FROM_ACCURACY` (e.g. Video Title, Initials, Location Name, User Notes) are not used when computing accuracy.

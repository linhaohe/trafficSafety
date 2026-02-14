# Traffic Research Algorithm Summary

## Overview

This system processes traffic crossing data from multiple reviewers (typically 3 CSV files per location) to generate a consensus reference dataset. It uses **graph-based, time-window matching** with a weighted similarity score to match corresponding rows across reviewers, then derives consensus values via `constructRowDict` and exports reference graphs and quality-control CSVs.

## Main Algorithm Flow

### 1. Data Preprocessing (`data_engineering.py`)

- **Input**: CSV files (typically 3 reviewers per folder: A, B, C)
- **Processing**:
  - Load and transpose CSV (encoding `cp1252`, `low_memory=False`)
  - Normalize strings and parse enums (boolean, user type, gender, age, clothing color, etc.) to numeric values
  - Parse time strings to seconds since midnight (handles 12-hour and 24-hour formats, including edge cases like `15:20:20 PM`)
  - Apply logic rules (e.g., Bus Interaction from Type of Bus Interaction, Bus Presence, Roadway Crossing, Refuge Island)
- **Output**: List of `{path, df}` entries with cleaned, typed DataFrames

### 2. Data Preparation and Splitting (`data_processing.py`)

For each folder of 3 CSVs:

1. **Load** all CSVs into a list of DataFrames via `generateDateFrameList`.
2. **Split** each DataFrame into three subsets by **Bus Interaction** and **Roadway Crossing**:
   - **NoneBusUserCrossing**: `Bus Interaction == 0`
   - **BusUserCrossing**: `Bus Interaction == 1` and `Roadway Crossing == 1`
   - **BusNotCrossing**: `Bus Interaction == 1` and `Roadway Crossing == 0`
3. **Sort** rows within each subset:
   - NoneBusUserCrossing and BusUserCrossing: by `Crossing Start Time`
   - BusNotCrossing: by `Bus Stop Arrival Time`
4. **Sort** the three DataFrames in each subset by length (shortest first).
5. For each subset, build a **reference graph** and then a **quality-control DataFrame**.

No index-range or swap step is used; matching is purely **time-window based** (see below).

### 3. Row Matching: Reference Graph (`matching.py` — `generateReferenceGraph`)

Matching is done by **time window** and **similarity score**, not by index range.

#### 3.1 Time-Window Strategy

- Each DataFrame in the list is sorted by a **time column** (`Crossing Start Time` or `Bus Stop Arrival Time`).
- For a row with `targetTime`, only rows in the target DataFrame whose time lies in  
  `[targetTime - timeThreshold, targetTime + timeThreshold]` are considered.
- A **binary search** finds the first index in the sorted target column with value ≥ `targetTime - timeThreshold`; then a linear scan within the window computes scores and picks the best match.

#### 3.2 Graph Structure

- **Nodes**: `(dfName, index)` — one node per row in the three DataFrames.
- **Edges**: For each node, up to two outgoing edges to the other two DataFrames (A→B, A→C, B→C). Each edge stores `{key: {dfName, index}, score}`.
- **One-to-one**: A row in B or C can be used as the best match for at most one row in the source DataFrame (`used_targets` set).

#### 3.3 Matching Process

1. **Helper(fromDF, toDF)** runs for (dflist[0], dflist[1]), (dflist[0], dflist[2]), (dflist[1], dflist[2]).
2. For each row in `fromDF`:
   - Read `targetTime` from the chosen time column.
   - If invalid (NaN or &lt; 0), record a no-match edge and continue.
   - Use **binary search** to get the start index in `toDF` for the window.
   - Scan forward in the window; for each candidate row, compute `computeFeatureScores(from_row, to_row, timeThreshold)`.
   - If score ≥ `percentageThreshold` and better than current best, update best match; if score ≥ 1.0, stop scanning.
   - Mark the chosen target `(toDFName, index)` as used; record the edge in the graph.
3. **Result**: A graph where each node has 0–2 matches (to the other two DataFrames). This graph is later used to build QC rows and is exported to CSV.

### 4. Similarity Scoring System (`scoring.py`)

#### 4.1 Time Score (`computeTimeScore`)

- **Fields compared**: Crossing Start Time, Bus Stop Arrival/Departure, Intend to Cross, Refuge Island Start/End, Crossing End.
- **Per field**: If both values are not -1, then if `abs_diff < threshold` → 1.0, else `exp(-abs_diff / (threshold + 10))`.
- **Aggregation**: Average over valid fields only.
- **Weight**: 50% of final score (`TIME_SCORE_WEIGHT`).

#### 4.2 Condition Score (`computeConditionScore`)

- **Fields compared**: User Type, Gender, Age Group, Bus Interaction, Roadway Crossing, Type of Bus Interaction, Crossing Interaction Notes, Crossing Location Relative to Bus Stop, Vehicle Traffic, Crosswalk Crossing, Did User Finish During Pedestrian Phase, Bus Presence.
- **Per field**: 1.0 if equal, 0.0 otherwise; average over these fields.
- **Clothing Color**: Exponential decay on brightness (1–10); weighted 30% of condition score (`COLOR_WEIGHT`); other conditions 70%.
- **Weight**: 50% of final score (`CONDITION_SCORE_WEIGHT`).

#### 4.3 Final Feature Score (`computeFeatureScores`)

```text
final_score = (timeScore × 0.5) + (conditionScore × 0.5)
```

- Range [0, 1]; must be ≥ `percentageThreshold` (e.g. 0.65) to count as a match.

### 5. Consensus and QC from Graph (`quality_control.py`)

#### 5.1 Building QC Rows (`generateQualityControlDataFramebyGraph`)

- **Input**: Reference graph from `generateReferenceGraph` and the list of three `{path, df}` for that subset.
- For each graph node (row in one reviewer), if it has at least one valid match:
  - Resolve the node and its matches to actual rows: `row0` (source), `row1` (first match if valid), `row2` (second match if valid). When the second match is missing, it may be inferred from the graph (transitive match).
  - Call **constructRowDict(row0, row1, row2, index, accuracy, timeThreshold)** to get one consensus row.
- **Output**: A DataFrame of consensus rows (QC table) and side-effect updates to `AccuracyScore` for accuracy tracking.

#### 5.2 Parameter Consensus (`compareParameters`)

For each non-time field, consensus from three reviewers (A, B, C):

- **All three agree** → return that value; track as full agreement.
- **Two agree** → return the agreeing value (with transitive rules A–B–C); track as partial agreement.
- **One pair matches** → return that pair’s value; track as partial agreement.
- **No matches** → return empty string; track as disagreement.

Certain fields are excluded from accuracy tracking (see `config.EXCLUDED_FROM_ACCURACY`).

#### 5.3 Time Consensus (`compareTimeDistance`)

- Pairwise distances |A−B|, |A−C|, |B−C|.
- If all three within `timeThreshold`: return time with minimum average distance.
- If one pair within threshold: return the value from that pair with smaller average distance.
- Missing values (-1) are imputed from the other values when possible.

### 6. Output Generation

For each input folder:

- **Per-subset reference graphs** (CSV):  
  `{folderName}NoneBusUserCrossing_graph.csv`, `{folderName}BusUserCrossing_graph.csv`, `{folderName}BusNotCrossing_graph.csv`  
  (via `exportGraphToCsv`: from_dfName, from_index, to_dfName_1, to_index_1, score_1, …).

- **Single combined QC CSV**: `{folderName}.csv` — transposed quality-control DataFrame (consensus rows from all three subsets, sorted by `sort_key` then column dropped).

- **Accuracy**: Appended to an `AccuracyScore`; at the end of processing all folders, a summary is written to `interated_summary.csv` in the output path.

## Key Parameters

- **`percentageThreshold`**: Minimum similarity score (0–1) for row matching (e.g. 0.65 in `main.py`).
- **`timeThreshold`**: Time window half-width in seconds (e.g. 10 in `main.py`); also used inside time scoring.
- **`timeColumn`**: Column used for sorting and time window (e.g. `"Crossing Start Time"` or `"Bus Stop Arrival Time"`).
- **`TIME_SCORE_WEIGHT`**: 0.5.
- **`CONDITION_SCORE_WEIGHT`**: 0.5.
- **`COLOR_WEIGHT`**: 0.3 within condition score.

## Algorithm Characteristics

1. **Time-window matching**: Uses a symmetric time window and binary search for candidate rows; no index-range parameter.
2. **Graph-based**: Explicit reference graph (nodes = rows, edges = best matches) supports transitive resolution and export.
3. **Three subsets**: NoneBusUserCrossing, BusUserCrossing, BusNotCrossing each have their own graph and QC table.
4. **Weighted scoring**: 50% time, 50% condition (with clothing color 30% of condition).
5. **One-to-one matches**: `used_targets` ensures each row is used at most once as a match target.
6. **Consensus via constructRowDict**: Same comparison logic for parameters and time; QC rows built from the graph.

## Example Workflow

1. Point `main.py` at `INPUT_DATA_PATH` and `OUTPUT_PATH` (e.g. via `config.py`).
2. For each folder in the input path, load 3 CSVs → 3 DataFrames; split into NoneBusUserCrossing, BusUserCrossing, BusNotCrossing; sort each subset by length and by the appropriate time column.
3. For each subset, run `generateReferenceGraph(...)` with `timeThreshold` and `percentageThreshold` → reference graph.
4. Run `generateQualityControlDataFramebyGraph(graph, dflist, accuracy, timeThreshold)` → QC DataFrame; append accuracy for the folder.
5. Export the three graphs to CSV and the combined QC to `{folderName}.csv`.
6. After all folders, write `interated_summary.csv` and run `performAccuracyTest` if human QC files are configured.

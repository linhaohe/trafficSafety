# Traffic Research Algorithm Summary

## Overview
This system processes traffic crossing data from multiple reviewers (typically 3 CSV files) to generate a consensus reference dataset. It uses a weighted similarity scoring system to match corresponding rows across reviewers and determine consensus values.

## Main Algorithm Flow

### 1. Data Preprocessing (`data_engineering.py`)
- **Input**: Multiple CSV files (typically 3 reviewers: A, B, C)
- **Processing**:
  - Parse CSV files into pandas DataFrames
  - Convert time strings to seconds since midnight (handles 12-hour and 24-hour formats)
  - Parse enum fields (boolean, categorical) to numeric values
  - Handle missing/invalid values (marked as -1)
- **Output**: List of cleaned DataFrames

### 2. Data Preparation (`data_processing.py`)
- Sort DataFrames by length (shortest to longest)
- **Swap**: Exchange positions of `dflist[0]` and `dflist[1]` (after sorting)
- Calculate `range_value = (longest_df_length - shortest_df_length) + 2`
- This range determines the search window for matching rows

### 3. Row Matching Algorithm (`matching.py` - `generateReferenceDataFrame`)

#### 3.1 Range-Based Matching Strategy
For each row at index `i` in dataframe A:
- Compare with rows in B within range `[i - range_value, i + range_value]`
- Compare with rows in C within range `[i - range_value, i + range_value]`
- This allows for slight misalignments between reviewers' data

#### 3.2 Matching Process
1. **B to C Matching** (pre-computation):
   - For each row `i` in B, find best match in C within range
   - Store matches in `bc_matches` dictionary

2. **A to B and A to C Matching**:
   - For each row `i` in A:
     - Find best match in B (highest score ≥ `percentageThreshold`)
     - Find best match in C (highest score ≥ `percentageThreshold`)
     - Mark matched rows as "visited" to prevent duplicate matches
     - Include B→C match information

3. **Visit Tracking**:
   - Once a row is matched, it cannot be used again
   - Ensures one-to-one matching relationships

### 4. Similarity Scoring System (`scoring.py`)

#### 4.1 Time Score (`computeTimeScore`)
- **Fields compared**: 6 time fields (Crossing Start, Bus Arrival/Departure, Refuge Island Start/End, Crossing End)
- **Scoring method**:
  - For each field: if both values are not -1, calculate similarity
  - If `abs_diff < threshold`: score = 1.0 (perfect match)
  - Otherwise: score = `exp(-abs_diff / (threshold + 10))` (exponential decay with 10-second buffer)
- **Aggregation**: Average of all valid field scores (fields where both are -1 are skipped)
- **Weight**: Applied at final combination (50% of total score)

#### 4.2 Condition Score (`computeConditionScore`)
- **Fields compared**: 12 categorical/boolean fields (User Type, Gender, Age, Bus Interaction, etc.)
- **Scoring method**:
  - Boolean match: 1.0 if equal, 0.0 otherwise
  - Average all condition field scores
- **Clothing Color** (special handling):
  - Uses exponential decay based on brightness ranking (1-10 scale)
  - Weighted 30% of condition score
  - Other conditions weighted 70%
- **Weight**: Applied at final combination (50% of total score)

#### 4.3 Final Feature Score (`computeFeatureScores`)
```
final_score = (timeScore × 0.5) + (conditionScore × 0.5)
```
- Range: [0, 1] where 1.0 = perfect match
- Must exceed `percentageThreshold` (default 0.65) to be considered a match

### 5. Consensus Determination (`quality_control.py`)

#### 5.1 Parameter Comparison (`compareParameters`)
For each field, determine consensus from three reviewers (A, B, C):

1. **All three agree** (match_count = 3):
   - Return that value
   - Track as perfect agreement

2. **Two agree** (match_count = 2):
   - If A matches both B and C → return A
   - If A matches B, and B matches C → return A (transitive)
   - If A matches C, and B matches C → return C
   - Track as partial agreement

3. **One pair matches** (match_count = 1):
   - Return the value from the matching pair
   - Track as partial agreement

4. **No matches** (match_count = 0):
   - Return empty string
   - Track as disagreement

#### 5.2 Time Comparison (`compareTimeDistance`)
For time fields, uses distance-based consensus:
- Calculate pairwise distances: |A-B|, |A-C|, |B-C|
- If all three within `timeThreshold`: return time with minimum average distance
- If one pair within threshold: return value from that pair with smaller average distance
- Handle missing values (-1) by imputing from available values

### 6. Output Generation

#### 6.1 Reference DataFrame (`refDF`)
- Contains consensus values for all fields
- One row per matched set of three reviewer rows
- Includes matching indices and similarity scores

#### 6.2 Quality Control DataFrame (`dfQualityControl`)
- Transposed matrix showing:
  - Row indices: Field names
  - Column indices: Reviewer names (A, B, C)
  - Values: Original reviewer values for comparison

#### 6.3 Accuracy Tracking
- Tracks agreement levels across all fields
- Calculates overall accuracy percentage
- Excludes certain metadata fields from accuracy calculation

## Key Parameters

- **`percentageThreshold`**: Minimum similarity score (0-1) for row matching (default: 0.65)
- **`timeThreshold`**: Maximum time difference in seconds for time matching (default: 6)
- **`range_value`**: Search window size for matching (calculated dynamically)
- **`TIME_SCORE_WEIGHT`**: 0.5 (50% of final score)
- **`CONDITION_SCORE_WEIGHT`**: 0.5 (50% of final score)
- **`COLOR_WEIGHT`**: 0.3 (30% of condition score)

## Algorithm Characteristics

1. **Robust to Misalignment**: Range-based matching handles cases where reviewers have different numbers of observations
2. **Weighted Scoring**: Combines temporal and categorical features with equal weight
3. **Exponential Decay**: Time differences beyond threshold still contribute to score (with buffer)
4. **Consensus-Based**: Uses majority agreement when possible, falls back to best match when needed
5. **One-to-One Matching**: Prevents duplicate matches through visit tracking
6. **Missing Data Handling**: Skips invalid fields (-1) in scoring, handles gracefully in consensus

## Example Workflow

1. Load 3 CSV files → 3 DataFrames
2. Sort by length, swap first two
3. For each row in shortest DataFrame:
   - Find best match in other two DataFrames (within range)
   - Calculate similarity scores
   - If score ≥ threshold, create match
4. For each matched triplet:
   - Compare all fields to determine consensus
   - Generate reference row with consensus values
5. Output reference DataFrame and quality control matrix

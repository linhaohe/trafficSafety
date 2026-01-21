import pandas as pd
import re
from enum import Enum

class DataEngining:
    """Data engineering class for processing traffic research data."""

    # ---------------- ENUM DEFINITIONS ----------------
    class UserType(Enum):
        """User type enumeration."""
        pedestrian = 0
        bicyclist = 1
        other = -1

    class Gender(Enum):
        """Gender enumeration."""
        male = 0
        female = 1
        other = -1

    class Boolean(Enum):
        """Boolean enumeration."""
        no = 0
        yes = 1
        unknown = -1
        other = -1

    class BusInteractions(Enum):
        """Bus interaction type enumeration."""
        boarded = 0
        alighted = 1
        waited = 2
        other = -1

    class WalkInteractions(Enum):
        """Walking interaction type enumeration."""
        walk = 0
        run = 1
        courtesy = 2
        cyclist = 3
        other = -1

    class CrossingLocationRelativeToBus(Enum):
        """Crossing location relative to bus enumeration."""
        front = 0
        behind = 1
        other = -1

    class CrossingLocationRelativeToBusStop(Enum):
        """Crossing location relative to bus stop enumeration."""
        upstream = 0
        downstream = 1
        other = -1

    class TrafficVolume(Enum):
        """Traffic volume enumeration."""
        light = 0
        medium = 1
        high = 2
        other = -1
    
    class AgeGroup(Enum):
        """Age group enumeration."""
        age_0_20 = 0  # 0-20
        age_21_35 = 1  # 21-35
        age_36_50 = 2  # 36-50
        age_50_plus = 3  # >50
        other = -1

    class ClothingColor(Enum):
        """Clothing color enumeration ranked by brightness (high -> low)."""
        white = 10
        yellow = 9
        orange = 8
        red = 7
        pink = 7
        light_color = 7
        green = 6
        blue = 5
        purple = 4
        brown = 3
        grey = 2
        gray = 2
        black = 1
        other = -1

    # Backward compatibility aliases
    userType = UserType
    gender = Gender
    boolean = Boolean
    busInteractions = BusInteractions
    walkInteractions = WalkInteractions
    crossingLocationRelativeToBus = CrossingLocationRelativeToBus
    crossingLocationRelativeToBusStop = CrossingLocationRelativeToBusStop
    trafficVolume = TrafficVolume
    ageGroup = AgeGroup
    clothingColor = ClothingColor

    # ---------------- HELPER FUNCTIONS ----------------
    @staticmethod
    def load_csv(file_path):
        """Load and transpose CSV file, setting first row as column headers."""
        df = pd.read_csv(file_path, header=None).transpose()
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.reset_index(drop=True)
        df.dropna(how='all', inplace=True)
        return df

    @staticmethod
    def normalize_string(x):
        """Normalize string values: strip, lowercase, and replace common variations."""
        if not isinstance(x, str):
            return x
        x = x.strip().lower().replace("  ", " ")
        x = x.replace("hard to tell", "other")
        x = x.replace("n/a", "other")
        x = x.replace("none", "other")
        return x

    @staticmethod
    def parseEnum(value, enum_type):
        """Parse a value into an enum type, returning the enum value or -1 for other/unknown."""
        if pd.isna(value):
            return -1
        
        # Special handling for AgeGroup
        if enum_type == DataEngining.AgeGroup:
            normalized = DataEngining.normalize_string(value).replace(" ", "")
            # Map age group strings to enum values
            if normalized == "0-20":
                return DataEngining.AgeGroup.age_0_20.value
            elif normalized == "21-35":
                return DataEngining.AgeGroup.age_21_35.value
            elif normalized == "36-50":
                return DataEngining.AgeGroup.age_36_50.value
            elif normalized in [">50", "50>", "50+"]:
                return DataEngining.AgeGroup.age_50_plus.value
            else:
                return DataEngining.AgeGroup.other.value
        
        # Standard parsing for other enums
        key = DataEngining.normalize_string(value).replace(" ", "")
        for name, member in enum_type.__members__.items():
            if name.lower() == key:
                return member.value
        return enum_type.other.value

    @staticmethod
    def parseTimeObject(pd_timestamp):
        """Parse pandas timestamp to seconds since midnight.
        
        Handles invalid inputs by converting them:
        - "15:20:20 PM" -> treated as 24-hour time (ignores erroneous PM suffix)
        - Validates hours > 23, minutes > 59, seconds > 59
        """
        if pd.isna(pd_timestamp):
            return -1
        
        try:
            time_str = str(pd_timestamp).strip()
            time_upper = time_str.upper()
            has_am_pm = 'AM' in time_upper or 'PM' in time_upper
            
            # Check for 24-hour format with erroneous AM/PM suffix
            # Example: "15:20:20 PM" should be treated as 24-hour time
            if has_am_pm:
                # Pattern: HH:MM:SS AM/PM or HH:MM AM/PM
                time_match = re.match(r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)', time_upper)
                if time_match:
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2))
                    second_str = time_match.group(3)
                    second = int(second_str) if second_str else 0
                    am_pm = time_match.group(4)
                    
                    # If hour > 12, treat as 24-hour time with erroneous AM/PM suffix
                    # Remove the AM/PM suffix and parse as 24-hour time
                    if hour > 12:
                        # Remove AM/PM suffix and parse as 24-hour time
                        time_str_clean = re.sub(r'\s*(AM|PM)', '', time_str, flags=re.IGNORECASE)
                        time_str = time_str_clean
                    elif hour < 1:
                        return -1
                    else:
                        # Handle 12-hour format with AM/PM (hour <= 12)
                        # 12:xx:xx AM -> 00:xx:xx, 12:xx:xx PM -> 12:xx:xx
                        # 1-11:xx:xx AM -> 01-11:xx:xx, 1-11:xx:xx PM -> 13-23:xx:xx
                        if am_pm == 'PM':
                            if hour != 12:
                                hour = hour + 12
                        else:  # AM
                            if hour == 12:
                                hour = 0
                        # Validate minutes and seconds
                        if minute > 59 or second > 59:
                            return -1
                        # Return directly without pandas parsing for AM/PM times
                        return hour * 3600 + minute * 60 + second
            
            # Try to parse with pandas (for times without AM/PM or > 12 hours)
            t = pd.to_datetime(time_str, errors='coerce')
            if pd.isna(t):
                return -1
            
            # Validate parsed time components
            hour = t.hour
            minute = t.minute
            second = t.second
            
            # Check for invalid time components (24-hour format)
            if hour > 23 or minute > 59 or second > 59:
                return -1
            
            return hour * 3600 + minute * 60 + second
            
        except (ValueError, TypeError, AttributeError):
            return -1


    # ---------------- MAIN ROW PROCESSOR ----------------
    @staticmethod
    def dataEnginingRow(row):
        """Process a single row of data, normalizing and converting to appropriate types."""
        row = row.copy()
        row = row.apply(DataEngining.normalize_string)

        def get(col):
            """Helper to safely get column value."""
            return row[col] if col in row else None

        # Parse Group Size
        try:
            row['Group Size'] = int(get('Group Size'))
        except (ValueError, TypeError):
            row['Group Size'] = -1

        # Parse enum fields
        row['User Type'] = DataEngining.parseEnum(get('User Type'), DataEngining.userType)
        row['Estimated Gender'] = DataEngining.parseEnum(get('Estimated Gender'), DataEngining.gender)
        row['Estimated Visible Distrction'] = DataEngining.parseEnum(
            get('Estimated Visible Distrction'), DataEngining.boolean
        )
        row['Estimated Age Group'] = DataEngining.parseEnum(get('Estimated Age Group'), DataEngining.ageGroup)
        row['Bus Interaction'] = DataEngining.parseEnum(get('Bus Interaction'), DataEngining.boolean)
        row['Roadway Crossing'] = DataEngining.parseEnum(get('Roadway Crossing'), DataEngining.boolean)
        row['Clothing Color'] = DataEngining.parseEnum(get('Clothing Color'), DataEngining.clothingColor)

        # Special handling for Type of Bus Interaction
        tbi = get('Type of Bus Interaction')
        if tbi == 'waited at bus stop':
            row['Type of Bus Interaction'] = DataEngining.busInteractions.waited.value
        else:
            row['Type of Bus Interaction'] = DataEngining.parseEnum(tbi, DataEngining.busInteractions)

        # Parse time fields
        time_cols = [
            'Bus Stop Arrival Time', 'Bus Stop Departure Time',
            'Intend to Cross Timestamp', 'Crossing Start Time',
            'Refuge Island Start Time', 'Refuge Island End Time',
            'Crossing End Time'
        ]

        for col in time_cols:
            sec = DataEngining.parseTimeObject(get(col))
            row[col] = sec

        # Parse boolean enum fields
        row['Crosswalk Crossing'] = DataEngining.parseEnum(get('Crosswalk Crossing'), DataEngining.boolean)
        row['Refuge Island'] = DataEngining.parseEnum(get('Refuge Island'), DataEngining.boolean)
        row['Pedestrian Phase Crossing'] = DataEngining.parseEnum(
            get('Pedestrian Phase Crossing'), DataEngining.boolean
        )
        row['Did User Finish Crossing During Pedestrian Phase'] = DataEngining.parseEnum(
            get('Did User Finish Crossing During Pedestrian Phase'), DataEngining.boolean
        )

        # Special handling for Crossing Interaction Notes
        cin = get('Crossing Interaction Notes')
        if cin == 'courtesy run':
            row['Crossing Interaction Notes'] = DataEngining.walkInteractions.courtesy.value
        else:
            row['Crossing Interaction Notes'] = DataEngining.parseEnum(cin, DataEngining.walkInteractions)

        row['Bus Presence'] = DataEngining.parseEnum(get('Bus Presence'), DataEngining.boolean)

        # Special handling for Crossing Location Relative to Bus
        loc = get('Crossing Location Relative to Bus')
        if loc == 'in front':
            row['Crossing Location Relative to Bus'] = DataEngining.crossingLocationRelativeToBus.front.value
        else:
            row['Crossing Location Relative to Bus'] = DataEngining.parseEnum(
                get('Crossing Location Relative to Bus'), DataEngining.crossingLocationRelativeToBus
            )

        row['Crossing Location Relative to Bus Stop'] = DataEngining.parseEnum(
            get('Crossing Location Relative to Bus Stop'),
            DataEngining.crossingLocationRelativeToBusStop
        )

        row['Vehicle Traffic'] = DataEngining.parseEnum(get('Vehicle Traffic'), DataEngining.trafficVolume)

        return row


# Column type definitions
INT_COLS = [
    'Group Size', 'User Type', 'Estimated Gender', 'Estimated Visible Distrction',
    'Bus Interaction', 'Roadway Crossing', 'Type of Bus Interaction', 'Refuge Island',
    'Crosswalk Crossing', 'Pedestrian Phase Crossing',
    'Did User Finish Crossing During Pedestrian Phase',
    'Crossing Interaction Notes', 'Bus Presence',
    'Crossing Location Relative to Bus', 'Vehicle Traffic',
    'Clothing Color'
]

FLOAT_COLS = [
    'Bus Stop Arrival Time', 'Bus Stop Departure Time', 'Intend to Cross Timestamp',
    'Crossing Start Time', 'Refuge Island Start Time', 'Refuge Island End Time',
    'Crossing End Time',
]

# Backward compatibility
int_cols = INT_COLS
float_cols = FLOAT_COLS

DTYPE_MAPPING = {
    **{col: "Int64" for col in INT_COLS},
    **{col: "float64" for col in FLOAT_COLS}
}

# Backward compatibility
dtypeMapping = DTYPE_MAPPING


def generateDateFrameList(path_urls):
    """Generate a list of DataFrames from a list of file paths."""
    df_list = []
    for path in path_urls:
        load_df = DataEngining.load_csv(path)
        load_df = load_df.apply(DataEngining.dataEnginingRow, axis=1)
        load_df = load_df.astype(DTYPE_MAPPING)
        df_list.append(load_df)
    return df_list


def generateDateFrame(path_url):
    """Generate a single DataFrame from a file path."""
    df = DataEngining.load_csv(path_url)
    df = df.apply(DataEngining.dataEnginingRow, axis=1)
    df = df.astype(DTYPE_MAPPING)
    return df

import pandas as pd
import numpy as np
from enum import Enum

class DataEngining:

    # ---------------- ENUM DEFINITIONS ----------------
    class userType(Enum):
        pedestrian = 0
        bicyclist = 1
        other = -1

    class gender(Enum):
        male = 0
        female = 1
        other = -1

    class boolean(Enum):
        no = 0
        yes = 1
        unknow = -1
        other = -1

    class busInteractions(Enum):
        boarded = 0
        alighted = 1
        waited = 2
        other = -1

    class walkInteractions(Enum):
        walk = 0
        run = 1
        courtesy = 2
        other = -1

    class crossingLocationRelativeToBus(Enum):
        front = 0
        behind = 1
        other = -1
    class crossingLocationRelativeToBusStop(Enum):
        upstream = 0
        downstream = 1
        other = -1

    class trafficVolume(Enum):
        light = 0
        medium = 1
        high = 2
        other = -1

    class clothingColor(Enum):
        orange = 10
        purple = 9
        white = 8
        yellow = 7
        green = 6
        red = 5
        blue = 4
        brown = 3
        grey = 2
        gray = 2
        black = 1
        other = -1

    # ---------------- HELPER FUNCTIONS ----------------
    @staticmethod
    def load_csv(filePath):
        df = pd.read_csv(filePath, header=None).transpose()
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.reset_index(drop=True)
        df.dropna(how='all', inplace=True)
        return df

    @staticmethod
    def normalize_string(x):
        if not isinstance(x, str):
            return x
        x = x.strip().lower().replace("  ", " ")
        x = x.replace("hard to tell", "other")
        x = x.replace("n/a", "other")
        x = x.replace("none", "other")
        return x

    @staticmethod
    def parseEnum(value, enumType):
        if pd.isna(value):
            return -1
        key = DataEngining.normalize_string(value).replace(" ", "")
        for name, member in enumType.__members__.items():
            if name.lower() == key:
                return member.value
        return enumType.other.value

    @staticmethod
    def parseTimeObject(pdTimeStamp):
        if pd.isna(pdTimeStamp):
            return -1
        try:
            t = pd.to_datetime(str(pdTimeStamp).strip(), errors='coerce')
            if pd.isna(t):
                return -1
            return t.hour * 3600 + t.minute * 60 + t.second
        except:
            return -1

    @staticmethod
    def encode_circular_time(seconds):
        if seconds is None or seconds < 0:
            return 0.0, 0.0
        angle = 2 * np.pi * (seconds / 86400)
        return np.sin(angle), np.cos(angle)

    # ---------------- MAIN ROW PROCESSOR ----------------
    @staticmethod
    def dataEnginingRow(row):
        row = row.copy()

        row = row.apply(DataEngining.normalize_string)

        def get(col):
            return row[col] if col in row else None

        try:
            row['Group Size'] = int(get('Group Size'))
        except:
            row['Group Size'] = -1

        row['User Type'] = DataEngining.parseEnum(get('User Type'), DataEngining.userType)
        row['Estimated Gender'] = DataEngining.parseEnum(get('Estimated Gender'), DataEngining.gender)
        row['Estimated Visible Distrction'] = DataEngining.parseEnum(get('Estimated Visible Distrction'), DataEngining.boolean)
        row['Bus Interaction'] = DataEngining.parseEnum(get('Bus Interaction'), DataEngining.boolean)
        row['Roadway Crossing'] = DataEngining.parseEnum(get('Roadway Crossing'), DataEngining.boolean)
        row['Clothing Color'] = DataEngining.parseEnum(get('Clothing Color'), DataEngining.clothingColor)

        tbi = get('Type of Bus Interaction')
        if tbi == 'waited at bus stop':
            row['Type of Bus Interaction'] = DataEngining.busInteractions.waited.value
        else:
            row['Type of Bus Interaction'] = DataEngining.parseEnum(tbi, DataEngining.busInteractions)

        time_cols = [
            'Bus Stop Arrival Time', 'Bus Stop Departure Time',
            'Intend to Cross Timestamp', 'Crossing Start Time',
            'Refuge Island Start Time', 'Refuge Island End Time',
            'Crossing End Time'
        ]

        for col in time_cols:
            sec = DataEngining.parseTimeObject(get(col))
            row[col] = sec

            sin_val, cos_val = DataEngining.encode_circular_time(sec)
            row[f"{col} (sin)"] = sin_val
            row[f"{col} (cos)"] = cos_val

        row['Crosswalk Crossing?'] = DataEngining.parseEnum(get('Crosswalk Crossing?'), DataEngining.boolean)
        row['Refuge Island'] = DataEngining.parseEnum(get('Refuge Island'), DataEngining.boolean)
        row['Pedestrian Phase Crossing?'] = DataEngining.parseEnum(get('Pedestrian Phase Crossing?'), DataEngining.boolean)
        row['Did User Finish Crossing During Pedestrian Phase?'] = DataEngining.parseEnum(get('Did User Finish Crossing During Pedestrian Phase?'), DataEngining.boolean)

        cin = get('Crossing Interaction Notes')
        if cin == 'courtesy run':
            row['Crossing Interaction Notes'] = DataEngining.walkInteractions.courtesy.value
        else:
            row['Crossing Interaction Notes'] = DataEngining.parseEnum(cin, DataEngining.walkInteractions)

        row['Bus Presence'] = DataEngining.parseEnum(get('Bus Presence'), DataEngining.boolean)

        loc = get('Crossing Location Relative to Bus')
        if loc == 'infront':
            row['Crossing Location Relative to Bus'] = 0
        elif loc == 'behind':
            row['Crossing Location Relative to Bus'] = 1
        else:
            row['Crossing Location Relative to Bus'] = -1

        row['Crossing Location Relative to Bus Stop'] = DataEngining.parseEnum(
            get('Crossing Location Relative to Bus Stop'),
            DataEngining.crossingLocationRelativeToBusStop
        )

        row['Vehicle Traffic'] = DataEngining.parseEnum(get('Vehicle Traffic'), DataEngining.trafficVolume)

        return row


int_cols = [
    'Group Size','User Type','Estimated Gender','Estimated Visible Distrction',
    'Bus Interaction','Roadway Crossing','Type of Bus Interaction','Refuge Island',
    'Crosswalk Crossing?','Pedestrian Phase Crossing?',
    'Did User Finish Crossing During Pedestrian Phase?',
    'Crossing Interaction Notes','Bus Presence',
    'Crossing Location Relative to Bus','Vehicle Traffic',
    'Clothing Color'
]

float_cols = [
    'Bus Stop Arrival Time','Bus Stop Departure Time','Intend to Cross Timestamp',
    'Crossing Start Time','Refuge Island Start Time','Refuge Island End Time',
    'Crossing End Time',

    'Bus Stop Arrival Time (sin)', 'Bus Stop Arrival Time (cos)',
    'Bus Stop Departure Time (sin)', 'Bus Stop Departure Time (cos)',
    'Intend to Cross Timestamp (sin)', 'Intend to Cross Timestamp (cos)',
    'Crossing Start Time (sin)', 'Crossing Start Time (cos)',
    'Refuge Island Start Time (sin)', 'Refuge Island Start Time (cos)',
    'Refuge Island End Time (sin)', 'Refuge Island End Time (cos)',
    'Crossing End Time (sin)', 'Crossing End Time (cos)'
]

dtypeMapping = {
    **{col: "Int64" for col in int_cols},
    **{col: "float64" for col in float_cols}
}

def generateDateFrameList(pathUrls):
    dfList = []
    for path in pathUrls:
        loadDf = DataEngining.load_csv(path)
        loadDf = loadDf.apply(DataEngining.dataEnginingRow, axis=1)
        loadDf = loadDf.astype(dtypeMapping)
        dfList.append(loadDf)
    return dfList

def generateDateFrame(pathUrls):
    dfList = []
    maxSize = 0
    for path in pathUrls:
        loadDf = DataEngining.load_csv(path)
        dfList.append(loadDf)
        maxSize = max(maxSize, len(loadDf))
    df = pd.concat(dfList, ignore_index=True)
    df = df.apply(DataEngining.dataEnginingRow, axis=1)
    df = df.astype(dtypeMapping)
    return df, maxSize

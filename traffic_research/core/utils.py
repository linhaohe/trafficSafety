"""Utility functions for data conversion and formatting."""

from .data_engineering import DataEngining


def secondsToTimeString(seconds):
    """Convert seconds to HH:MM:SS format (24-hour format).
    
    Args:
        seconds: Number of seconds since midnight (0-86399)
        
    Returns:
        Time string in HH:MM:SS format, or "N/A" if invalid
    """
    if seconds is None or seconds < 0:
        return "N/A"
    # Ensure seconds are within a single day (wrap around if needed)
    seconds = int(seconds) % 86400
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02}:{minutes:02}:{secs:02}"


def enumToString(enumVal, enumList):
    """Convert enum value to string representation."""
    if enumVal is None or enumVal == -1:
        return ""
    try:
        # Special handling for AgeGroup to return readable format
        if enumList == DataEngining.ageGroup or enumList == DataEngining.AgeGroup:
            age_mapping = {
                0: "0-20",   # age_0_20
                1: "21-35",  # age_21_35
                2: "36-50",  # age_36_50
                3: ">50",    # age_50_plus
                -1: ""       # other
            }
            return age_mapping.get(enumVal, "")
        
        # Standard enum conversion
        enum_member = enumList(enumVal)
        return enum_member.name
    except:
        return ""

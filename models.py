"""Data models for traffic research analysis."""


class AccuracyScore:
    """Tracks accuracy scores across multiple files."""
    
    def __init__(self, nofVisitedCell=0, nofDifferent=0):
        self.nofVisitedCell = nofVisitedCell
        self.nofDifferent = nofDifferent
        self.filesAccuracy = []
    
    def update(self, visitedCells, differentCells):
        self.nofVisitedCell += visitedCells
        self.nofDifferent += differentCells

    def getAccuracy(self):
        if self.nofVisitedCell == 0:
            return 0.0
        return 1.0 - (self.nofDifferent / self.nofVisitedCell)
    
    def getFilesAccuracy(self):
        return self.filesAccuracy
    
    def appendFileAccuracy(self, fileName, accuracy):
        self.filesAccuracy.append({
            'Location': fileName,
            'Accuracy': accuracy
        })
    
    def reset(self):
        self.nofVisitedCell = 0
        self.nofDifferent = 0

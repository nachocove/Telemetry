class HockeyAppError(Exception):
    def __init__(self, desc):
        self.desc = desc

    def __str__(self):
        return str(self.desc)
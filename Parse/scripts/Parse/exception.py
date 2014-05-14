class ParseException(Exception):
    def __init__(self, code=None, error=None):
        self.code = code
        self.error = error

    def __str__(self):
        return 'ParseException (code=%s, error=%s)' % (self.code, self.error)

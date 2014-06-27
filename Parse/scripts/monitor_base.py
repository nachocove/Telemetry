import Parse
from html_elements import *
from number_formatter import pretty_number


class Summary(Table):
    def __init__(self):
        Table.__init__(self)
        self.colors = list()
        self.current_color_idx = 0

    def _current_color(self):
        if len(self.colors) == 0:
            return None
        assert self.current_color_idx < len(self.colors)
        return self.colors[self.current_color_idx]

    def add_entry(self, desc, value):
        row = TableRow([TableElement(Text(desc)),
                        TableElement(Text(str(value)))])
        color = self._current_color()
        if color is not None:
            row.attrs['bgcolor'] = color
        self.add_row(row)

    def toggle_color(self):
        self.current_color_idx += 1
        if self.current_color_idx == len(self.colors):
            self.current_color_idx = 0


class Monitor:
    """
    A monitor is a module that examines telemetry events of a particular
    nature and reports information (if any). For example, an error
    monitor looks for all error logs and reports the error count, error
    rate, and a list of errors.

    This is the base class of a monitor. All derived classes must
    implement the following methods:

    1. run() - Perform queries and analyze the returned events.
    2. report() - Generate reporting information.
    3. attachment() - Optionally generate an attachment for the notification
                      email.

    The user specifies the list of monitors to run in the command line. This
    script then invokes run(), report() and attachment() for each monitor.
    """
    def __init__(self, conn, desc, start=None, end=None):
        self.conn = conn
        self.desc = desc
        self.start = start
        self.end = end

    def run(self):
        """
        Perform query to telemetry server. Analyze the returned events.
        """
        raise NotImplementedError()

    def report(self, summary):
        """
        Report the result (if any). It may display the result to stdout. A Summary
        object (which is a simple wrapper of Table) is given for this monitor to
        append to the summary section. The summary section is the first table in
        the telemetry summary email. It is a Nx2 table. A monitor can append
        as many rows as it needs. Usually, each row consists of a description
        and a value.

        A report can return a separate report that will be appended to the
        telemetry report after the summary. This must be a HTML element
        (see html_elements.py) or None (when there is no report).
        """
        raise NotImplementedError()

    def title(self):
        return self.desc[0].upper() + self.desc[1:]

    def attachment(self):
        """
        Return the file name of the attachment. If there is no attachment,
        return None. One can report the raw log from the query as attachment
        and report a summarized version in HTML table format in report().
        """
        raise NotImplementedError()

    def query_all(self,  query, count_only=False):
        events = list()

        # Get the count first
        query.limit = 0
        query.count = 1
        event_count = Parse.query.Query.objects('Events', query, self.conn)[1]
        if count_only:
            return events, event_count

        query.limit = 1000
        query.skip = 0

        # Keep querying until the list is less than 1000
        # TODO - we need a robust way to pull more than 11,000 events
        results = Parse.query.Query.objects('Events', query, self.conn)[0]
        events.extend(results)
        while len(results) == query.limit and query.skip < 10000:
            query.skip += query.limit
            results = Parse.query.Query.objects('Events', query, self.conn)[0]
            events.extend(results)
        if event_count < len(events):
            event_count = len(events)

        return events, event_count

    @staticmethod
    def compute_rate(count, start, end, unit):
        if start is None or end is None:
            return None
        if unit is 'sec':
            scale = 1.0
        elif unit is 'min':
            scale = 60.0
        elif unit is 'hr':
            scale = 3600.0
        else:
            raise ValueError('unit must be sec, min, or hr')
        return pretty_number(float(count) / (end - start) * scale) + ' / ' + unit

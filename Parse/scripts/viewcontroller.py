import analytics


class ViewController:
    VC_EVENTS = ('WILL_APPEAR', 'DID_APPEAR', 'WILL_DISAPPEAR', 'DID_DISAPPEAR', 'IN_USE')

    def __init__(self, description, logger=None):
        self.description = description
        self.logger = logger
        self.intervals = dict()
        self.samples = dict()
        self.reset_interval()
        for vc_event in ViewController.VC_EVENTS:
            self.samples[vc_event] = analytics.samples.Samples(description=vc_event)

    @staticmethod
    def update_interval(interval, utc, event):
        if event.endswith('_BEGIN'):
            if interval.start is not None:
                raise IndexError('%s is already set' % event)
            interval.start = utc.datetime
        elif event.endswith('_END'):
            if interval.stop is not None:
                raise IndexError('%s is already set' % event)
            interval.stop = utc.datetime
        else:
            raise ValueError()

    def _parse_internal(self, utc, event):
        interval = None
        for vc_event in ViewController.VC_EVENTS:
            if event.startswith(vc_event):
                interval = self.intervals[vc_event]
                break
        if interval is None:
            raise ValueError('unknown event vc event %s' % event)
        self.update_interval(interval, utc, event)
        if event == 'DID_APPEAR_END':
            self.intervals['IN_USE'].start = utc.datetime
        if event == 'WILL_DISAPPEAR_BEGIN':
            self.intervals['IN_USE'].stop = utc.datetime

    def parse(self, utc, event):
        try:
            self._parse_internal(utc, event)
            if self.is_complete():
                self.update_samples()
                self.reset_interval()
        except IndexError:
            self.update_samples()
            self.reset_interval()
            self._parse_internal(utc, event)

    def is_complete(self):
        return reduce(lambda x, y: x and y, [self.intervals[z].is_complete() for z in ViewController.VC_EVENTS], True)

    def update_samples(self):
        for vc_event in ViewController.VC_EVENTS:
            interval = self.intervals[vc_event]
            if interval.is_complete():
                try:
                    self.samples[vc_event].add(interval.value())
                except ValueError:
                    if self.logger is not None:
                        self.logger.warning('invalid interval for view controller %s event %s' %
                                            (self.description, vc_event))

    def reset_interval(self):
        for vc_event in ViewController.VC_EVENTS:
            self.intervals[vc_event] = analytics.interval.Interval()

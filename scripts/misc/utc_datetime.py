import string
import datetime
import re

import dateutil.parser
import dateutil.tz
import pytz

timezone_dict = {'FNT': -7200, 'AKDT': -28800, 'GST': 14400, 'PMDT': -7200, 'BOT': -14400, 'EGT': -3600, 'HAST': -36000,
                 'WT': 0, 'WDT': 32400, 'WITA': 28800, 'NZST': 43200, 'I': 32400, 'ECT': -18000, 'AWST': 28800,
                 'EEDT': 10800, 'KUYT': 14400, 'WAT': 3600, 'TMT': 18000, 'PETT': 43200, 'HAT': -9000, 'IOT': 21600,
                 'NZDT': 46800, 'TAHT': -36000, 'HKT': 28800, 'IRST': 12600, 'HST': -36000, 'CCT': 23400,
                 'NOVST': 25200, 'AMST': 18000, 'PMST': -10800, 'MAWT': 18000, 'FJST': 46800, 'GIT': -32400, 'D': 14400,
                 'HAA': -10800, 'HAC': -18000, 'VET': -16200, 'HAE': -14400, 'BNT': 28800, 'P': -10800, 'SLT': 19800,
                 'T': -25200, 'WET': 0, 'HAP': -25200, 'X': -39600, 'HAR': -21600, 'OMSST': 25200, 'ACDT': 37800,
                 'BRST': -7200, 'THA': 25200, 'ANAT': 43200, 'CXT': 25200, 'UYT': -10800, 'CKT': -36000, 'HOVT': 25200,
                 'VLAST': 39600, 'PYT': -14400, 'VUT': 39600, 'ALMT': 21600, 'COST': -14400, 'IRKST': 32400,
                 'NPT': 20700, 'PHT': 28800, 'KST': 32400, 'YEKST': 21600, 'EET': 7200, 'ACT': 28800, 'LHDT': 39600,
                 'VLAT': 36000, 'LHST': 37800, 'AZST': 18000, 'WFT': 43200, 'MART': -34200, 'HNT': -12600, 'PT': -28800,
                 'GET': 14400, 'HMT': 18000, 'YEKT': 18000, 'MIT': -34200, 'EGST': 0, 'TKT': -36000, 'CET': 3600,
                 'EEST': 10800, 'SCT': 14400, 'AMT': 14400, 'ChST': 36000, 'C': 10800, 'G': 25200, 'K': 36000,
                 'O': -7200, 'MAGT': 39600, 'WGT': -10800, 'S': -21600, 'NFT': 41400, 'W': -36000, 'AFT': 16200,
                 'ET': -18000, 'MHT': 43200, 'BTT': 21600, 'FKST': -10800, 'BDT': 28800, 'SST': -39600, 'TJT': 18000,
                 'TVT': 43200, 'AWDT': 32400, 'HADT': -32400, 'PST': -28800, 'HNA': -14400, 'MUT': 14400, 'HNC': -21600,
                 'GAMT': -32400, 'HNE': -18000, 'COT': -18000, 'PET': -18000, 'IDT': 10800, 'IRDT': 16200, 'RET': 14400,
                 'MYT': 28800, 'GILT': 43200, 'HNP': -28800, 'AEDT': 39600, 'HNR': -25200, 'YAKST': 36000,
                 'CST': -21600, 'HNY': -32400, 'FJT': 43200, 'IRKT': 28800, 'SAST': 7200, 'AST': -14400, 'CIST': -28800,
                 'NUT': -39600, 'JST': 32400, 'CAST': 28800, 'ANAST': 43200, 'UYST': -7200, 'MAGST': 43200,
                 'AQTT': 18000, 'PONT': 39600, 'YAPT': 36000, 'EAST': -21600, 'NT': -12600, 'MDT': -21600,
                 'GALT': -21600, 'ADT': -10800, 'B': 7200, 'CLST': -10800, 'BIOT': 21600, 'F': 21600, 'DFT': 3600,
                 'PDT': -25200, 'KGT': 21600, 'N': -3600, 'R': -18000, 'SRT': -10800, 'V': -32400, 'CLT': -14400,
                 'Z': 0, 'NDT': -9000, 'GMT': 0, 'WIB': 25200, 'SBT': 39600, 'PYST': -10800, 'MMT': 23400,
                 'BRT': -10800, 'YAKT': 32400, 'CDT': -18000, 'WIT': 32400, 'EDT': -14400, 'HLV': -16200, 'NOVT': 21600,
                 'KRAST': 28800, 'ULAT': 28800, 'KRAT': 25200, 'NCT': 39600, 'CVT': -3600, 'MST': -25200, 'WAST': 7200,
                 'CAT': 7200, 'AEST': 36000, 'MSK': 10800, 'WST': 28800, 'MVT': 18000, 'MSD': 14400, 'WEDT': 3600,
                 'AZT': 14400, 'TLT': 32400, 'SGT': 28800, 'CEDT': 7200, 'AKST': -32400, 'PGT': 36000, 'GYT': -14400,
                 'CEST': 7200, 'OMST': 21600, 'UZT': 18000, 'NST': -12600, 'EAT': 10800, 'A': 3600, 'UTC': 0,
                 'EST': -18000, 'E': 18000, 'PETST': 43200, 'DAVT': 25200, 'M': 43200, 'L': 39600, 'Q': -14400,
                 'U': -28800, 'TFT': 18000, 'Y': -43200, 'AZOT': -3600, 'ICT': 25200, 'PWT': 32400, 'ART': -10800,
                 'PKT': 18000, 'HAY': -28800, 'WEST': 3600, 'FKT': -14400, 'GFT': -10800, 'WGST': -7200, 'H': 28800,
                 'ACST': 34200, 'EASST': -18000, 'SAMT': 14400}


class UtcDateTime:
    hours = ('h', 'hr', 'hrs')
    minutes = ('m', 'min', 'mins', 'minutes')
    seconds = ('s', 'sec', 'secs', 'seconds')
    days = ('d', 'day', 'days')
    match_str = "|".join(hours + minutes + seconds + days)

    def __init__(self, value=None):
        dt = None
        if isinstance(value, (str, unicode)):
            if value.startswith('now'):
                parts = value.split('-')
                dt = datetime.datetime.utcnow().replace(microsecond=0)
                if len(parts) == 1:
                    pass
                elif len(parts) == 2 and parts[1][0] in string.digits:
                    m = re.match(r'(?P<digit>[0-9]+)(?P<hmsd>[%s])' % self.match_str, parts[1])
                    if m:
                        if m.group('hmsd') in self.hours:
                            sub = datetime.timedelta(hours=int(m.group('digit')))
                        elif m.group('hmsd') in self.minutes:
                            sub = datetime.timedelta(minutes=int(m.group('digit')))
                        elif m.group('hmsd') in self.seconds:
                            sub = datetime.timedelta(seconds=int(m.group('digit')))
                        elif m.group('hmsd') in self.days:
                            sub = datetime.timedelta(days=int(m.group('digit')))
                        else:
                            raise Exception("unknown timeframe %s" % m.group('hmsd'))
                        dt -= sub
                else:
                    raise ValueError('format %s is not valid' % value)
            else:
                ignoretz = True if str(value).endswith('Z') else False
                tzinfos = None if str(value).endswith('Z') else timezone_dict
                dt = dateutil.parser.parse(str(value), ignoretz=ignoretz, tzinfos=tzinfos)
        elif isinstance(value, UtcDateTime):
            dt = value.datetime
        elif isinstance(value, datetime.datetime):
            dt = value
        elif isinstance(value, int):
            milliseconds = value / 10000
            (days, milliseconds) = divmod(milliseconds, 86400 * 1000)
            date = datetime.date.fromordinal(days + 1)
            (hours, milliseconds) = divmod(milliseconds, 3600 * 1000)
            (minutes, milliseconds) = divmod(milliseconds, 60 * 1000)
            (seconds, milliseconds) = divmod(milliseconds, 1000)

            dt = datetime.datetime(year=date.year,
                                   month=date.month,
                                   day=date.day,
                                   hour=hours,
                                   minute=minutes,
                                   second=seconds,
                                   microsecond=milliseconds * 1000,
                                   )
        else:
            raise ValueError("Unsupported input type %s" % value.__class__)
        if dt:
            if dt.tzinfo is None:
                self.datetime = dt.replace(tzinfo=pytz.utc)
            else:
                self.datetime = dt.astimezone(pytz.utc)

    def __repr__(self):
        s = self.datetime.strftime('%Y-%m-%dT%H:%M:%S')
        if self.datetime.microsecond == 0:
            return s + 'Z'
        return s + '.%03dZ' % int(self.datetime.microsecond / 1000.)

    def __cmp__(self, other):
        return cmp(self - other, 0.0)

    def __sub__(self, other):
        """
        Return the elapsed time in seconds (with millisecond resolution).
        """
        delta = self.datetime - other.datetime
        return (float(delta.days) * 86400.0) + float(delta.seconds) + (float(delta.microseconds) / 1.e6)

    def file_suffix(self):
        return str(self).replace(':', '_').replace('-', '_').replace('.', '_')

    def toticks(self):
        days = datetime.date.toordinal(self.datetime.date()) - 1
        ticks = days * 86400
        ticks += self.datetime.hour * 3600
        ticks += self.datetime.minute * 60
        ticks += self.datetime.second
        ticks = (ticks * 1000000) + UtcDateTime._round_to_millisecond(self.datetime.microsecond)
        return ticks * 10  # convert to ticks

    @staticmethod
    def _round_to_millisecond(microsecond):
        return microsecond - (microsecond % 1000)

    @staticmethod
    def now():
        dt = datetime.datetime.utcnow()
        return UtcDateTime(dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))

    @staticmethod
    def from_ticks(ticks):
        assert isinstance(ticks, int)
        return UtcDateTime(ticks)

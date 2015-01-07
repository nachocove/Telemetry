import subprocess
import events


class AnsiDecorator:
    """
    Decorator a string using ANSI escape sequence to add color, bold
    or italic font.
    """
    COLOR_CODES = {'black': 30,
                   'red': 31,
                   'green': 32,
                   'yellow': 33,
                   'blue': 34,
                   'magenta': 35,
                   'cyan': 36,
                   'white': 37}
    ESC = chr(27)

    def __init__(self, color=None, bold=False, underscore=False):
        self.esc_on = None
        self.esc_off = None
        codes = []
        if color is not None:
            if color not in AnsiDecorator.COLOR_CODES:
                raise ValueError('invalid color code %s' % color)
            else:
                codes.append(AnsiDecorator.COLOR_CODES[color])
        if bold:
            codes.append(1)
        if underscore:
            codes.append(5)
        if len(codes) > 0:
            self.esc_on = AnsiDecorator.ESC + '[' + ','.join(str(x) for x in codes) + 'm'
            self.esc_off = AnsiDecorator.ESC + '[0m'

    def format(self, text):
        if self.esc_on:
            assert self.esc_off
            return self.esc_on + text + self.esc_off
        return text


class EventFormatterSection:
    """
    One section of the formatted event. Each section consists of the
    content and its decorator.
    """
    def __init__(self, decorator=None):
        self.decorator = decorator
        if self.decorator is None:
            self.decorator = AnsiDecorator()
        self.content_ = ''

    def format(self, field, value):
        raise Exception('not implemented')

    def content(self):
        return self.decorator.format(self.content_)

    def reset(self):
        self.content_ = ''

    @staticmethod
    def format_content(value):
        if isinstance(value, dict):
            if value['__type'] == 'Date':
                return value['iso']
            elif value['__type'] == 'Bytes':
                return value['base64']
            else:
                raise TypeError('unsupported type %s (%s)' % (value.__class__.__name__, value))
        else:
            try:
                return str(value)
            except UnicodeEncodeError:
                return value


class RecordStyleSection(EventFormatterSection):
    """
    Record style section consists of both the name of the field and the content of the field.
    """
    def format(self, field, value):
        self.content_ += '%s: %s\n' % (field, EventFormatterSection.format_content(value))


class LogStyleSection(EventFormatterSection):
    """
    Log style section consists of only the content. The field is implicitly identified by
    its position.
    """
    def format(self, field, value):
        self.content_ += EventFormatterSection.format_content(value)

    def content(self):
        return EventFormatterSection.content(self) + ' '


class LogStyleIdentSection(LogStyleSection):
    def format(self, field, value):
        if self.content_ != '':
            self.content_ += ', '
        self.content_ += EventFormatterSection.format_content(value)

    def content(self):
        if len(self.content_) == 0:
            return ''
        return self.decorator.format('[' + self.content_ + '] ')


class EventDecorator:
    def __init__(self, decorator=None):
        self.timestamp = decorator
        self.event_type = decorator
        self.ident = decorator
        self.info = decorator


class EventFormatter:
    def __init__(self,
                 timestamp_section, event_type_section,
                 ident_section, info_section, link_section, prefix,
                 default_decorator=None,
                 wbxml_tool_path=None):
        self.timestamp = timestamp_section(None)
        self.event_type = event_type_section(None)
        self.ident = ident_section(None)
        self.info = info_section(None)
        self.link = link_section(None)
        self.default_decorator = default_decorator
        self.decorators = dict()
        self.prefix=prefix
        for et in events.TYPES:
            self.decorators[et] = EventDecorator(default_decorator)
        self.wbxml_tool_path = wbxml_tool_path
        self.telemetry_viewer_url_prefix = 'http://localhost:8000/'

    def may_add(self, section, obj, field):
        if field in obj:
            section.format(field, obj[field])

    def reset(self):
        self.timestamp.reset()
        self.event_type.reset()
        self.ident.reset()
        self.info.reset()
        self.link.reset()

    def set_decorators(self, obj):
        if 'event_type' not in obj:
            return
        event_decorator = self.decorators[obj['event_type']]
        self.timestamp.decorator = event_decorator.timestamp
        self.event_type.decorator = event_decorator.event_type
        self.ident.decorator = event_decorator.ident
        self.info.decorator = event_decorator.info

    def reset_decorators(self):
        self.timestamp.decorator = self.default_decorator
        self.event_type.decorator = self.default_decorator
        self.ident.decorator = self.default_decorator
        self.info.decorator = self.default_decorator

    def decode_wbxml(self, wbxml):
        if self.wbxml_tool_path is None:
            return None
        command = ['mono', self.wbxml_tool_path, '-d', '-f', '-']
        try:
            p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            (output, error) = p.communicate(wbxml)
        except subprocess.CalledProcessError:
            return None
        return output

    def format(self, obj):
        self.reset()
        self.set_decorators(obj)

        # Format timestamp
        self.may_add(self.timestamp, obj, 'timestamp')

        # Format event type
        self.may_add(self.event_type, obj, 'event_type')

        # Format telemetry link
        link = '%sbugfix/%s/logs/%s/%s/1/' % (self.telemetry_viewer_url_prefix, self.prefix, obj['client'], obj['timestamp'])
        self.link.format('telemetry', link)

        # Format the identification section
        for field in (events.IDENT_FIELDS + events.INTERNAL_FIELDS):
            self.may_add(self.ident, obj, field)

        # Format the information section
        for field in events.INFO_FIELDS:
            self.may_add(self.info, obj, field)
        if 'wbxml' in obj:
            # WBXML is special because we may optionally decode it.
            decoded = self.decode_wbxml(obj['wbxml'].encode())
            if decoded is None:
                self.info.format('wbxml', obj['wbxml'])
            else:
                self.info.format('wbxml', obj['wbxml']['base64'] + '\n\n' + decoded)

        # Combine all sections
        output = self.timestamp.content() + \
                 self.event_type.content() + \
                 self.ident.content() + \
                 self.link.content() + \
                 self.info.content()

        self.reset_decorators()
        return output


class LogStyleEventFormatter(EventFormatter):
    def __init__(self, **kwargs):
        decorator = kwargs.get('decorator', None)
        if decorator is None:
            decorator = AnsiDecorator()
        EventFormatter.__init__(self,
                                timestamp_section=LogStyleSection,
                                event_type_section=LogStyleSection,
                                ident_section=LogStyleIdentSection,
                                info_section=LogStyleSection,
                                link_section=LogStyleSection,
                                default_decorator=decorator, **kwargs)


class RecordStyleEventFormatter(EventFormatter):
    def __init__(self, **kwargs):
        decorator = kwargs.get('decorator', None)
        if decorator is None:
            decorator = AnsiDecorator()
        EventFormatter.__init__(self,
                                timestamp_section=RecordStyleSection,
                                event_type_section=RecordStyleSection,
                                ident_section=RecordStyleSection,
                                info_section=RecordStyleSection,
                                link_section=RecordStyleSection,
                                default_decorator=decorator, **kwargs)

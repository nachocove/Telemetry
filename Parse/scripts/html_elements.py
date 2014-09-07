# This module contains a set of classes for creating HTML email with alternative
# plain text version.
#
# There are many Python package for creating HTML from Python structure / objects.
# But none of them will generate a reasonable plain text version of the content.
# This library is based on the base class Element. It provides two key methods -
# 1) html(), 2) plain_text() for creating HTML and plain text content.
# An email message can create a document using these HTML elements and generates
# both versions as MIME content (with plain text as MIME alternatives)
import cgi
import re


class Element:
    @staticmethod
    def assert_type(obj, class_):
        if not isinstance(obj, class_) and not issubclass(obj.__class__, class_):
            raise TypeError('Object (%s) must be of class %s' % (obj.__class__.__name__, str(class_)))

    @staticmethod
    def assert_list(obj, ex_msg):
        """
        Verify that the content of an element class is a list of objects.
        Raise TypeError exception if not.
        """
        if not isinstance(obj, list):
            raise TypeError(ex_msg)

    @staticmethod
    def assert_list_objects(obj_list, class_, obj_class):
        """
        Verify that the content of an element is a list of objects and each
        object is of a particular class type. Raise TypeError exception if
        not.
        """
        if isinstance(obj_class, tuple):
            obj_class_str = ','.join([x.__name__ for x in list(obj_class)])
        else:
            obj_class_str = obj_class.__name__
        Element.assert_list(obj_list, '%s expects a list of %s' % (class_.__name__, obj_class_str))
        for obj in obj_list:
            if not isinstance(obj, obj_class):
                raise TypeError('Expects %s but get %s' % (obj_class_str, obj.__class__.__name__))

    def __init__(self, tag, content):
        # Each HTML element has a tag like <P>. No "<" and ">" are needed
        # in tag though.
        self.tag = tag
        # HTML attributes go here as a dictionary. E.g. <font color="red">
        # becomes {'color': 'red'}
        self.attrs = dict()
        self.content = content

    def html(self):
        """
        Create HTML version of the element (and all its sub-elements).
        """
        val = self.start_tag()
        if isinstance(self.content, list):
            for c in self.content:
                val += c.html()
        elif issubclass(self.content.__class__, Element):
            val += self.content.html()
        else:
            val += self.content.encode('utf-8')
        val += self.end_tag()
        return val

    def plain_text(self):
        """
        Create plain-text version of the element (and all its sub-elements).
        """
        val = ''
        if isinstance(self.content, list):
            for c in self.content:
                val += c.plain_text()
        elif issubclass(self.content.__class__, Element):
            val = self.content.plain_text()
        else:
            val += self.content.encode('utf-8')
        return val

    def start_tag(self):
        """
        Create HTML start tag with optional attributes
        """
        if self.tag is None:
            return ''  # (base) Text has no HTML tag at all
        tag = '<%s' % self.tag
        if len(self.attrs) > 0:
            # If it has attributes, add them here
            for (key, value) in self.attrs.items():
                tag += ' %s="%s"' % (key, value)
        tag += '>'
        return tag

    def end_tag(self):
        """
        Create HTML end tag
        """
        if self.tag is None:
            return ''  # (base) Text class has no HTML tag at all.
        return '</%s>' % self.tag

    def attribute(self, attr_name):
        return self.attrs.get(attr_name, None)


class Html(Element):
    def __init__(self, content=None):
        if content is None:
            content = []
        Element.assert_list_objects(content, Html, Element)
        Element.__init__(self, 'html', content)

    def add(self, content):
        Element.assert_type(content, Element)
        self.content.append(content)


class Text(Element):
    """
    This is the base class of various text elements (Color, Bold, Italic
    """
    @staticmethod
    def assert_text(obj):
        Element.assert_type(obj, (str, unicode, Text))

    def __init__(self, text, keep_linefeed=False):
        Text.assert_text(text)
        Element.__init__(self, None, text)
        # keep_linefeed retain the formatting of linefeed in the original
        # text by converting them to <BR>. The default is False and let
        # the browser to deciding when to break a line.
        self.keep_linefeed = keep_linefeed

    def html(self):
        val = self.start_tag()
        if issubclass(self.content.__class__, Element):
            val += self.content.html()
        else:
            text = cgi.escape(self.content.encode('utf-8'))
            if self.keep_linefeed:
                text = re.sub('\n', '<br>', text)
            val += text
        val += self.end_tag()
        return val


class Color(Text):
    def __init__(self, text, color):
        Text.__init__(self, text)
        self.tag = 'font'
        self.attrs['color'] = color


class Bold(Text):
    def __init__(self, text):
        Text.__init__(self, text)
        self.tag = 'b'


class Italic(Text):
    def __init__(self, text):
        Text.__init__(self, text)
        self.tag = 'i'


class Link(Text):
    def __init__(self, content, link):
        Text.assert_text(content)
        Text.__init__(self, content)
        self.tag = 'a'
        self.attrs['href'] = link

    def plain_text(self):
        return Text.plain_text(self) + ' (%s)' % self.attrs['href']


class Paragraph(Element):
    def __init__(self, content):
        Element.assert_list_objects(content, Paragraph, Element)
        Element.__init__(self, 'p', content)

    def plain_text(self):
        return Element.plain_text(self) + '\n'


class UnorderedList(Element):
    def __init__(self, rows):
        Element.__init__(self, 'ul', rows)

    def plain_text(self):
        # We need to format the list items by giving each list item a bullet
        for row in self.content:
            assert isinstance(row, ListItem)
            row.header = '- '
        return Element.plain_text(self)


class OrderedList(Element):
    def __init__(self, rows):
        Element.assert_list_objects(rows, OrderedList, ListItem)
        Element.__init__(self, 'ol', rows)

    def plain_text(self):
        # We need to format the list items by giving each list item a number
        n = 1
        for row in self.content:
            assert isinstance(row, ListItem)
            row.header = '%d. ' % n
            n += 1
        return Element.plain_text(self)


class ListItem(Element):
    def __init__(self, content):
        Text.assert_text(content)
        Element.__init__(self, 'li', content)
        # The item value for ordered list.
        self.header = ''

    def plain_text(self):
        return self.header + Element.plain_text(self) + '\n'


class Table(Element):
    def __init__(self, table_rows=None):
        if table_rows is None:
            table_rows = []
        Element.assert_list_objects(table_rows, Table, TableRow)
        Element.__init__(self, 'table', table_rows)
        self.attrs = {'style': 'border-collapse: collapse',
                      'border': 1,
                      'cellpadding': 2}

    def plain_text(self):
        # We need to size the widths of table elements / headers
        # First, find the max. width for all columns
        rows = self.rows()
        if len(rows) == 0:
            return ''

        num_cols = 0
        max_widths = []
        rowspan = []
        for row in rows:
            if num_cols == 0:
                num_cols = len(row.elements())
                max_widths = [0] * num_cols
                rowspan = [0] * num_cols
            else:
                # Check all rows to have the same # of columns
                if num_cols != (len(row.elements()) + sum([x > 0 and 1 or 0 for x in rowspan])):
                    raise ValueError('all rows must have the same number of columns')

            # Insert filler elements
            for n in range(num_cols):
                if rowspan[n] != 0:
                    row.content.insert(n, TableRowSpan())

            # Get the width of all columns (filler has 0 width)
            widths = row.get_widths()
            max_widths = [max(a, b) for (a, b) in zip(widths, max_widths)]

            # Update rowspan count
            for n in range(num_cols):
                # Add new rowspan
                rs = row.element(n).attribute('rowspan')
                if rs is not None:
                    assert isinstance(rs, int)
                    rowspan[n] += rs
                # Decrement for the current row
                if rowspan[n] != 0:
                    rowspan[n] -= 1

        # Second, record the width for each table element
        for row in rows:
            row.set_widths(max_widths)

        # The problem with adding filler element is that it changes the Table object.
        # (The good is that this approach simplies the layout code a lot.)
        # So, we need to delete all the fillers
        plain_text = Element.plain_text(self)

        for row in rows:
            for n in reversed(range(len(row.content))):
                if isinstance(row.element(n), TableRowSpan):
                    row.content.pop(n)

        return plain_text

    def row(self, index):
        return self.content[index]

    def rows(self):
        return self.content

    def add_row(self, row):
        Element.assert_type(row, TableRow)
        self.content.append(row)

    def add_rows(self, rows):
        Element.assert_list_objects(rows, Table, TableRow)
        self.content.extend(rows)


class TableRow(Element):
    def __init__(self, table_elements=None):
        if table_elements is None:
            table_elements = []
        Element.assert_list_objects(table_elements, TableRow, (TableHeader, TableElement))
        Element.__init__(self, 'tr', table_elements)

    def element(self, index):
        return self.content[index]

    def elements(self):
        return self.content

    def add_element(self, element):
        Element.assert_type(element, (TableHeader, TableElement))
        self.content.append(element)

    def add_elements(self, elements):
        Element.assert_list_objects(elements, TableRow, (TableHeader, TableElement))
        self.content.extend(elements)

    def get_widths(self):
        return [len(e.plain_text()) - 1 for e in self.elements()]

    def set_widths(self, widths):
        assert len(widths) == len(self.content)
        for (element, width) in zip(self.elements(), widths):
            element.width = width

    def plain_text(self):
        return Element.plain_text(self) + '\n'


class TableHeader(Element):
    def __init__(self, content):
        Text.assert_text(content)
        Element.__init__(self, 'th', content)
        self.width = 0

    def plain_text(self):
        return ('%%-%ds ' % self.width) % self.content.plain_text()


class TableElement(Element):
    def __init__(self, content, **attrs):
        Text.assert_text(content)
        Element.__init__(self, 'td', content)
        self.width = 0
        self.attrs = attrs

    def plain_text(self):
        return ('%%-%ds ' % self.width) % self.content.plain_text()


class TableRowSpan(TableElement):
    """
    TableRowSpan is not a real HTML element. It is a space filler for implementing
    rowspan for plain text. It is an element that emits no HTML and only white
    spaces
    """
    def __init__(self):
        TableElement.__init__(self, '')

    def html(self):
        return ''

    def plain_text(self):
        return ' ' * (self.width + 1)
import unittest
from misc.html_elements import *


class TestText(unittest.TestCase):
    def setUp(self):
        self.plain_text = 'This is plain text.'
        self.bold_text = 'This is bold text.'
        self.italic_text = 'This is italic text.'
        self.red_text = 'This is red text'
        self.composite_text = 'This is bold, italic, red text.'

    def test_text(self):
        text = Text(self.plain_text)
        self.assertEqual(text.html(), self.plain_text)
        self.assertEqual(text.plain_text(), self.plain_text)

    def test_bold(self):
        bold = Bold(self.bold_text)
        self.assertEqual(bold.html(), '<b>%s</b>' % self.bold_text)
        self.assertEqual(bold.plain_text(), self.bold_text)

    def test_italic(self):
        italic = Italic(self.italic_text)
        self.assertEqual(italic.html(), '<i>%s</i>' % self.italic_text)
        self.assertEqual(italic.plain_text(), self.italic_text)

    def test_color(self):
        red = Color(self.red_text, 'red')
        self.assertEqual(red.html(), '<font color="red">%s</font>' % self.red_text)
        self.assertEqual(red.plain_text(), self.red_text)

    def test_composite(self):
        composite = Bold(Italic(Color(self.composite_text, 'red')))
        self.assertEqual(composite.html(), '<b><i><font color="red">%s</font></i></b>' % self.composite_text)
        self.assertEqual(composite.plain_text(), self.composite_text)

    def test_link(self):
        link = Link(Text('this is a link'), 'http://www.google.com')
        self.assertEqual(link.html(), '<a href="http://www.google.com">this is a link</a>')
        self.assertEqual(link.plain_text(), 'this is a link (http://www.google.com)')


class TestList(unittest.TestCase):
    def test_list_item(self):
        content = 'List item #1'
        list_item = ListItem(Bold(content))
        self.assertEqual(list_item.html(), '<li><b>%s</b></li>' % content)
        self.assertEqual(list_item.plain_text(), '%s\n' % content)

    def test_unordered_list(self):
        contents = ['List item #1', 'List item #2', 'List item #3']
        list_items = list()
        list_items.append(ListItem(Text(contents[0])))
        list_items.append(ListItem(Bold(contents[1])))
        list_items.append(ListItem(Color(contents[2], 'red')))
        unordered_list = UnorderedList(list_items)
        self.assertEqual(unordered_list.html(),
                         '<ul><li>%s</li><li><b>%s</b></li><li><font color="red">%s</font></li></ul>' % tuple(contents))
        self.assertEqual(unordered_list.plain_text(), '- %s\n- %s\n- %s\n' % tuple(contents))

    def test_ordered_list(self):
        contents = ['List item #1', 'List item #2', 'List item #3']
        list_items = list()
        list_items.append(ListItem(Text(contents[0])))
        list_items.append(ListItem(Bold(contents[1])))
        list_items.append(ListItem(Color(contents[2], 'red')))
        ordered_list = OrderedList(list_items)
        self.assertEqual(ordered_list.html(),
                         '<ol><li>%s</li><li><b>%s</b></li><li><font color="red">%s</font></li></ol>' % tuple(contents))
        self.assertEqual(ordered_list.plain_text(), '1. %s\n2. %s\n3. %s\n' % tuple(contents))


class TestTable(unittest.TestCase):
    def test_table_header(self):
        content = 'Header'
        header = TableHeader(Bold(content))
        self.assertEqual(header.html(), '<th><b>%s</b></th>' % content)
        self.assertEqual(header.plain_text(), content + ' ')

    def test_2x2(self):
        headers = TableRow([TableHeader(Bold('First column')), TableHeader(Bold('2nd col.'))])
        row1 = TableRow([TableElement(Text('a')), TableElement(Bold('xyz'))])
        row2 = TableRow([TableElement(Text('abc')), TableElement(Italic('0123456789'))])
        table = Table([headers, row1, row2])
        print table.html()

        self.assertEqual(table.html(), '<table cellpadding="2" style="border-collapse: collapse" border="1">'
                                       '<tr><th><b>First column</b></th><th><b>2nd col.</b></th></tr>'
                                       '<tr><td>a</td><td><b>xyz</b></td></tr>'
                                       '<tr><td>abc</td><td><i>0123456789</i></td></tr>'
                                       '</table>')
        self.assertEqual(table.plain_text(), 'First column 2nd col.   \n'
                                             'a            xyz        \n'
                                             'abc          0123456789 \n')

    def test_3x3(self):
        header = TableRow([TableHeader(Bold('Class')),
                           TableHeader(Bold('Method')),
                           TableHeader(Bold('Parameters'))])
        row1 = TableRow([TableElement(Text('Math'), rowspan=3),
                         TableElement(Text('Add'), rowspan=2),
                         TableElement(Text('(int a, int b)'))])
        row2 = TableRow([TableElement(Text('(float a, float b)'), rowspan=1)])
        row3 = TableRow([TableElement(Text('Subtract')), TableElement(Text('(int a, int b)'))])
        table = Table([header, row1, row2, row3])
        print table.html()
        print table.plain_text()

        print len(table.plain_text())
        print len(table.plain_text())
        print len(table.plain_text())

        self.assertEqual(table.html(),
                         '<table cellpadding="2" style="border-collapse: collapse" border="1">'
                         '<tr><th><b>Class</b></th><th><b>Method</b></th><th><b>Parameters</b></th></tr>'
                         '<tr><td rowspan="3">Math</td><td rowspan="2">Add</td><td>(int a, int b)</td></tr>'
                         '<tr><td rowspan="1">(float a, float b)</td></tr>'
                         '<tr><td>Subtract</td><td>(int a, int b)</td></tr>'
                         '</table>')
        self.assertEqual(table.plain_text(),
                         'Class Method   Parameters         \n'
                         'Math  Add      (int a, int b)     \n'
                         '               (float a, float b) \n'
                         '      Subtract (int a, int b)     \n')


class TestColorList(unittest.TestCase):
    def setUp(self):
        self.colors = ['red', 'white', 'blue']
        self.color_list = ColorList()

    def test_no_colors(self):
        self.assertEqual(None, self.color_list.color())
        self.color_list.advance()
        self.assertEqual(None, self.color_list.color())

    def test_alternate(self):
        self.color_list.configure(colors=self.colors)
        for n in range(2):
            for color in self.colors:
                self.assertEqual(color, self.color_list.color())
                self.color_list.advance()

    def test_every_n(self):
        """
        Test that every N advance results in a new colors
        """
        N = 3
        self.color_list.configure(colors=self.colors, counts=N)
        for n in range(2):
            for color in self.colors:
                for m in range(N):
                    self.assertEqual(color, self.color_list.color())
                    self.color_list.advance()

    def test_one_of_n(self):
        """
        By setting count correctly, we can get a line of highlight every N lines
        """
        N = 5
        colors = ['white', 'gray']
        self.color_list.configure(colors=colors, counts=[N-1, 1])
        for n in range(2):
            for i in range(N-1):
                self.assertEqual('white', self.color_list.color())
                self.color_list.advance()
            self.assertEqual('gray', self.color_list.color())
            self.color_list.advance()

if __name__ == '__main__':
    unittest.main()
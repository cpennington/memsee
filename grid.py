import sys

class ColumnSpec(object):
    def __init__(self, align, width):
        self.align = align
        self.width = int(width)

class GridWriter(object):
    def __init__(self, formats, out=sys.stdout, sep=" "):
        self.specs = [ColumnSpec(f[0], f[1:]) for f in formats]
        self.fmt = u""
        for spec in self.specs:
            # Change "<10" into "{!s:<10.10}"
            self.fmt += u"{{!s:{0.align}{0.width}.{0.width}}}{1}".format(spec, sep)
        self.fmt = self.fmt.strip() + "\n"
        self.out = out

    def row(self, data):
        self.out.write(self.fmt.format(*data))

    def header(self, headers):
        if headers:
            self.row(headers)
        self.lines()

    def rows(self, iterable):
        for row in iterable:
            self.row(row)

    def lines(self):
        dashes = ["-"*spec.width for spec in self.specs]
        self.row(dashes)

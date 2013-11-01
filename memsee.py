import cmd
import gzip
import json
import sqlite3
import sys
import time

# Data is like:
#
# {"address": 125817416, "type": "list", "size": 72, "len": 0, "refs": []}

SCHEMA = [
    "create table object (address int primary key, type text, name text, value text, size int, len int);",
    "create index size on object (size);",
    "create index type on object (type);",
    "create index name on object (name);",
    "create index value on object (value);",
    "create table ref (parent int, child int);",
    "create index parent on ref (parent);",
    "create index child on ref (child);",
]

def nice_num(n):
    if n < 1e4:
        return "%d" % n
    elif n < 1e6:
        return "%.1fK" % (n / 1e3)
    elif n < 1e9:
        return "%.1fM" % (n / 1e6)
    elif n < 1e12:
        return "%.1fG" % (n / 1e9)
    else:
        return "%.1fT" % (n / 1e12)


class MemSeeApp(cmd.Cmd):

    prompt = "::> "

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.dbfile = None
        self.db = None

    def emptyline(self):
        pass

    def default(self, line):
        if line == "EOF":
            # Seriously? That's how I find out about the end of input?
            print
            return True
        print "I don't understand %r" % line

    def do_create(self, line):
        """Create a new database: create DBFILE"""
        if not line:
            print "Need a db to create"
            return
        words = line.split()
        if len(words) > 1:
            self.default(line)
        self.dbfile = words[0]
        self.db = sqlite3.connect(self.dbfile)
        c = self.db.cursor()
        for stmt in SCHEMA:
            c.execute(stmt)
        self.db.commit()

    def do_open(self, line):
        """Open a database: open DBFILE"""
        if not line:
            print "Need a db to open"
            return
        words = line.split()
        if len(words) > 1:
            self.default(line)
        self.dbfile = words[0]
        self.db = sqlite3.connect(self.dbfile)

    def do_read(self, line):
        """Read a data file: read DATAFILE"""
        if not self.db:
            print "Can't read a file until you open a database."
            return
        if not line:
            print "Need a file to read"
            return
        words = line.split()
        if len(words) > 1:
            self.default(line)
            return
        filename = words[0]
        if filename.endswith(".gz"):
            opener = gzip.open
        else:
            opener = open

        objs = refs = bytes = 0
        start = time.time()
        sys.stdout.write("Reading")
        sys.stdout.flush()
        c = self.db.cursor()
        with opener(filename) as data:
            for line in data:
                objdata = json.loads(line)
                c.execute(
                    "insert into object (address, type, name, value, size, len) values (?, ?, ?, ?, ?, ?)",
                    (objdata['address'], objdata['type'], objdata.get('name'), objdata.get('value'), objdata['size'], objdata.get('len', 0))
                )
                objs += 1
                bytes += objdata['size']
                for ref in objdata['refs']:
                    c.execute(
                        "insert into ref (parent, child) values (?, ?)",
                        (objdata['address'], ref)
                    )
                    refs += 1
                if objs % 10000 == 0:
                    self.db.commit()
                    sys.stdout.write(".")
                    sys.stdout.flush()
        self.db.commit()
        end = time.time()
        print "\n%s objects and %s references totalling %s bytes read in %.1fs" % (
            nice_num(objs), nice_num(refs), nice_num(bytes), (end-start),
        )


if __name__ == "__main__":
    app = MemSeeApp()
    app.cmdloop()

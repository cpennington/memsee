#!/usr/bin/env python

import cmd
import gzip
import json
import os
import re
import sqlite3
import sys
import time

# Data is like:
#
# {"address": 125817416, "type": "list", "size": 72, "len": 0, "refs": []}

MAX_VALUE = 80

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


class MemSeeDb(object):
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

    def __init__(self, filename):
        self.filename = filename
        self.conn = sqlite3.connect(filename)

    def create_schema(self):
        c = self.conn.cursor()
        for stmt in self.SCHEMA:
            c.execute(stmt)
        self.conn.commit()

    def import_data(self, data, out=None):
        if out:
            out.write("Reading")
            out.flush()

        objs = refs = bytes = 0
        c = self.conn.cursor()
        for line in data:
            try:
                objdata = json.loads(line)
            except ValueError:
                # https://bugs.launchpad.net/meliae/+bug/876810
                objdata = json.loads(re.sub(r'"value": "(\\"|[^"])*"', '"value": "SURROGATE ERROR REMOVED"', line))
            c.execute(
                "insert into object (address, type, name, value, size, len) values (?, ?, ?, ?, ?, ?)", (
                    objdata['address'],
                    objdata['type'],
                    objdata.get('name'),
                    objdata.get('value'),
                    objdata['size'],
                    objdata.get('len', 0),
                )
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
                self.conn.commit()
                if out:
                    out.write(".")
                    out.flush()

        self.conn.commit()
        if out:
            out.write("\n")

        return {'objs': objs, 'refs': refs, 'bytes': bytes}

    def execute(self, query, args):
        c = self.conn.cursor()
        c.execute(query, args)
        print c.description
        for row in c.fetchall():
            yield row

    def fetchone(self, query, args):
        c = self.conn.cursor()
        c.execute(query, args)
        print c.description
        return c.fetchone()


class MemSeeApp(cmd.Cmd):

    prompt = "::> "

    def __init__(self):
        cmd.Cmd.__init__(self)  # cmd.Cmd isn't an object()!
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
        dbfile = words[0]
        self.db = MemSeeDb(dbfile)
        self.db.create_schema()

    def do_open(self, line):
        """Open a database: open DBFILE"""
        if not line:
            print "Need a db to open"
            return
        words = line.split()
        if len(words) > 1:
            self.default(line)
        dbfile = os.path.expanduser(words[0])
        self.db = MemSeeDb(dbfile)

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

        start = time.time()
        with opener(filename) as data:
            stats = self.db.import_data(data, sys.stdout)
        end = time.time()
        print "{} objects and {} references totalling {} bytes ({.1f}s)".format(
            nice_num(stats['objs']),
            nice_num(stats['refs']),
            nice_num(stats['bytes']),
            end - start,
        )

    def do_parents(self, line):
        """Show parent objects: parents ADDRESS"""
        if not line or not line.isdigit():
            print "Need an address to check for: parents ADDRESS"
            return

        if not self.db:
            print "Database must be open: open DBFILE"
            return

        address = int(line)
        query = "select address, type, name, value, size, len from object, ref where object.address = ref.parent and ref.child = ?"
        for row in self.db.execute(query, (address,)):
            print row

    def do_info(self, line):
        """Show info about an object: info ADDRESS"""
        if not line or not line.isdigit():
            print "Need an address to check for: info ADDRESS"
            return

        if not self.db:
            print "Database must be open: open DBFILE"
            return

        address = int(line)
        query = "select * from object, ref where object.address = ?"
        print self.db.fetchone(query, (address,))


if __name__ == "__main__":
    app = MemSeeApp()
    if len(sys.argv) > 1:
        app.do_open(sys.argv[1])
    app.cmdloop()

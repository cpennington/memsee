#!/usr/bin/env python

import cmd
import gzip
import json
import os
import re
import sqlite3
import sys
import time

from tabulate import tabulate


# Data is like:
#
# {"address": 125817416, "type": "list", "size": 72, "len": 0, "refs": []}

MAX_VALUE = 80

class Num(object):
    """A helper for .format formatting numbers."""
    def __init__(self, n):
        self.n = n

    def __str__(self):
        return str(self.n)

    @property
    def nice(self):
        n = self.n
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

    @property
    def both(self):
        return "{0} ({0.nice})".format(self)


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

    def execute(self, query, args=()):
        """For running SQL that makes changes, and doesn't expect results."""
        c = self.conn.cursor()
        c.execute(query, args)
        self.conn.commit()
        return c.rowcount

    def fetchall(self, query, args=(), header=False):
        c = self.conn.cursor()
        c.execute(query, args)
        if header:
            yield [d[0] for d in c.description]
        for row in c.fetchall():
            yield row

    def fetchone(self, query, args=()):
        c = self.conn.cursor()
        c.execute(query, args)
        return c.fetchone()

    def num_objects(self):
        return int(self.fetchone("select count(*) from object;")[0])

    def num_refs(self):
        return int(self.fetchone("select count(*) from ref;")[0])

    def total_bytes(self):
        return int(self.fetchone("select sum(size) from object;")[0])


def need_db(fn):
    def _dec(self, *args, **kwargs):
        if not self.db:
            print "Need an open database"
            return
        return fn(self, *args, **kwargs)
    return _dec

def handle_sql_error(fn):
    def _dec(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except sqlite3.Error as e:
            print "*** SQL error: {}".format(e)
    return _dec


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

    @need_db
    def do_read(self, line):
        """Read a data file: read DATAFILE"""
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

        sys.stdout.write("Marking top objects...")
        sys.stdout.flush()
        n = self.db.execute("insert into ref (parent, child) select 0, address from object where address not in (select child from ref);")
        print " {}".format(n)

        end = time.time()
        print "{.both} objects and {.both} references totalling {.both} bytes ({:.1f}s)".format(
            Num(stats['objs']),
            Num(stats['refs']),
            Num(stats['bytes']),
            end - start,
        )

    @need_db
    def do_stats(self, line):
        print "{.both} objects, {.both} references, {.both} total bytes".format(
            Num(self.db.num_objects()),
            Num(self.db.num_refs()),
            Num(self.db.total_bytes()),
        )

    @need_db
    def do_parents(self, line):
        """Show parent objects: parents ADDRESS"""
        if not line or not line.isdigit():
            print "Need an address to check for: parents ADDRESS"
            return

        address = int(line)
        query = "select address, type, name, value, size, len from object, ref where object.address = ref.parent and ref.child = ?"
        for row in self.db.fetchall(query, (address,)):
            print row

    @need_db
    def do_info(self, line):
        """Show info about an object: info ADDRESS"""
        if not line or not line.isdigit():
            print "Need an address to check for: info ADDRESS"
            return

        address = int(line)
        query = "select * from object, ref where object.address = ?"
        print self.db.fetchone(query, (address,))

    def fix_cell(self, c):
        if isinstance(c, (int, long)):
            return str(c)
        return c

    def process_row(self, data):
        for row in data:
            newrow = [self.fix_cell(c) for c in row]
            yield newrow

    @need_db
    @handle_sql_error
    def do_select(self, line):
        query = "select " + line
        results = self.db.fetchall(query, header=True)
        names = list(next(results))
        print tabulate(self.process_row(results), headers=names)

    @need_db
    @handle_sql_error
    def do_delete(self, line):
        query = "delete " + line
        nrows = self.db.execute(query)
        print "{} rows deleted".format(nrows)

    @need_db
    def do_gc(self, line):
        """Delete orphan objects and their references, recursively."""
        num_objects = self.db.num_objects()
        num_refs = self.db.num_refs()

        while True:
            self.db.execute("delete from ref where parent != 0 and parent not in (select address from object)")
            new_num_refs = self.db.num_refs()
            print "Deleted {} references, total is {}".format((num_refs - new_num_refs), new_num_refs)
            if new_num_refs == num_refs:
                print "Done."
                break
            num_refs = new_num_refs

            self.db.execute("delete from object where address not in (select child from ref)")
            new_num_objects = self.db.num_objects()
            print "Deleted {} objects, total is {}".format((num_objects - new_num_objects), new_num_objects)
            if new_num_objects == num_objects:
                print "Done."
                break
            num_objects = new_num_objects


if __name__ == "__main__":
    app = MemSeeApp()
    if len(sys.argv) > 1:
        app.do_open(sys.argv[1])
    app.cmdloop()

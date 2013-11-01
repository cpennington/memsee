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
    # Fixed schema for the database.
    SCHEMA = [
        "create table gen (num int, current int);",
    ]

    # Schema for each generation, each is its own set of tables and indexes.
    GEN_SCHEMA = [
        "create table obj (address int primary key, type text, name text, value text, size int, len int);",
        "create index size{gen} on obj (size);",
        "create index type{gen} on obj (type);",
        "create index name{gen} on obj (name);",
        "create index value{gen} on obj (value);",

        "create table ref (parent int, child int);",
        "create index parent{gen} on ref (parent);",
        "create index child{gen} on ref (child);",
    ]
    GEN_TABLES = ['obj', 'ref']

    def __init__(self, filename):
        self.filename = filename
        self.conn = sqlite3.connect(filename)

    def create_schema(self):
        c = self.conn.cursor()
        for stmt in self.SCHEMA:
            c.execute(stmt)
        self.conn.commit()

    def switch_to_generation(self, newgen):
        oldgen = self.fetchint("select num from gen where current = 1")
        if oldgen is not None:
            for table in self.GEN_TABLES:
                self.execute("alter table {table} rename to {table}{gen}".format(table=table, gen=oldgen))
        self.execute("update gen set current=0")
        if newgen:
            for table in self.GEN_TABLES:
                self.execute("alter table {table}{gen} rename to {table}".format(table=table, gen=newgen))
            self.execute("update gen set current=1 where num=?", (newgen,))

    def make_new_generation(self):
        gen = self.fetchint("select max(num) from gen", default=0)
        gen += 1
        self.execute(
            "insert into gen (num, current) values (?, 1)",
            (gen,)
        )
        for stmt in self.GEN_SCHEMA:
            self.execute(stmt.format(gen=gen))

    def import_data(self, data, out=None):
        # Put away the current generation tables.
        self.switch_to_generation(None)

        # Make a new current generation.
        self.make_new_generation()

        # Read the data.
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
                "insert into obj (address, type, name, value, size, len) values (?, ?, ?, ?, ?, ?)", (
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

    def fetchint(self, query, args=(), default=None):
        one = self.fetchone(query, args)
        if one is None:
            return default
        if one[0] is None:
            return default
        return int(one[0])

    def num_objects(self):
        return self.fetchint("select count(*) from obj")

    def num_refs(self):
        return self.fetchint("select count(*) from ref")

    def total_bytes(self):
        return self.fetchint("select sum(size) from obj")


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
        n = self.db.execute("insert into ref (parent, child) select 0, address from obj where address not in (select child from ref);")
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
        query = "select address, type, name, value, size, len from obj, ref where obj.address = ref.parent and ref.child = ?"
        for row in self.db.fetchall(query, (address,)):
            print row

    @need_db
    def do_info(self, line):
        """Show info about an object: info ADDRESS"""
        if not line or not line.isdigit():
            print "Need an address to check for: info ADDRESS"
            return

        address = int(line)
        query = "select * from obj, ref where obj.address = ?"
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
            self.db.execute("delete from ref where parent != 0 and parent not in (select address from obj)")
            new_num_refs = self.db.num_refs()
            print "Deleted {} references, total is {}".format((num_refs - new_num_refs), new_num_refs)
            if new_num_refs == num_refs:
                print "Done."
                break
            num_refs = new_num_refs

            self.db.execute("delete from obj where address not in (select child from ref)")
            new_num_objects = self.db.num_objects()
            print "Deleted {} objects, total is {}".format((num_objects - new_num_objects), new_num_objects)
            if new_num_objects == num_objects:
                print "Done."
                break
            num_objects = new_num_objects

    @need_db
    def do_gen(self, line):
        """Examine or switch generations."""
        words = line.split()
        gens = self.db.fetchint("select count(*) from gen")
        if not words:
            gen = self.db.fetchint("select num from gen where current=1")
            print "{} generations, current is {}".format(gens, gen or "-none-")
        else:
            if words[0] == "none":
                gen = None
                msg = "Using no generation, of {gens}"
            else:
                try:
                    gen = int(words[0])
                except ValueError:
                    print "** Didn't understand {!r} as a generation".format(words[0])
                    return
                if not (0 < gen <= gens):
                    print "** Not a valid generation number: {}".format(gen)
                    return
                msg = "Using generation {gen} of {gens}"
            self.db.switch_to_generation(gen)
            print msg.format(gen=gen, gens=gens)


if __name__ == "__main__":
    app = MemSeeApp()
    if len(sys.argv) > 1:
        app.do_open(sys.argv[1])
    app.cmdloop()

#!/usr/bin/env python

import atexit
import cmd
import functools
import gzip
import json
import os
import re
import readline
import shutil
import sqlite3
import sys
import time

from grid import GridWriter

# Set up readline history across invocations
histfile = os.path.join(os.path.expanduser('~'), '.memsee_history')
try:
    readline.read_history_file(histfile)
except IOError:
    pass

atexit.register(readline.write_history_file, histfile)
del histfile


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
        if self.n >= 10000:
            return "{0} ({0.nice})".format(self)
        else:
            return "{0}".format(self)


class MemSeeDb(object):
    # Fixed schema for the database.
    SCHEMA = [
        "create table gen (num int, current int);",
        "create table env (name text, value text);",
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
                    objdata.get('len'),
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

    DEBUG = False

    def fetchall(self, query, args=(), header=False):
        c = self.conn.cursor()
        if self.DEBUG:
            print query
            if args:
                print args
            start = time.time()
        c.execute(query, args)
        if self.DEBUG:
            print "({:.2f}s)".format(time.time() - start)
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

    def define_name(self, name, value):
        self.execute("insert into env (name, value) values (?, ?)", (name, value))

    def all_names(self):
        return self.fetchall("select name, value from env")


def need_db(fn):
    """Decorator for command handlers that need an open database."""
    @functools.wraps(fn)
    def _dec(self, *args, **kwargs):
        if not self.db:
            print "Need an open database"
            return
        return fn(self, *args, **kwargs)
    return _dec

def handle_errors(fn):
    """Decorator for command handlers that accept user SQL, to handle errors."""
    @functools.wraps(fn)
    def _dec(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except sqlite3.Error as e:
            print "*** SQL error: {}".format(e)
        except MemSeeException as e:
            print "*** {}".format(e)
    return _dec


class MemSeeApp(cmd.Cmd):

    prompt = "::> "

    def __init__(self):
        cmd.Cmd.__init__(self)  # cmd.Cmd isn't an object()!
        self.db = None
        self.reset()

        self.column_formats = {
            '#': '<8',

            # obj columns
            'address': '>16',
            'type': '<20',
            'name': '<20',
            'value': '<60',
            'size': '>10',
            'len': '>10',

            # ref columns
            'parent': '>15',
            'child': '>15',

            # ad-hoc columns
            'count(*)': '>10',
            'num': '>10',
            'refs': '>10',
            'n': '>10',
        }

    def reset(self):
        # results is a list of dicts:
        #   [{
        #       'data': [[col, col, col, ...],
        #                [col, col, col, ...],
        #                ...
        #               ],
        #       'names': ['col1', 'col2', 'col3', ...],
        #       'id_column': 0,
        #   }, ...
        #   ]
        self.results = []

        # env is a map from names to values, rev_env is values to names.
        self.env = {}
        self.rev_env = {}

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
            return

        dbfile = words[0]
        self.db = MemSeeDb(dbfile)
        self.db.create_schema()
        self.reset()

    def do_open(self, line):
        """Open a database: open DBFILE"""
        if not line:
            print "Need a db to open"
            return
        words = line.split()
        if len(words) > 1:
            self.default(line)
            return

        dbfile = os.path.expanduser(words[0])
        self.db = MemSeeDb(dbfile)
        self.reset()

        # Load the defined names
        for name, value in self.db.all_names():
            self.env[name] = value
            self.rev_env[value] = name

    @need_db
    def do_read(self, line):
        """Read a data file: read DATAFILE

        Each file read becomes a new generation in the database.
        The last file read is the default generation, in tables obj and ref.

        """
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
        """Print object and reference counts, and total size."""
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

    def substitute_symbols(self, sql):
        """Replace tokens in `sql`."""
        def replace_relationship(m):
            """re.sub function for ^ and & in SQL."""
            ref = m.group(1)
            ops = m.group(2)
            ref = self.substitute_symbols(ref)
            for op in ops:
                if ref.startswith("("):
                    condition = "IN"
                else:
                    condition = "="
                if op == "^":
                    sql = "(select parent from ref where child {condition} {ref})"
                else:
                    sql = "(select child from ref where parent {condition} {ref})"
                ref = sql.format(condition=condition, ref=ref)
            return ref

        def replace_result(m):
            """re.sub function for #\d+.\d+ in SQL."""
            resnum = int(m.group(1))
            rownum = int(m.group(2))
            try:
                results = self.results[resnum]
                row = results['data'][rownum]
            except IndexError:
                raise SubstitutionError("Result reference out of range: {}".format(m.group()))
            id_column = results['id_column']
            if id_column is None:
                raise SubstitutionError("Results had no address column: {}".format(m.group()))

            return str(row[id_column])

        def replace_column(m):
            """re.sub function for #\d+.\w+ in SQL."""
            resnum = int(m.group(1))
            column = m.group(2)
            try:
                results = self.results[resnum]
            except IndexError:
                raise SubstitutionError("Result reference out of range: {}".format(m.group()))
            try:
                colnum = results['names'].index(column)
            except ValueError:
                raise SubstitutionError("No such column: {}".format(m.group()))
            return "({})".format(",".join(str(r[colnum]) for r in results['data']))

        def replace_env(m):
            """re.sub function for $\w+ in SQL."""
            try:
                return self.env[m.group(1)]
            except KeyError:
                raise SubstitutionError("Named reference undefined: {}".format(m.group()))

        sql = re.sub(r"([#$]?[\w.:]+)([&^]+)", replace_relationship, sql)
        sql = re.sub(r"#(\d+)\.(\d+)", replace_result, sql)
        sql = re.sub(r"#(\d+)\.(\w+)", replace_column, sql)
        sql = re.sub(r"\$([\w.:]+)", replace_env, sql)
        return sql

    def fix_cell(self, c):
        """Fix cell data for good presentation."""
        if c is None:
            c = u"\N{RING OPERATOR}"
        if isinstance(c, (int, long)):
            c = str(c)
            if c in self.rev_env:
                c = "$" + self.rev_env[c]
            return c
        if isinstance(c, (str, unicode)):
            # Scrub things that will mess with the output.
            c = c.replace("\n", r"\n").replace("\r", r"\r").replace("\t", r"\t")
        return c

    def process_rows(self, data, id_fmt):
        """Process a row for output."""
        for i, row in enumerate(data):
            new_row = [self.fix_cell(c) for c in row]
            if id_fmt is not None:
                yield [id_fmt.format(i)] + new_row
            else:
                yield new_row

    @need_db
    @handle_errors
    def do_select(self, line):
        """Perform a query against the SQLite db.

        If the query has an address column, then rows are labelled like #2.5.
        Row numbers can be used in queries to use that row's address.

        Defined names (see the set command) can be used like $name.
        """
        query = self.substitute_symbols("select " + line)
        self.show_select(query)

    def id_column(self, names):
        """Return the index of the id column in `names`, or None."""
        try:
            return names.index('address')
        except ValueError:
            return None

    def show_select(self, query, args=(), show_headers=True):
        results = self.db.fetchall(query, args, header=True)

        names = list(next(results))
        data = list(results)

        id_column = self.id_column(names)
        headers = ['#'] + names
        id_fmt = "#{}.{{}}".format(len(self.results))
        self.results.append({
            'names': names,
            'data': data,
            'id_column': id_column,
        })

        # Show the results.
        formats = [self.column_formats.get(h, '<10') for h in headers]
        gw = GridWriter(formats=formats, out=sys.stdout)
        gw.header(headers if show_headers else None)
        gw.rows(self.process_rows(data, id_fmt))

        return data

    @need_db
    @handle_errors
    def do_delete(self, line):
        """Execute a delete statement against the SQLite db.

        See the select command for available shorthands.
        """
        query = self.substitute_symbols("delete " + line)
        nrows = self.db.execute(query)
        print "{} rows deleted".format(nrows)

    @need_db
    @handle_errors
    def do_pin(self, condition):
        """Prevent all objects in obj selected by `condition` from being deleted by `gc`"""
        query = self.substitute_sql('insert into ref (parent, child) select 0, address from obj where {};'.format(condition))
        nrows = self.db.execute(query)
        print "{} rows pinned".format(nrows)

    @need_db
    def do_backup(self, _line):
        backup = self.db.filename + '.bak'
        if os.path.exists(backup):
            print "DB already backed up"
            return
        else:
            shutil.copyfile(self.db.filename, backup)

    @need_db
    def do_restore(self, _line):
        backup = self.db.filename + '.bak'
        if not os.path.exists(backup):
            print "No backed up DB"
            return
        else:
            shutil.copyfile(backup, self.db.filename)
            self.db = MemSeeDb(self.db.filename)

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
        """Examine or switch generations.

        Each data file read becomes a new generation.  The current generation
        is available in tables obj and ref.  Other generations are in tables
        objN and refN, where N is the generation number.

        "gen 3" will switch to generation 3, making its data available in obj
        and ref.  At that point, obj3 and ref3 are no longer available.
        "gen none" puts all generations into their numbered tables, and no
        obj or ref table exists.
        """
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

    @need_db
    def do_set(self, line):
        """Set or examine named values.

        "set NAME VALUE" defines a new name.  VALUE can contain other names,
        or row numbers.

        "set" prints all the defined values.
        """
        if not line:
            gw = GridWriter(["<15", "<30"])
            gw.header(["name", "value"])
            gw.rows(sorted(self.env.items()))
        else:
            words = self.substitute_symbols(line).split(None, 1)
            if len(words) != 2:
                return self.default(line)
            name, value = words
            self.env[name] = value
            self.rev_env[value] = name
            self.db.define_name(name, value)

    @handle_errors
    def do_echo(self, line):
        """Show the value of an expression."""
        line = self.substitute_symbols(line)
        print line

    def do_width(self, line):
        """Set the widths for columns: WIDTH colname width ..."""
        words = line.split()
        if len(words) % 2 != 0:
            print "Need pairs: colname width ..."
            return
        while words:
            colname, width = words[:2]
            words = words[2:]
            try:
                width = int(width)
            except ValueError:
                print "This isn't a width: {!r}".format(width)
                return
            align = self.column_formats.get(colname, "<")[0]
            self.column_formats[colname] = "{}{}".format(align, width)

    def do_kids(self, line):
        """Display object descending from an object."""
        words = self.substitute_symbols(line).split()
        if len(words) != 1:
            print "Need an object address."
            return
        id = int(words[0])
        self.show_select("select * from obj where address = ?", (id,))

        ids_to_show = set([id])
        ids_shown = set()
        while ids_to_show:
            ids_shown.update(ids_to_show)
            id_list = ",".join(str(i) for i in ids_to_show)

            print
            children = self.show_select("""
                select
                    obj.*,
                    (select count(*) from ref where child = obj.address) refs
                from obj
                where address in (select child from ref where parent in ({}))
                """.format(id_list),
                show_headers=False,
            )
            child_ids = set(r[0] for r in children if r[-1] == 1)
            ids_to_show = child_ids - ids_shown


class MemSeeException(Exception):
    pass

class SubstitutionError(MemSeeException):
    pass


if __name__ == "__main__":
    app = MemSeeApp()
    if len(sys.argv) > 1:
        app.do_open(sys.argv[1])
    app.cmdloop()

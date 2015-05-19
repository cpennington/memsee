#!/usr/bin/env python

from __future__ import division
from __future__ import print_function
from builtins import str
from builtins import range
from builtins import object
from past.utils import old_div
import functools
import gzip
import igraph
import math
import os
import qgrid
import re
import shlex
import shutil
import sqlite3
import sys
import time
import ujson

from grid import GridWriter
from IPython.core.magic import (
    Magics, magics_class, line_magic,
    cell_magic, line_cell_magic
)
from IPython.utils.traitlets import Bool
from pandas import Series
from sql.connection import Connection
from sql.magic import SqlMagic, load_ipython_extension as sql_load_ipython_extension
from sql.run import ResultSet




if __name__ == "__main__":
    print("Memsee is now an IPython magics library. Start ipython notebook with 'ipython notebook', and import memsee")
    sys.exit(1)


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


def need_db(fn):
    """Decorator for command handlers that need an open database."""
    @functools.wraps(fn)
    def _dec(self, *args, **kwargs):
        if not Connection.get(None):
            print("Need an open database")
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
            print("*** SQL error: {}".format(e))
        except MemSeeException as e:
            print("*** {}".format(e))
    return _dec


@magics_class
class MemSeeApp(SqlMagic):

    # Fixed schema for the database.
    SCHEMA = [
        "create table gen (num int, current int);",
        "create table env (name text, value text);",
    ]

    # Schema for each generation, each is its own set of tables and indexes.
    GEN_SCHEMA = [
        "create table obj (address int primary key, type text, name text, value text, size int, len int, mark int, repr text);",
        "create index size{gen} on obj (size);",
        "create index type{gen} on obj (type);",
        "create index name{gen} on obj (name);",
        "create index value{gen} on obj (value);",
        "create index mark{gen} on obj (mark);",
        "create index repr{gen} on obj (repr);",

        "create table ref (parent int, child int);",
        "create index child{gen} on ref (child);",
        "create index parent{gen} on ref (parent);",
    ]
    GEN_TABLES = ['obj', 'ref']

    feedback = Bool(False, config=True, help="Print number of rows affected by DML")

    def __init__(self, *args, **kwargs):
        super(MemSeeApp, self).__init__(*args, **kwargs)
        self.graphs = {}
        self.reset()
        self.debug = False

    @property
    def graph(self):
        if self.current_gen not in self.graphs:
            self._load_graph(self.current_gen)

        return self.graphs[self.current_gen]

    @property
    def current_gen(self):
        return self.fetchint("select num from gen where current = 1")

    def _load_graph(self, gen):
        print("Loading object graph for generation {}\n".format(gen))

        # Store the current generation
        current_gen = self.current_gen

        # Activate the target generation
        self.switch_to_generation(gen)

        graph = igraph.Graph(directed=True)

        print("Loading {} objects... ".format(self.fetchint("select count(*) from obj")))

        start = time.time()
        # Add all object addresses as vertex names
        graph.add_vertices(row[0] for row in self.fetchall("select cast(address as text) from obj"))

        print("Done ({} secs)\n".format(time.time() - start))

        print("Loading {} edges... ".format(self.fetchint("select count(*) from ref")))

        start = time.time()

        # Add all of the edges
        graph.add_edges(self.fetchall("""
            SELECT cast(parent as text), cast(child as text)
            FROM ref
            INNER JOIN obj AS obj_parent ON ref.parent = obj_parent.address
            INNER JOIN obj AS obj_child ON ref.child = obj_child.address
        """))

        print("Done ({} secs)\n".format(time.time() - start))

        self.graphs[gen] = graph

        # Switch back to the previous active generation
        self.switch_to_generation(current_gen)

    def create_schema(self):
        self.execute(line='', cell='\n'.join(self.SCHEMA))

    def switch_to_generation(self, newgen):
        oldgen = self.current_gen
        if oldgen is not None:
            for table in self.GEN_TABLES:
                self.execute_and_ignore("alter table {table} rename to {table}{gen}".format(table=table, gen=oldgen))
        self.execute_and_ignore("update gen set current=0")
        if newgen:
            for table in self.GEN_TABLES:
                self.execute_and_ignore("alter table {table}{gen} rename to {table}".format(table=table, gen=newgen))
            self.execute_and_ignore("update gen set current=1 where num=:gen", gen=newgen)

    def make_new_generation(self):
        gen = self.fetchint("select max(num) from gen", default=0)
        gen += 1
        self.execute_and_ignore(
            "insert into gen (num, current) values (:gen, 1)",
            gen=gen
        )
        for stmt in self.GEN_SCHEMA:
            self.execute_and_ignore(stmt.format(gen=gen))

    def _parse_data(self, data):
        for line in data:
            try:
                objdata = ujson.loads(line)
            except ValueError:
                # https://bugs.launchpad.net/meliae/+bug/876810
                objdata = ujson.loads(re.sub(r'"value": "(\\"|[^"])*"', '"value": "SURROGATE ERROR REMOVED"', line))

            try:
                if objdata['type'] in ('function', 'type', 'module'):
                    objdata['repr'] = objdata.get('name', objdata.get('value', objdata['type']))
                elif objdata['type'] in ('int', 'str', 'unicode'):
                    objdata['repr'] = repr(objdata['value'])
                else:
                    objdata['repr'] = objdata['type']
            except:
                print(objdata)
                objdata['repr'] = objdata['type']
            yield objdata

    def import_data(self, data):
        # Put away the current generation tables.
        self.switch_to_generation(None)

        # Make a new current generation.
        self.make_new_generation()

        # Read the data.
        print("Reading")

        objs = refs = bytes = 0

        sql_alch_conn = Connection.get(None).session

        transaction = sql_alch_conn.begin()

        for objdata in self._parse_data(data):

            sql_alch_conn.execute(
                """insert into obj
                        (address, type, name, value, size, len, repr)
                   values
                        (:address, :type, :name, :value, :size, :len, :repr)
                """,
                address=objdata['address'],
                type=objdata['type'],
                name=objdata.get('name'),
                value=objdata.get('value'),
                size=objdata['size'],
                len=objdata.get('len'),
                repr=objdata['repr']
            )
            objs += 1
            bytes += objdata['size']
            for ref in objdata['refs']:
                sql_alch_conn.execute(
                    "insert into ref (parent, child) values (:parent, :child)",
                    parent=objdata['address'],
                    child=ref,
                )
                refs += 1
            if objs % 10000 == 0:
                transaction.commit()
                transaction = sql_alch_conn.begin()
                print("loaded {} objects, {} refs".format(objs, refs))

        self.execute_and_ignore('COMMIT')
        print("")

        return {'objs': objs, 'refs': refs, 'bytes': bytes}

    def execute_and_ignore(self, query, **kwargs):
        """For running SQL that makes changes, and doesn't expect results."""
        result = self.execute(query, local_ns=kwargs)
        if result:
            return len(result)
        else:
            return None

    def executemany(self, query, arglist=()):
        """Execute a SQL query many times over a list of arguments."""
        return sum(len(self.execute(query, args)) for args in arglist)

    def fetchall(self, query, header=False, **kwargs):
        return self.execute(line=query, local_ns=kwargs)

    def execute(self, line, cell='', local_ns={}):
        if self.debug:
            print(line)
            print(cell)
            if local_ns:
                print(local_ns)
            start = time.time()
        result = super(MemSeeApp, self).execute(line=line, cell=cell, local_ns=local_ns)
        if self.debug:
            print("({:.2f}s)".format(time.time() - start))
        return result

    def fetchone(self, query, args={}):
        result = self.fetchall(query, args)
        if len(result) >= 1:
            return result[0]
        else:
            return None

    def fetchint(self, query, args={}, default=None):
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
        self.execute_and_ignore("insert into env (name, value) values (:name, :value)", name=name, value=value)

    def all_names(self):
        return self.fetchall("select name, value from env")

    def reset(self):
        """Reset the db-derived state of the app."""
        # results is a list of DataFrames
        self.results = []

        # env is a map from names to values, rev_env is values to names.
        self.env = {}
        self.rev_env = {}

    @line_magic
    def debug(self, line):
        """Toggle Debug Mode"""
        self.debug = not self.debug
        print("DEBUG MODE", "ON" if self.debug else "OFF")

    @line_magic
    def create(self, line):
        """Create a new database: create DBFILE"""
        if not line:
            print("Need a db to create")
            return
        words = line.split()
        if len(words) > 1:
            self.default(line)
            return

        self.filename = words[0]
        self.execute("sqlite:///{}".format(self.filename))
        self.create_schema()
        self.reset()
        self.shell.push({'memsee': self})

        print("Database created, available via variable 'memsee'")

    @line_magic
    def open(self, line):
        """Open a database: open DBFILE"""
        if not line:
            print("Need a db to open")
            return
        words = line.split()
        if len(words) > 1:
            self.default(line)
            return

        self.filename = os.path.expanduser(words[0])
        self.execute("sqlite:///{}".format(self.filename))
        self.reset()

        # Load the defined names
        for name, value in self.all_names():
            self.env[name] = value
            self.rev_env[value] = name
        self.shell.push({'memsee': self})

        print("Database opened, available via variable 'memsee'")

    @need_db
    @line_magic
    def read(self, line):
        """Read a data file: read DATAFILE

        Each file read becomes a new generation in the database.
        The last file read is the default generation, in tables obj and ref.

        """
        if not line:
            print("Need a file to read")
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
            stats = self.import_data(data)

        sys.stdout.write("Marking top objects...")
        sys.stdout.flush()
        self.execute_and_ignore("INSERT INTO obj (address) VALUES (0)")
        n = self.execute_and_ignore("insert into ref (parent, child) select 0, address from obj where address not in (select child from ref);")
        print(" {}".format(n))

        end = time.time()
        print("{.both} objects and {.both} references totalling {.both} bytes ({:.1f}s)".format(
            Num(stats['objs']),
            Num(stats['refs']),
            Num(stats['bytes']),
            end - start,
        ))

    @need_db
    @line_magic
    def stats(self, line):
        """Print object and reference counts, and total size."""
        print("{.both} objects, {.both} references, {.both} total bytes".format(
            Num(self.num_objects()),
            Num(self.num_refs()),
            Num(self.total_bytes()),
        ))

    @need_db
    @line_magic
    def parents(self, line):
        """Show parent objects: parents ADDRESS"""
        if not line or not line.isdigit():
            print("Need an address to check for: parents ADDRESS")
            return

        address = int(line)
        query = "select address, type, name, value, size, len from obj, ref where obj.address = ref.parent and ref.child = :child"
        for row in self.fetchall(query, child=address):
            print(row)

    @need_db
    @line_magic
    def info(self, line):
        """Show info about an object: info ADDRESS"""
        if not line or not line.isdigit():
            print("Need an address to check for: info ADDRESS")
            return

        address = int(line)
        query = "select * from obj, ref where obj.address = :addr"
        print(self.fetchone(query, addr=address))

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
            except IndexError:
                raise SubstitutionError("#{} doesn't name a result".format(m.group(1)))

            if 'address' not in results:
                raise SubstitutionError("Results had no address column: {}".format(m.group()))

            try:
                return str(results['address'][m.group(0)])
            except IndexError:
                raise SubstitutionError("Result reference out of range: {}".format(m.group()))

        def replace_column(m):
            """re.sub function for #\d+.\w+ in SQL."""
            resnum = int(m.group(1))
            column = m.group(2)
            try:
                results = self.results[resnum]
            except IndexError:
                raise SubstitutionError("Result reference out of range: {}".format(m.group()))
            try:
                return "({})".format(",".join(str(value) for value in results[column]))
            except ValueError:
                raise SubstitutionError("No such column: {}".format(m.group()))

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
        if isinstance(c, int):
            if str(c) in self.rev_env:
                c = "$" + self.rev_env[str(c)]
            return c
        return c

    def process_rows(self, results):
        """Process a row for output."""
        return results.applymap(self.fix_cell)

    @need_db
    @handle_errors
    @line_magic
    def select(self, line):
        """Perform a query against the SQLite db.

        If the query has an address column, then rows are labelled like #2.5.
        Row numbers can be used in queries to use that row's address.

        Defined names (see the set command) can be used like $name.
        """
        query = self.substitute_symbols("select " + line)
        return self.display_fancy(self.fetchall(query))

    def display_fancy(self, results):
        if isinstance(results, ResultSet):
            results = results.DataFrame()

        if len(results) == 0:
            print("No results found.")
            return

        num_results = len(self.results)
        width = int(math.ceil(math.log10(len(results))))
        fmt_str = "#{{result}}.{{row:0>{width}}}".format(width=width)
        index = Series(fmt_str.format(result=num_results, row=row) for row in range(len(results)))

        results.insert(0, '#', index)
        results = results.set_index('#')

        results = self.process_rows(results)

        self.results.append(results)

        return qgrid.show_grid(results)

    @need_db
    @handle_errors
    @line_magic
    def insert(self, line):
        """Execute a insert statement against the SQLite db.

        See the select command for available shorthands.
        """
        query = self.substitute_symbols("insert " + line)
        nrows = self.execute_and_ignore(query)
        print("{} rows inserted".format(nrows))

    @need_db
    @handle_errors
    @line_magic
    def delete(self, line):
        """Execute a delete statement against the SQLite db.

        See the select command for available shorthands.
        """
        query = self.substitute_symbols("delete " + line)
        nrows = self.execute_and_ignore(query)
        print("{} rows deleted".format(nrows))

    @need_db
    @handle_errors
    @line_magic
    def pin(self, condition):
        """Prevent all objects in obj selected by `condition` from being deleted by `gc`"""
        query = self.substitute_symbols('insert into ref (parent, child) select 0, address from obj where {};'.format(condition))
        nrows = self.execute_and_ignore(query)
        print("{} rows pinned".format(nrows))

    @need_db
    @line_magic
    def backup(self, _line):
        """Copy the database for safe-keeping.  Only one level."""
        backup = self.filename + '.bak'
        if os.path.exists(backup):
            print("DB already backed up")
            return
        else:
            shutil.copyfile(self.filename, backup)

    @need_db
    @line_magic
    def restore(self, _line):
        """Restore a saved-away database."""
        backup = self.filename + '.bak'
        if not os.path.exists(backup):
            print("No backed up DB")
            return
        else:
            shutil.copyfile(backup, self.filename)
            self.open(self.filename)

    @need_db
    @line_magic
    def gc(self, line):
        """Delete orphan objects and their references, recursively."""
        self.stats('')
        self.execute_and_ignore("UPDATE obj SET mark = NULL WHERE mark IS NOT NULL")
        num_marked = self.execute_and_ignore(self.substitute_symbols("UPDATE obj SET mark = 1 WHERE address IN 0&"))
        print("Marked {} top level objects".format(num_marked))
        self.continue_gc(line)

    @need_db
    @line_magic
    def continue_gc(self, line):
        """Continue a previously interrupted garbage collection"""

        depth = self.fetchint("select max(mark) from obj")

        while True:
            num_marked = self.execute_and_ignore(
                """UPDATE obj
                      SET mark = :depth + 1
                    WHERE address IN (
                          SELECT child
                            FROM ref, obj p, obj c
                           WHERE ref.parent = p.address
                             AND ref.child = c.address
                             AND p.mark = :depth
                             AND c.mark is NULL
                          )
                """,
                depth=depth
            )

            if num_marked == 0:
                print("Marking complete")
                break

            print("Marked {} objects at depth {}".format(num_marked, depth))
            depth += 1

        num_deleted = self.execute_and_ignore("DELETE FROM obj WHERE mark IS NULL")
        print("Deleted {} objects".format(num_deleted))

        self.stats('')

    @need_db
    @line_magic
    def gen(self, line):
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
        gens = self.fetchint("select count(*) from gen")
        if not words:
            gen = self.fetchint("select num from gen where current=1")
            print("{} generations, current is {}".format(gens, gen or "-none-"))
        else:
            if words[0] == "none":
                gen = None
                msg = "Using no generation, of {gens}"
            else:
                try:
                    gen = int(words[0])
                except ValueError:
                    print("** Didn't understand {!r} as a generation".format(words[0]))
                    return
                if not (0 < gen <= gens):
                    print("** Not a valid generation number: {}".format(gen))
                    return
                msg = "Using generation {gen} of {gens}"
            self.switch_to_generation(gen)
            print(msg.format(gen=gen, gens=gens))

    @need_db
    @line_magic
    def set(self, line):
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
            self.define_name(name, value)

    @handle_errors
    @line_magic
    def echo(self, line):
        """Show the value of an expression."""
        line = self.substitute_symbols(line)
        print(line)

    @line_magic
    def kids(self, line):
        """Display object descending from an object."""
        words = self.substitute_symbols(line).split()
        if len(words) != 1:
            print("Need an object address.")
            return
        id = int(words[0])
        self.display_fancy(
            self.fetchall(
                "select * from obj where address = :addr",
                addr=id
            )
        )

        ids_to_show = set([id])
        ids_shown = set()
        while ids_to_show:
            ids_shown.update(ids_to_show)
            id_list = ",".join(str(i) for i in ids_to_show)

            print()
            children = self.display_fancy("""
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

    @need_db
    @handle_errors
    @line_magic
    def path(self, line):
        """Find a path from one set of objects to another."""
        words = shlex.split(self.substitute_symbols(line).encode('utf8'))
        if (len(words) not in (4, 5)
            or words[0] != "from"
            or words[2] != "to"
            or len(words) == 5 and words[4] != 'reversed'):
            print('Syntax:  path from "condition1" to "condition2" [reversed]')
            return
        from_cond = words[1]
        to_cond = words[3]
        reversed = len(words) == 5

        from_address = self.fetchone("SELECT cast(address as text) FROM obj WHERE {}".format(from_cond))
        to_addresses = (row[0] for row in self.fetchall("SELECT cast(address as text) FROM obj WHERE {}".format(to_cond)))

        paths = self.graph.get_shortest_paths(
            v=from_address[0],
            to=to_addresses,
            mode=igraph.IN if reversed else igraph.OUT,
            output='vpath',
        )

        for path in paths:
            addresses = self.graph.vs.select(path)['name']

            self.execute_and_ignore("drop table if exists tmp_path_order")
            self.execute_and_ignore("create temp table tmp_path_order (idx int, address int)")
            self.executemany(
                "insert into tmp_path_order (idx, address) values (:idx, :addr)",
                ({"idx": idx, "addr": addr} for idx, addr in enumerate(addresses))
            )

            results = self.fetchall(
                "select obj.* from obj, tmp_path_order where "
                "obj.address = tmp_path_order.address order by idx"
            )
            if len(results):
                self.display_fancy(results)

    @need_db
    @handle_errors
    @line_magic
    def ancestor_types(self, condition):
        """Display the set of types in each generation of ancestors of the objects selected by `condition`"""
        condition = self.substitute_symbols(condition)
        self.execute_and_ignore('drop table if exists tmp_ancestor_types')
        self.execute_and_ignore("create table tmp_ancestor_types (address int, type text, gen int, refs int, PRIMARY KEY (address, refs))")
        gen = 0
        inserted = self.execute_and_ignore("insert into tmp_ancestor_types select address, type, 0, 0 from obj where {}".format(condition))

        while inserted > 0:
            inserted = self.execute_and_ignore(
                """INSERT INTO tmp_ancestor_types
                        SELECT DISTINCT r.parent, parent.type, (:gen + 1), r.child
                          FROM ref r LEFT OUTER JOIN tmp_ancestor_types seen
                            ON r.parent = seen.address
                           AND r.child = seen.refs,
                               obj parent, tmp_ancestor_types child
                         WHERE r.parent = parent.address
                           AND r.child = child.address
                           AND seen.address is NULL
                           AND child.gen = :gen
                           AND child.type not in ('module', 'Settings')
                """,
                gen=gen
            )
            print("Found {} new ancestors".format(inserted))
            gen += 1

        self.display_fancy(self.fetchall(
            """SELECT gen, type, count(*)
                 FROM (
                     SELECT max(gen) AS gen, type
                       FROM tmp_ancestor_types
                   GROUP BY address
                 )
             GROUP BY gen, type
             ORDER BY gen, type
            """
        ))


    @need_db
    @handle_errors
    @line_magic
    def shell(self, line):
        """Execute a raw sqlite command against the connected database"""
        self.execute_and_ignore(self.substitute_symbols(line))


class MemSeeException(Exception):
    pass

class SubstitutionError(MemSeeException):
    pass

def load_ipython_extension(ipython):
    sql_load_ipython_extension(ipython)
    ipython.register_magics(MemSeeApp)
    qgrid.nbinstall()


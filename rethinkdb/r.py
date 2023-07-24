# Copyright 2022 RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file incorporates work covered by the following copyright:
# Copyright 2010-2016 RethinkDB, all rights reserved.
from __future__ import annotations
import asyncio
from typing import Literal, overload
from rethinkdb.core import ast
from rethinkdb import ql2_pb2
from rethinkdb import connections

# region Connections


connect = connections.new_connection
connect_async = connections.connect_async
# endregion

def json(json_string: str):
    """
    Transform *arguments parameters into JSON.
    """
    return ast.Json(json_string)


def js(js_string: str, timeout: float = 5):  # pylint: disable=invalid-name
    """
    Create a javascript expression.
    """
    return ast.JavaScript(js_string, timeout=timeout)


def args(array: list):
    """
    r.args is a special term that's used to splice an array of arguments into
    another term. This is useful when you want to call a variadic term such as
    get_all with a set of arguments produced at runtime.

    This is analogous to unpacking argument lists in Python. (However, note
    that arguments evaluates all its arguments before passing them into the parent
    term, even if the parent term otherwise allows lazy evaluation.)
    """
    return ast.Args(*array)


def http(url: str, **kwargs):
    """
    Retrieve data from the specified URL over HTTP. The return type depends on
    the result_format option, which checks the Content-Type of the response by
    default. Make sure that you never use this command for user provided URLs.
    """
    return ast.Http(ast.func_wrap(url), **kwargs)


def error(msg: str):
    """
    Throw a runtime error. If called with no arguments inside the second
    argument to default, re-throw the current error.
    """
    return ast.UserError(msg)


def random(*arguments, **kwargs):
    """
    Generate a random number between given (or implied) bounds. random takes
    zero, one or two arguments.


    * With zero arguments, the result will be a floating-point number in the
    range [0,1) (from 0 up to but not including 1).
    * With one argument x, the result will be in the range [0,x), and will be
    integer unless float=True is given as an option. Specifying a floating
    point number without the float option will raise an error.
    * With two arguments x and y, the result will be in the range [x,y), and
    will be integer unless float=True is given as an option. If x and y are
    equal an error will occur, unless the floating-point option has been
    specified, in which case x will be returned. Specifying a floating point
    number without the float option will raise an error.

    Note: The last argument given will always be the 'open' side of the range,
    but when generating a floating-point number, the 'open' side may be less
    than the 'closed' side.
    """
    return ast.Random(*arguments, **kwargs)


def do(*arguments):  # pylint: disable=invalid-name
    """
    Call an anonymous function using return values from other Reql commands or
    queries as arguments.

    The last argument to do (or, in some forms, the only argument) is an
    expression or an anonymous function which receives values from either the
    previous arguments or from prefixed commands chained before do. The do
    command is essentially a single-element map, letting you map a function
    over just one document. This allows you to bind a query result to a local
    variable within the scope of do, letting you compute the result just once
    and reuse it in a complex expression or in a series of Reql commands.

    Arguments passed to the do function must be basic data types, and cannot
    be streams or selections. (Read about Reql data types.) While the
    arguments will all be evaluated before the function is executed, they may
    be evaluated in any order, so their values should not be dependent on one
    another. The type of do's result is the type of the value returned from the
    function or last expression.
    """
    return ast.FunCall(*arguments)


def table(*arguments, **kwargs):
    """
    Return all documents in a table. Other commands may be chained after table
    to return a subset of documents (such as get and filter) or perform further
    processing.
    """
    return ast.Table(*arguments, **kwargs)


def db(db_name: str):  # pylint: disable=invalid-name
    """
    Reference a database.

    The db command is optional. If it is not present in a query, the query will
    run against the database specified in the db argument given to run if one
    was specified. Otherwise, the query will run against the default database
    for the connection, specified in the db argument to connect.
    """
    return ast.DB(db_name)


def db_create(db_name: str):
    """
    Create a database. A RethinkDB database is a collection of tables, similar
    to relational databases.
    """
    return ast.DbCreate(db_name)


def db_drop(db_name: str):
    """
    Drop a database. The database, all its tables, and corresponding data will
    be deleted.
    """
    return ast.DbDrop(db_name)


def db_list():
    """
    List all database names in the system. The result is a list of strings.
    """
    return ast.DbList()


def db_config(*arguments):
    """
    DB config is for retrieving the database configuration.
    """
    return ast.Config(*arguments)


def table_create(table_name: str, *arguments, **kwargs):
    """
    Create a table. A RethinkDB table is a collection of JSON documents.
    """
    return ast.TableCreateTL(table_name, *arguments, **kwargs)


def table_drop(table_name: str):
    """
    Drop a table. The table and all its data will be deleted.
    """
    return ast.TableDropTL(table_name)


def table_list():
    """
    List all table names in a database. The result is a list of strings.
    """
    return ast.TableListTL()


def grant(*arguments, **kwargs):
    """
    Grant or deny access permissions for a user account, globally or on a
    per-database or per-table basis.
    """
    return ast.GrantTL(*arguments, **kwargs)


def branch(*arguments):
    """
    Perform a branching conditional equivalent to if-then-else.

    The branch command takes 2n+1 arguments: pairs of conditional expressions
    and commands to be executed if the conditionals return any value but False
    or None (i.e., “truthy” values), with a final “else” command to be
    evaluated if all of the conditionals are False or None.
    """
    return ast.Branch(*arguments)


def union(*arguments):
    """
    Merge two or more sequences.
    """
    return ast.Union(*arguments)


def map(*arguments):  # pylint: disable=redefined-builtin
    """
    Transform each element of one or more sequences by applying a mapping
    function to them. If map is run with two or more sequences, it will
    iterate for as many items as there are in the shortest sequence.

    Note that map can only be applied to sequences, not single values. If
    you wish to apply a function to a single value/selection (including an
    array), use the do command.
    """
    if len(arguments) > 0:
        # `func_wrap` only the last argument
        return ast.Map(*(arguments[:-1] + (ast.func_wrap(arguments[-1]),)))

    return ast.Map()


def group(*arguments):
    """
    Takes a stream and partitions it into multiple groups based on the fields
    or functions provided.

    With the multi flag single documents can be assigned to multiple groups,
    similar to the behavior of multi-indexes. When multi is True and the
    grouping value is an array, documents will be placed in each group that
    corresponds to the elements of the array. If the array is empty the row
    will be ignored.
    """
    return ast.Group(*(ast.func_wrap(arg) for arg in arguments))


def reduce(*arguments):
    """
    Produce a single value from a sequence through repeated application of a
    reduction function.
    """
    return ast.Reduce(*(ast.func_wrap(arg) for arg in arguments))


def count(*arguments):
    """
    Counts the number of elements in a sequence or key/value pairs in an
    object, or returns the size of a string or binary object.

    When count is called on a sequence with a predicate value or function, it
    returns the number of elements in the sequence equal to that value or
    where the function returns True. On a binary object, count returns the
    size of the object in bytes; on strings, count returns the string's length.
    This is determined by counting the number of Unicode codepoints in the
    string, counting combining codepoints separately.
    """
    return ast.Count(*(ast.func_wrap(arg) for arg in arguments))


def sum(*arguments):  # pylint: disable=redefined-builtin
    """
    Sums all the elements of a sequence. If called with a field name, sums all
    the values of that field in the sequence, skipping elements of the sequence
    that lack that field. If called with a function, calls that function on
    every element of the sequence and sums the results, skipping elements of
    the sequence where that function returns None or a non-existence error.

    Returns 0 when called on an empty sequence.
    """
    return ast.Sum(*(ast.func_wrap(arg) for arg in arguments))


def avg(*arguments):
    """
    Averages all the elements of a sequence. If called with a field name,
    averages all the values of that field in the sequence, skipping elements of
    the sequence that lack that field. If called with a function, calls that
    function on every element of the sequence and averages the results, skipping
    elements of the sequence where that function returns None or a non-existence
    error.

    Produces a non-existence error when called on an empty sequence. You can
    handle this case with default.
    """
    return ast.Avg(*(ast.func_wrap(arg) for arg in arguments))


def min(*arguments):  # pylint: disable=redefined-builtin
    """
    Finds the minimum element of a sequence.
    """
    return ast.Min(*(ast.func_wrap(arg) for arg in arguments))


def max(*arguments):  # pylint: disable=redefined-builtin
    """
    Finds the maximum element of a sequence.
    """
    return ast.Max(*(ast.func_wrap(arg) for arg in arguments))


def distinct(*arguments):
    """
    Removes duplicate elements from a sequence.

    The distinct command can be called on any sequence or table with an index.
    """
    return ast.Distinct(*(ast.func_wrap(arg) for arg in arguments))


def contains(*arguments):
    """
    When called with values, returns True if a sequence contains all the
    specified values. When called with predicate functions, returns True if
    for each predicate there exists at least one element of the stream where
    that predicate returns True.

    Values and predicates may be mixed freely in the argument list.
    """
    return ast.Contains(*(ast.func_wrap(arg) for arg in arguments))


def asc(*arguments):
    """
    Sort the sequence by document values of the given key(s). To specify the
    ordering, wrap the attribute with either r.asc or r.desc (defaults to
    ascending).
    """
    return ast.Asc(*(ast.func_wrap(arg) for arg in arguments))


def desc(*arguments):
    """
    Sort the sequence by document values of the given key(s). To specify the
    ordering, wrap the attribute with either r.asc or r.desc (defaults to
    ascending).
    """
    return ast.Desc(*(ast.func_wrap(arg) for arg in arguments))


def eq(*arguments):  # pylint: disable=invalid-name
    """
    Equals function.
    """
    return ast.Eq(*arguments)


def ne(*arguments):  # pylint: disable=invalid-name
    """
    Not equal function.
    """
    return ast.Ne(*arguments)


def lt(*arguments):  # pylint: disable=invalid-name
    """
    Less than function.
    """
    return ast.Lt(*arguments)


def le(*arguments):  # pylint: disable=invalid-name
    """
    Less or equal than function.
    """
    return ast.Le(*arguments)


def gt(*arguments):  # pylint: disable=invalid-name
    """
    Greater than function.
    """
    return ast.Gt(*arguments)


def ge(*arguments):  # pylint: disable=invalid-name
    """
    Greater or equal than function.
    """
    return ast.Ge(*arguments)


def add(*arguments):
    """
    Add function.
    """
    return ast.Add(*arguments)


def sub(*arguments):
    """
    Subtract function.
    """
    return ast.Sub(*arguments)


def mul(*arguments):
    """
    Multiply function.
    """
    return ast.Mul(*arguments)


def div(*arguments):
    """
    Divide function.
    """
    return ast.Div(*arguments)


def mod(*arguments):
    """
    Module function.
    """
    return ast.Mod(*arguments)


def bit_and(*arguments):
    """
    Bitwise AND function.
    """
    return ast.BitAnd(*arguments)


def bit_or(*arguments):
    """
    Bitwise OR function.
    """
    return ast.BitOr(*arguments)


def bit_xor(*arguments):
    """
    Bitwise XOR function.
    """
    return ast.BitXor(*arguments)


def bit_not(*arguments):
    """
    Bit negation function.
    """
    return ast.BitNot(*arguments)


def bit_sal(*arguments):
    """
    In an arithmetic shift (also referred to as signed shift), like a logical
    shift, the bits that slide off the end disappear (except for the last,
    which goes into the carry flag). But in an arithmetic shift, the spaces are
    filled in such a way to preserve the sign of the number being slid. For
    this reason, arithmetic shifts are better suited for signed numbers in
    two's complement format.

    Note: SHL and SAL are the same, and differentiation only happens because
    SAR and SHR (right shifting) has differences in their implementation.
    """
    return ast.BitSal(*arguments)


def bit_sar(*arguments):
    """
    In an arithmetic shift (also referred to as signed shift), like a logical
    shift, the bits that slide off the end disappear (except for the last,
    which goes into the carry flag). But in an arithmetic shift, the spaces
    are filled in such a way to preserve the sign of the number being slid.
    For this reason, arithmetic shifts are better suited for signed numbers
    in two's complement format.
    """
    return ast.BitSar(*arguments)


def floor(*arguments):
    """
    Floor function.
    """
    return ast.Floor(*arguments)


def ceil(*arguments):
    """
    Ceil function.
    """
    return ast.Ceil(*arguments)


def round(*arguments):  # pylint: disable=redefined-builtin
    """
    Round function.
    """
    return ast.Round(*arguments)


def not_(*arguments):
    """
    Not function.
    """
    return ast.Not(*arguments)


def and_(*arguments):
    """
    AND function.
    """
    return ast.And(*arguments)


def or_(*arguments):
    """
    OR function.
    """
    return ast.Or(*arguments)


def type_of(*arguments):
    """
    Type function.
    """
    return ast.TypeOf(*arguments)


def info(*arguments):
    """
    Information function.
    """
    return ast.Info(*arguments)


def binary(data):
    """
    Binary function.
    """
    return ast.Binary(data)


def range(*arguments):  # pylint: disable=redefined-builtin
    """
    Range function.
    """
    return ast.Range(*arguments)


def make_timezone(*arguments):
    """
    Add timezone function.
    """
    return ast.ReqlTzinfo(*arguments)


def time(*arguments):
    """
    Time function.
    """
    return ast.Time(*arguments)


def iso8601(*arguments, **kwargs):
    """
    ISO8601 function.
    """
    return ast.ISO8601(*arguments, **kwargs)


def epoch_time(*arguments):
    """
    Epoch time function.
    """
    return ast.EpochTime(*arguments)


def now(*arguments):
    """
    Now function.
    """
    return ast.Now(*arguments)


# Merge values

def literal(*arguments):
    """
    Replace an object in a field instead of merging it with an existing object
    in a merge or update operation. = Using literal with no arguments in a
    merge or update operation will remove the corresponding field.
    """
    return ast.Literal(*arguments)


def object(*arguments):  # pylint: disable=redefined-builtin
    """
    Creates an object from a list of key-value pairs, where the keys must be
    strings. r.object(A, B, C, D) is equivalent to
    r.expr([[A, B], [C, D]]).coerce_to('OBJECT').
    """
    return ast.Object(*arguments)


def uuid(*arguments):
    """
    Return a UUID (universally unique identifier), a string that can be used as
    a unique ID. If a string is passed to uuid as an argument, the UUID will be
    deterministic, derived from the string's SHA-1 hash.
    """
    return ast.UUID(*arguments)


# Global geospatial operations

def geojson(*arguments):
    """
    Convert a GeoJSON object to a Reql geometry object.

    RethinkDB only allows conversion of GeoJSON objects which have Reql
    equivalents: Point, LineString, and Polygon. MultiPoint, MultiLineString,
    and MultiPolygon are not supported. (You could, however, store multiple
    points, lines and polygons in an array and use a geospatial multi index
    with them.)
    """
    return ast.GeoJson(*arguments)


def point(*arguments):
    """
    Construct a geometry object of type Point. The point is specified by two
    floating point numbers, the longitude (-180 to 180) and latitude (-90 to 90)
    of the point on a perfect sphere
    """
    return ast.Point(*arguments)


def line(*arguments):
    """
    Construct a geometry object of type Line.
    """
    return ast.Line(*arguments)


def polygon(*arguments):
    """
    Construct a geometry object of type Polygon.
    """
    return ast.Polygon(*arguments)


def distance(*arguments, **kwargs):
    """
    Compute the distance between a point and another geometry object. At least
    one of the geometry objects specified must be a point.
    """
    return ast.Distance(*arguments, **kwargs)


def intersects(*arguments):
    """
    Tests whether two geometry objects intersect with one another. When applied
    to a sequence of geometry objects, intersects acts as a filter, returning a
    sequence of objects from the sequence that intersect with the argument.
    """
    return ast.Intersects(*arguments)


def circle(*arguments, **kwargs):
    """
    Construct a circular line or polygon. A circle in RethinkDB is a polygon or
    line approximating a circle of a given radius around a given center,
    consisting of a specified number of vertices (default 32).
    """
    return ast.Circle(*arguments, **kwargs)


def format(*arguments, **kwargs):  # pylint: disable=redefined-builtin
    """
    Format command takes a string as a template and formatting parameters as an
    object. The parameters in the template string must exist as keys in the
    object, otherwise, an error raised. The template must be a string literal
    and cannot be the result of other commands.
    """
    return ast.Format(*arguments, **kwargs)


row = ast.ImplicitVar()

# Time enum values
_TERM_TYPE = ql2_pb2.Term.TermType

# Convert days of week into constants
monday = ast.ReqlConstant("monday", _TERM_TYPE.MONDAY)
tuesday = ast.ReqlConstant("tuesday", _TERM_TYPE.TUESDAY)
wednesday = ast.ReqlConstant("wednesday", _TERM_TYPE.WEDNESDAY)
thursday = ast.ReqlConstant("thursday", _TERM_TYPE.THURSDAY)
friday = ast.ReqlConstant("friday", _TERM_TYPE.FRIDAY)
saturday = ast.ReqlConstant("saturday", _TERM_TYPE.SATURDAY)
sunday = ast.ReqlConstant("sunday", _TERM_TYPE.SUNDAY)

# Convert months of the year into constants
january = ast.ReqlConstant("january", _TERM_TYPE.JANUARY)
february = ast.ReqlConstant("february", _TERM_TYPE.FEBRUARY)
march = ast.ReqlConstant("march", _TERM_TYPE.MARCH)
april = ast.ReqlConstant("april", _TERM_TYPE.APRIL)
may = ast.ReqlConstant("may", _TERM_TYPE.MAY)
june = ast.ReqlConstant("june", _TERM_TYPE.JUNE)
july = ast.ReqlConstant("july", _TERM_TYPE.JULY)
august = ast.ReqlConstant("august", _TERM_TYPE.AUGUST)
september = ast.ReqlConstant("september", _TERM_TYPE.SEPTEMBER)
october = ast.ReqlConstant("october", _TERM_TYPE.OCTOBER)
november = ast.ReqlConstant("november", _TERM_TYPE.NOVEMBER)
december = ast.ReqlConstant("december", _TERM_TYPE.DECEMBER)

minval = ast.ReqlConstant("minval", _TERM_TYPE.MINVAL)
maxval = ast.ReqlConstant("maxval", _TERM_TYPE.MAXVAL)

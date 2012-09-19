"""To communicate with a RethinkDB server, construct queries using this module
and then pass them to the server using the :mod:`rethinkdb.net` module.

.. contents::

.. autoclass:: BaseQuery
    :members:

.. autoclass:: ReadQuery
    :members:

JSON expressions
================

.. autoclass:: JSONExpression
    :members:

    .. automethod:: __getitem__

    .. automethod:: __eq__
    .. automethod:: __ne__
    .. automethod:: __lt__
    .. automethod:: __le__
    .. automethod:: __gt__
    .. automethod:: __ge__

    .. automethod:: __add__
    .. automethod:: __sub__
    .. automethod:: __mul__
    .. automethod:: __div__
    .. automethod:: __mod__
    .. automethod:: __neg__

    .. automethod:: __or__
    .. automethod:: __and__
    .. automethod:: __invert__

Stream expressions
==================

.. autoclass:: StreamExpression
    :members:

    .. automethod:: __getitem__

.. autofunction:: expr
.. autofunction:: if_then_else
.. autofunction:: R
.. autofunction:: js
.. autofunction:: let
.. autofunction:: fn
.. autoclass:: JSONFunction
.. autoclass:: StreamFunction

Write queries
=============

.. autoclass:: BaseSelection
    :members:
.. autoclass:: RowSelection
.. autoclass:: MultiRowSelection
.. autoclass:: WriteQuery

Manipulating databases and tables
=================================

.. autoclass:: MetaQuery
.. autofunction:: db_create
.. autofunction:: db_drop
.. autofunction:: db_list
.. autoclass:: Database
    :members:
.. autofunction:: db
.. autoclass:: Table
.. autofunction:: table
"""

import query_language_pb2 as p
import net

class BaseQuery(object):
    """A base class for all ReQL queries. Queries can be run by calling the
    :meth:`rethinkdb.net.Connection.run()` method or by calling :meth:`run()` on
    the query object itself.

    There are two types of queries: expressions and write queries. Expressions
    are instances of :class:`JSONExpression` or :class:`StreamExpression`. They
    can be as simple as fetching a single document, or even just doing some
    arithmetic server-side, or as complicated as joins involving subqueries and
    multiple tables, but expressions never modify the database. Write queries
    are instances of :class:`WriteQuery`."""

    def _finalize_query(self, root):
        raise NotImplementedError()

    def run(self, conn=None):
        """Evaluate the expression on the server using the connection
        specified by `conn`. If `conn` is empty, uses the last created
        connection (located in :data:`rethinkdb.net.last_connection`).

        This method is shorthand for
        :func:`rethinkdb.net.Connection.run` - see its documentation
        for more details.

        :param conn: An optional connection object used to evaluate
          the expression on the RethinkDB server.
        :type conn: :class:`rethinkdb.net.Connection`
        :returns: See the documentation for :func:`rethinkdb.net.Connection.run`

        >>> conn = rethinkdb.net.connect() # Connect to localhost, default port
        >>> res = table('db_name.table_name').insert({ 'a': 1, 'b': 2 }).run(conn)
        >>> res = table('db_name.table_name').run() # uses conn since it's the last created connection
        """
        if conn is None:
            if net.last_connection is None:
                raise StandardError("Call rethinkdb.net.connect() to connect to a server before calling run()")
            conn = net.last_connection
        return conn.run(self)

class ReadQuery(BaseQuery):
    """Base class for expressions"""

    def __init__(self, inner):
        assert isinstance(inner, internal.ExpressionInner)
        self._inner = inner

    def __str__(self):
        return internal.ReprPrettyPrinter().expr_wrapped(self, [])

    def _finalize_query(self, root):
        root.type = p.Query.READ
        self._inner._write_ast(root.read_query.term)

class JSONExpression(ReadQuery):
    """An expression that evaluates to a JSON value.

    Use :func:`expr` to create a :class:`JSONExpression` that encodes a literal
    JSON value:

    >>> expr("foo").run()
    "foo"
    >>> expr([1, 2, 3]).run()
    [1, 2, 3]
    >>> expr([expr(1) + expr(2), 2, 1]).run()
    [3, 2, 1]

    Literal Python strings, booleans, numbers, arrays and dictionaries, as well
    as `None`, can also be implicitly cast to :class:`JSONExpression`:

    >>> (expr(1) + 2).run()   # `2` is implicitly converted
    3

    :class:`JSONExpression` overloads Python operators wherever possible to
    implement arithmetic, attribute access, and so on.
    """

    def __repr__(self):
        return "<JSONExpression %s>" % str(self)

    def __getitem__(self, index):
        """If `index` is a string, expects `self` to evaluate to an object and
        fetches the key called `index` from the object:

        >>> expr({"a": 1})["a"].run()
        1

        If the key is not present, or if `self` does not evaluate to an object,
        fails when the query is evaluated.

        If `index` is not a string, it is interpreted as an array index:

        >>> expr([1, 2, 3, 4])[expr(8) - expr(6)].run()
        4

        Ranges work as well:

        >>> expr([1, 2, 3, 4])[1:2].run()
        [2]

        If the index to fetch is out of range, or `self` does not evaluate to an
        array, then it fails when the query is evaluated.

        :param index: The key to fetch from an object, or index or slice to
            fetch from an array.
        :type index: string, :class:`JSONExpression`, or slice containing :class:`JSONExpression`
        :returns: :class:`JSONExpression`
        """
        if isinstance(index, slice):
            if index.step is not None:
                raise ValueError("slice stepping is unsupported")
            return JSONExpression(internal.Slice(self, index.start, index.stop))
        elif isinstance(index, str):
            return JSONExpression(internal.Attr(self, index))
        else:
            return JSONExpression(internal.Nth(self, index))

    def __eq__(self, other):
        """Evaluates to `true` if `self` evaluates to the same value as `other`.

        :param other: The object to compare against
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to a boolean
        """
        return JSONExpression(internal.CompareEQ(self, other))

    def __ne__(self, other):
        """Evaluates to `true` if `self` evaluates to the same value as `other`.
         Equivalent to `~(self == other)`.

        :param other: The object to compare against
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to a boolean
        """
        return JSONExpression(internal.CompareNE(self, other))

    def __lt__(self, other):
        """Evaluates to `true` if `self` evaluates to a value that is strictly
        less than what `other` evaluates to.

        TODO: define ordering

        :param other: The object to compare against
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to a boolean
        """
        return JSONExpression(internal.CompareLT(self, other))

    def __le__(self, other):
        """Evaluates to `true` if `self` evaluates to a value that is less than
        or equal to what `other` evaluates to.

        TODO: define ordering

        :param other: The object to compare against
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to a boolean
        """
        return JSONExpression(internal.CompareLE(self, other))

    def __gt__(self, other):
        """Evaluates to `true` if `self` evaluates to a value that is strictly
        greater than what `other` evaluates to.

        TODO: define ordering

        :param other: The object to compare against
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to a boolean
        """
        return JSONExpression(internal.CompareGT(self, other))

    def __ge__(self, other):
        """Evaluates to `true` if `self` evaluates to a value that is greater
        than or equal to what `other` evaluates to.

        TODO: define ordering

        :param other: The object to compare against
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to a boolean
        """
        return JSONExpression(internal.CompareGE(self, other))

    def __add__(self, other):
        """Sums two numbers, or concatenates two arrays.

        If the types are different, fails when the query is run.

        TODO: strings

        :param other: The number to add or array to concatenate with `self`
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression`
        """
        return JSONExpression(internal.Add(self, other))

    def __sub__(self, other):
        """Subtracts two numbers.

        If `self` or `other` is not a number, fails when the query is run.

        :param other: The number to subtract from `self`
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression`
        """
        return JSONExpression(internal.Sub(self, other))

    def __mul__(self, other):
        """Multiplies two numbers.

        If `self` or `other` is not a number, fails when the query is run.

        :param other: The number to multiply `self` by
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression`
        """
        return JSONExpression(internal.Mul(self, other))

    def __div__(self, other):
        """Divides two numbers.

        If `self` or `other` is not a number, fails when the query is run. If
        `other` is zero, evaluates to `NaN`.

        :param other: The number to divide `self` by
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression`
        """
        return JSONExpression(internal.Div(self, other))

    def __mod__(self, other):
        """Computes the modulus of two numbers.

        If `self` or `other` is not a number, fails when the query is run. If
        `other` is zero, evaluates to `NaN`.

        :param other: The number to compute the modulus with
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression`
        """
        return JSONExpression(internal.Mod(self, other))

    def __radd__(self, other):
        return JSONExpression(internal.Add(other, self))
    def __rsub__(self, other):
        return JSONExpression(internal.Sub(other, self))
    def __rmul__(self, other):
        return JSONExpression(internal.Mul(other, self))
    def __rdiv__(self, other):
        return JSONExpression(internal.Div(other, self))
    def __rmod__(self, other):
        return JSONExpression(internal.Mod(other, self))

    def __neg__(self):
        """Negates a number.

        If `self` is not a number, fails when the query is run.

        :returns: :class:`JSONExpression`
        """
        return JSONExpression(internal.Negate(self))

    def __or__(self, other):
        """Computes the boolean "or" of `self` and `other`.

        If `self` or `other` is not a boolean, fails when the query is run.

        :param other: The value to "or" with `self`
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to a boolean

        >>> (expr(True) | (expr(3) < 2)).run()
        True
        """
        return JSONExpression(internal.Any(self, other))

    def __and__(self, other):
        """Computes the boolean "and" of `self` and `other`.

        If `self` or `other` is not a boolean, fails when the query is run.

        :param other: The value to "and" with `self`
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to a boolean

        >>> ((expr(4) < 3) & (expr(8) > 7)).run()
        False
        """
        return JSONExpression(internal.All(self, other))

    def __ror__(self, other):
        return JSONExpression(internal.Any(other, self))
    def __rand__(self, other):
        return JSONExpression(internal.All(other, self))

    def __invert__(self):
        """Computes the boolean "not" of `self`.

        If `self` is not a boolean, fails when the query is run.

        :returns: :class:`JSONExpression` evaluating to a boolean

        >>> (~expr(True)).run()
        False
        """
        return JSONExpression(internal.Not(self))

    def has_attr(self, name):
        """Determines whether an object has a key called `name`.
        Evaluates to `true` if the key is present, or `false` if not.

        If `self` is not an object, fails when the query is run.

        :param name: The key to check for
        :type name: string
        :returns: :class:`JSONExpression` evaluating to a boolean

        >>> expr({}).has_attr("foo").run()
        False
        >>> expr({"a": 1}).has_attr("a").run()
        True
        """
        return JSONExpression(internal.Has(self, name))

    def extend(self, other):
        """Combines two objects by taking all the key-value pairs from both. If
        a given key is present in both objects, take the value from `other`.

        If `self` or `other` is not an object, fails when the query is run.

        :param other: The object to take additional key-value pairs from
        :type other: :class:`JSONExpression`
        :returns: :class:`JSONExpression` evaluating to an object

        >>> expr({"a": 1, "b": 2}).extend({"b": 3, "c": 4}).run()
        {"a": 1, "b": 3, "c": 4}
        """
        return JSONExpression(internal.Extend(self, other))

    def append(self, other):
        return JSONExpression(internal.Append(self, other))

    # TODO: Implement `range()` for arrays as soon as the server supports it.

    def filter(self, predicate):
        """Apply the given predicate to each element of an array, and evaluate
        to an array with only those elements that match the predicate.

        This is like :meth:`StreamExpression.filter`, but with arrays instead of
        streams. See :meth:`StreamExpression.filter` for an explanation of the
        format of `predicate`.

        If the input is not an array, fails when the query is run.

        :param predicate: the predicate to filter with
        :type predicate: dict, :class:`JSONExpression`, or :class:`JSONFunction`
        :returns: :class:`JSONExpression`

        >>> expr([1, 2, 3, 4, 5]).filter(fn("x", R("$x") > 2)).run()
        [3, 4, 5]
        """
        if isinstance(predicate, dict):
            predicate = JSONExpression(internal.All(*[R(k) == v for k, v in predicate.iteritems()]))
        if not isinstance(predicate, JSONFunction):
            predicate = JSONFunction(predicate)

        return JSONExpression(internal.Filter(self, predicate))

    def skip(self, offset):
        """Skip the first `offset` elements of an array.

        This is like :meth:`StreamExpression.skip` but with arrays instead of
        streams.

        If the input is not an array, fails when the query is run.

        .. note:: ``e.skip(offset)`` is equivalent to ``e[offset:]``.

        :param offset: The number of elements to skip.
        :type offset: int
        :returns: :class:`JSONExpression`
        """
        return self[offset:]

    def limit(self, count):
        """Truncates an array at `count` elements.

        This is like :meth:`StreamExpression.limit` but with arrays instead of
        streams.

        If the input is not an array, fails when the query is run.

        .. note:: ``e.limit(count)`` is equivalent to ``e[:count]``.

        :param count: The number of elements to select.
        :type count: int
        :returns: :class:`JSONExpression`
        """
        return self[:count]

    def orderby(self, *attributes):
        """Sorts an array of objects according to the given attributes.

        Items are sorted in ascending order unless the attribute name starts
        with '-', which sorts the attribute in descending order.

        This is like :meth:`StreamExpression.orderby` but with arrays instead
        of streams.

        If the input is not an array, fails when the query is run.

        :param attributes: attribute names to order by
        :type attributes: strings
        :returns: :class:`JSONExpression`
        """
        order = []
        for attr in attributes:
            if attr.startswith('-'):
                order.append((attr[1:], False))
            else:
                order.append((attr, True))
        return JSONExpression(internal.OrderBy(self, order))

    def map(self, mapping):
        """Applies the given function to each element of an array.

        This is like :meth:`StreamExpression.map` but with arrays instead of
        streams.

        If the input is not an array, fails when the query is run.

        :param mapping: The function to evaluate
        :type mapping: :class:`JSONExpression` or :class:`JSONFunction`
        :returns: :class:`JSONExpression`

        >>> expr([1, 2, 3]).map(fn("x", R('$x') * 2)).run()
        [2, 4, 6]
        """
        if not isinstance(mapping, JSONFunction):
            mapping = JSONFunction(mapping)
        return JSONExpression(internal.Map(self, mapping))

    def concat_map(self, mapping):
        """Applies the given function to each element of an array. The result of
        `mapping` must be a stream; all those streams will be concatenated to
        produce the result of `concat_map()`, which is an array.

        This is like :meth:`StreamExpression.concat_map` but with arrays instead
        of streams.

        If the input is not an array, fails when the query is run.

        :param mapping: The mapping to evaluate
        :type mapping: :class:`StreamExpression` or :class:`StreamFunction`
        :returns: :class:`JSONExpression`

        >>> expr([1, 2, 3]).concat_map(fn("a", expr([R("$a"), "test"]).to_stream())).run()
        [1, "test", 2, "test", 3, "test"]
        """
        if not isinstance(mapping, StreamFunction):
            mapping = StreamFunction(mapping)
        return JSONExpression(internal.ConcatMap(self, mapping))

    def reduce(self, base, func):
        """Combines all the elements of an array into one value by repeatedly
        applying `func` to pairs of values. Returns `base` if the array is
        empty.

        `base` should be an identity; that is, `func(base, e) == e`.

        This is like :meth:`StreamExpression.reduce`, but with an array instead
        of a stream.

        If the input is not an array, fails when the query is run.

        :param base: The identity of the reduction
        :type base: :class:`JSONExpression`
        :param func: The function to use to combine things
        :type func: :class:`JSONFunction`
        :rtype: :class:`JSONExpression`

        >>> expr([1, 2, 3]).reduce(0, fn('a', 'b', R('$a') + R('$b'))).run()
        6
        """
        assert isinstance(func, JSONFunction)
        return JSONExpression(internal.Reduce(self, base, func))

    def grouped_map_reduce(self, group_mapping, value_mapping, reduction_base, reduction_func):
        """This is like :meth:`StreamExpression.grouped_map_reduce()`, but with
        an array instead of a stream. See
        :meth:`StreamExpression.grouped_map_reduce()` for an explanation.

        If the input is not an array, fails when the query is run.
        """
        if not isinstance(group_mapping, JSONFunction):
            group_mapping = JSONFunction(group_mapping)
        if not isinstance(value_mapping, JSONFunction):
            value_mapping = JSONFunction(value_mapping)
        return JSONExpression(internal.GroupedMapReduce(self, group_mapping, value_mapping, reduction_base, reduction_func))

    def distinct(self):
        """Discards duplicate elements from an array.

        This is like :meth:`StreamExpression.distinct` except with arrays
        instead of streams.

        If the input is not an array, fails when the query is run.

        :returns: :class:`JSONExpression`

        >>> expr([1, 9, 1, 1, 3, 8, 3]).distinct().run()
        [1, 9, 3, 8]
        """
        return JSONExpression(internal.Distinct(self))

    def pluck(self, *attrs):
        """For each element of an array, picks out the specified
        attributes from the object and returns only those.

        This is like :meth:`StreamExpression.pluck()`, but with arrays instead
        of streams.

        If the input is not an array, fails when the query is run.

        :param attrs: The attributes to pluck out
        :type attrs: strings
        :returns: :class:`JSONExpression`
        
        >>> expr([{ 'a': 1, 'b': 1, 'c': 1},
                  { 'a': 2, 'b': 2, 'c': 2}]).pluck('a', 'b').run()
        [{ 'a': 1, 'b': 1 }, { 'a': 2, 'b': 2 }]
        """
        # TODO: reimplement in terms of pickattr when that's done
        return self.map(fn("r", {a: R("$r")[a] for a in attrs}))

    def length(self):
        """Returns the length of an array.

        TODO: Strings?

        :returns: :class:`JSONExpression`

        >>> expr([1, 2, 3]).length().run()
        3
        """
        return JSONExpression(internal.Length(self))

    def __len__(self):
        raise ValueError("To construct a `rethinkdb.JSONExpression` "
            "representing the length of a RethinkDB protocol term, call "
            "`expr.length()`. (We couldn't overload `len(expr)` because it's "
            "illegal to return anything other than an integer from `__len__()` "
            "in Python.)")

    def to_stream(self):
        """Converts a JSON array to a stream. This is the reverse of
        :meth:`StreamExpression.to_array()`.

        If the input is not an array, fails when the query is run.

        :returns: :class:`StreamExpression`
        """
        return StreamExpression(internal.ToStream(self))

class StreamExpression(ReadQuery):
    """A sequence of JSON values which can be read."""

    def __repr__(self):
        return "<StreamExpression %s>" % str(self)

    def _make_selector(self, inner):
        if isinstance(self, MultiRowSelection):
            return MultiRowSelection(inner)
        else:
            return StreamExpression(inner)

    def to_array(self):
        """Convert the stream into a JSON array.

        :returns: :class:`JSONExpression`"""
        return JSONExpression(internal.ToArray(self))

    def range(self, lower_bound, upper_bound, attr_name = "id"):
        """Filter a stream of objects according to whether the `attr_name`
        attribute of each object falls between `lower_bound` and `upper_bound`.
        Both bounds are inclusive.

        The most common use case for this is to filter tables by primary key.
        RethinkDB will take advantage of the primary key index if you call
        :meth:`range()` on a :class:`Table` where `attr_name` is the primary
        key.

        :param lower_bound: lower bound of range, inclusive
        :type lower_bound: :class:`JSONExpression`
        :param upper_bound: upper bound of range, inclusive
        :type upper_bound: :class:`JSONExpression`
        :returns: :class:`StreamExpression` or :class:`MultiRowSelection` (same as input)
        """
        return self._make_selector(internal.Range(self, lower_bound, upper_bound, attr_name))

    def filter(self, predicate):
        """Apply the given predicate to each element of the stream, and return
        a stream with only those elements that match the predicate.

        There are a number of ways to specify a predicate for :meth:`filter`.
        The simplest way is to pass a dict that defines a JSON document:

        >>> table('users').filter( { 'age': 30, 'state': 'CA'}) # select all thirty year olds in california

        We can also pass ReQL expressions directly. The above query is
        equivalent to the following query:

        >>> table('users').filter((R('age') == 30) & (R('state') == 'CA')))

        The values in a dict can contain ReQL expressions - they will get
        evaluated in order to evaluate the condition:

        >>> # Select all Californians whose age is equal to the number
        >>> # of colleges attended added to the number of jobs held
        >>> table('users').filter( { 'state': 'CA', 'age': R('jobs_held') + R('colleges_attended') })

        We can of course specify this query as a ReQL expression directly:

        >>> table('users').filter((R('state') == 'CA') &
        ...                       (R('age') == R('jobs_held') + R('colleges_attended')))

        We can use subqueries as well:

        >>> # Select all Californians whose age is equal to the number
        >>> of users in the database
        >>> table('users').filter( { 'state': 'CA', 'age': table('users').length() })

        So far we've been grabbing attributes from the implicit scope. We can
        bind the value of each row to a variable and operate on that:

        >>> table('users').filter(fn('row', R('$row.state') == 'CA' &
        ...                                 R('$row.age') == R('$row.jobs_held') + R('$row.colleges_attended')))

        This type of syntax allows us to execute inner subqueries that refer to
        the outer row:

        >>> # Select all users whose age is equal to a number of blog
        >>> # posts written by all users with the same first name:
        >>> table('users').filter(fn('user',
        ...     R('$user.age') == table('posts').filter(fn('post',
        ...         R('$post.author.first_name') == R('$user.first_name')))
        ...         .length()))

        :param predicate: the predicate to filter with
        :type predicate: dict, :class:`JSONExpression`, or :class:`JSONFunction`
        :returns: :class:`StreamExpression` or :class:`MultiRowSelection` (same as input)
        """
        if isinstance(predicate, dict):
            predicate = JSONExpression(internal.All(*[R(k) == v for k, v in predicate.iteritems()]))
        if not isinstance(predicate, JSONFunction):
            predicate = JSONFunction(predicate)

        return self._make_selector(internal.Filter(self, predicate))

    def __getitem__(self, index):
        """Extract the `index`'th element of the stream, or extract some
        sub-sequence of the stream.

        >>> expr([1, 2, 3, 4]).to_stream()[2]
        3
        >>> expr([1, 2, 3, 4]).to_stream()[1:2].to_array()
        [2]
        >>> expr([1, 2, 3, 4]).to_stream()[1:].to_array()
        [2, 3, 4]

        :param index: the index or slice to fetch
        :type index: :class:`JSONExpression`, or slice containing :class:`JSONExpression`
        :returns: :class:`JSONExpression` (if single index) or :class:`StreamExpression` (if slice)
        """
        if isinstance(index, slice):
            if index.step is not None:
                raise ValueError("slice stepping is unsupported")
            return self._make_selector(internal.Slice(self, index.start, index.stop))
        else:
            return JSONExpression(internal.Nth(self, index))

    def skip(self, offset):
        """Skip the first `offset` elements of the stream.

        .. note:: ``e.skip(offset)`` is equivalent to ``e[offset:]``.

        :param offset: The number of elements to skip.
        :type offset: int
        :returns: :class:`StreamExpression` or :class:`MultiRowSelection` (same as input)
        """
        return self[offset:]

    def limit(self, count):
        """Truncate the stream at `count` elements.

        .. note:: ``e.limit(count)`` is equivalent to ``e[:count]``.

        :param count: The number of elements to select.
        :type count: int
        :returns: :class:`StreamExpression` or :class:`MultiRowSelection` (same as input)
        """
        return self[:count]

    def orderby(self, *attributes):
        """Sort the stream according to the given attributes.

        Items are sorted in ascending order unless the attribute name starts
        with '-', which sorts the attribute in descending order.

        TODO: What if an attribute is missing?

        :param attributes: attribute names to order by
        :type attributes: strings
        :returns: :class:`StreamExpression` or :class:`MultiRowSelection` (same as input)

        >>> table('users').orderby('name')  # order users by name A-Z
        >>> table('users').orderby('-level', 'name') # levels high-low, then names A-Z
        """
        order = []
        for attr in attributes:
            if attr.startswith('-'):
                order.append((attr[1:], False))
            else:
                order.append((attr, True))
        return self._make_selector(internal.OrderBy(self, order))

    def map(self, mapping):
        """Applies the given function to each element of the stream.

        :param mapping: The function to evaluate
        :type mapping: :class:`JSONExpression` or :class:`JSONFunction`
        :returns: :class:`StreamExpression`

        >>> table('users').map(R('age'))
        >>> table('users').map(fn('user', table('posts').filter({'userid': R('$user.id')})))
        """
        if not isinstance(mapping, JSONFunction):
            mapping = JSONFunction(mapping)
        return StreamExpression(internal.Map(self, mapping))

    def concat_map(self, mapping):
        """Applies the given function to each element of the stream. The result
        of `mapping` must be a stream; all those streams will be concatenated to
        produce the result of `concat_map()`.

        :param mapping: The mapping to evaluate
        :type mapping: :class:`StreamExpression` or :class:`StreamFunction`
        :returns: :class:`StreamExpression`
        """
        if not isinstance(mapping, StreamFunction):
            mapping = StreamFunction(mapping)
        return StreamExpression(internal.ConcatMap(self, mapping))

    def reduce(self, base, func):
        """Combines all the elements of a stream into one value by repeatedly
        applying `func` to pairs of values. Returns `base` if the stream is
        empty.

        `base` should be an identity; that is, `func(base, e) == e`.

        :param base: The identity of the reduction
        :type base: :class:`JSONExpression`
        :param func: The function to use to combine things
        :type func: :class:`JSONFunction`
        :rtype: :class:`JSONExpression`

        """
        if not isinstance(func, JSONFunction):
            func = JSONFunction(func)
        return JSONExpression(internal.Reduce(self, base, func))

    def grouped_map_reduce(self, group_mapping, value_mapping, reduction_base, reduction_func):
        """Does the equivalent of SQL's `GROUP BY`. First, the elements of the
        stream are grouped by applying `group_mapping` to each one and then
        bucketing them by the result. Next, each bucket is combined into a
        single value by first applying `value_mapping` to each value and then
        reducing the results using `reduction_base` and `reduction_func` as in
        :meth:`reduce()`. The result is a JSON object where the keys are the
        results from `group_mapping` and the values are the results of the
        reduction.

        :param group_mapping: Function to group values by
        :type group_mapping: :class:`JSONFunction`
        :param value_mapping: Function to transform values by before reduction
        :type value_mapping: :class:`JSONFunction`
        :param reduction_base: base value for reduction, as in :meth:`reduce()`
        :type reduction_base: :class:`JSONExpression`
        :param reduction_func: combiner function for reduction
        :type reduction_func: :class:`JSONFunction`

        >>> # This will compute the total value of the expenses in each category
        >>> table('expenses').grouped_map_reduce(
        ...     fn("e", R("$e.category_name")),
        ...     fn("e", R("$e.dollar_value")),
        ...     0,
        ...     fn("a", "b", R("$a") + R("$b"))
        ...     ).run()
        {"employees": 409950, "rent": 214000, "inventory": 386533}
        """
        if not isinstance(group_mapping, JSONFunction):
            group_mapping = JSONFunction(group_mapping)
        if not isinstance(value_mapping, JSONFunction):
            value_mapping = JSONFunction(value_mapping)
        return JSONExpression(internal.GroupedMapReduce(self, group_mapping, value_mapping, reduction_base, reduction_func))

    def distinct(self):
        """Discards duplicate elements from a stream.

        :returns: :class:`StreamExpression`
        """
        return StreamExpression(internal.Distinct(self))

    def pluck(self, *attrs):
        """For each row of the stream, picks out the specified
        attributes from the object and returns only those.

        :param attrs: The attributes to pluck out
        :type attrs: strings
        :returns: :class:`JSONExpression`
        
        >>> table('foo').insert([{ 'a': 1, 'b': 1, 'c': 1},
                                 { 'a': 2, 'b': 2, 'c': 2}]).run()
        >>> table('foo').pluck('a', 'b').run()
        <BatchedIterator [{ 'a': 1, 'b': 1 }, { 'a': 2, 'b': 2 }]>
        """
        # TODO: reimplement in terms of pickattr when that's done
        return self.map(fn("r", {a: R("$r")[a] for a in attrs}))

    def length(self):
        """Returns the length of the stream.

        :returns: :class:`JSONExpression`

        >>> table("users").length()   # Total number of users in the system
        """
        return JSONExpression(internal.Length(self))

    def __len__(self):
        raise ValueError("To construct a `rethinkdb.JSONExpression` "
            "representing the length of a RethinkDB protocol stream, call "
            "`expr.length()`. (We couldn't overload `len(expr)` because it's "
            "illegal to return anything other than an integer from `__len__()` "
            "in Python.)")

    def eq_join(self, left_attr_name, right_table, right_attr_name):
        """For each row in the input, look for a row in `right_table` whose
        value for `right_attr_name` is the same as the input row's value for
        `left_attr_name`. If such a row is found, merge the rows together.
        Returns a stream with all the merged rows.

        Currently, `right_attr_name` must be the primary key name for
        `right_table`.

        If a field is in both the row from the input stream and the row from
        `right_table`, the value from the input stream will be taken.

        :param left_attr_name: The attribute to match in the input
        :type left_attr_name: str
        :param right_table: The table to join against
        :type right_table: :class:`Table`
        :param right_attr_name: The attribute to match in `right_table` (must
            be the primary key)
        :type right_attr_name: str
        :returns: :class:`StreamExpression`
        """
        # This is a hack for the demo
        assert isinstance(left_attr_name, str)
        assert isinstance(right_table, Table)
        assert isinstance(right_attr_name, str)
        return self.concat_map(fn("left_row",
            expr([right_table.get(R("$left_row.%s" % left_attr_name), right_attr_name)]).to_stream() \
                .filter(fn("x", R("$x") != None)) \
                .map(fn("right_row", R("$right_row").extend(R("$left_row"))))
            ))

def expr(val):
    """Converts a python value to a ReQL :class:`JSONExpression`.

    :param val: Any Python value that can be converted to JSON.
    :returns: :class:`JSONExpression`

    >>> expr(1).run()
    1
    >>> expr("foo").run()
    "foo"
    >>> expr(["foo", 1]).run()
    ["foo", 1]
    >>> expr({ 'name': 'Joe', 'age': 30 }).run()
    {'name': 'Joe', 'age': 30}
    """
    if isinstance(val, JSONExpression):
        return val
    elif val is None:
        return JSONExpression(internal.LiteralNull())
    elif isinstance(val, bool):
        return JSONExpression(internal.LiteralBool(val))
    elif isinstance(val, (int, float)):
        return JSONExpression(internal.LiteralNumber(val))
    elif isinstance(val, (str, unicode)):
        return JSONExpression(internal.LiteralString(val))
    elif isinstance(val, list):
        return JSONExpression(internal.LiteralArray(val))
    elif isinstance(val, object):
        return JSONExpression(internal.LiteralObject(val))
    else:
        raise TypeError("%r is not a valid JSONExpression" % val)

def if_then_else(test, true_branch, false_branch):
    """If `test` returns `true`, evaluates to `true_branch`. If `test` returns
    `false`, evaluates to `false_branch`. If `test` returns a non-boolean value,
    fails when the query is run.

    `true_branch` and `false_branch` can be any subclass of
    :class:`ReadQuery`. They need not be the same, but they must be
    convertible to the same type; the type that they can both be converted to
    will be the return type of `if_then_else()`. So if one is a
    :class:`StreamExpression` and the other is a :class:`MultiRowSelection`, the
    result will be a :class:`StreamExpression`. But if one is a
    :class:`StreamExpression` and the other is a :class:`JSONExpression`, then
    `if_then_else()` will throw an exception rather than return an expression
    object at all.

    :param test: The condition to switch on
    :type test: :class:`JSONExpression`
    :param true_branch: The return value if `test` is `true`
    :param false_branch: The return value if `test` is `false`
    """
    if isinstance(true_branch, MultiRowSelection) and isinstance(false_branch, MultiRowSelection):
        t = MultiRowSelection
    elif isinstance(true_branch, StreamExpression) and isinstance(false_branch, StreamExpression):
        t = StreamExpression
    elif isinstance(true_branch, RowSelection) and isinstance(false_branch, RowSelection):
        t = RowSelection
    elif not isinstance(true_branch, StreamExpression) and not isinstance(false_branch, StreamExpression):
        true_branch = expr(true_branch)
        false_branch = expr(false_branch)
        t = JSONExpression
    return t(internal.If(test, true_branch, false_branch))

def R(string):
    """Get the value of a variable or attribute.

    To get a variable, prefix the name with `$`.

    >>> R('$user')

    To get attributes of variables, use dot notation.

    >>> R('$user.name')
    >>> R('$user.options.ads')

    Filter and map bind the current element to the implicit variable.

    To access an attribute of the implicit variable, pass the attribute name.

    >>> R('name')
    >>> R('options.ads')

    To get the implicit variable, use '@'.

    >>> R('@')

    For attributes that would be misinterpreted, use alternative notations.

    >>> R('@.$special')     # get implicit var's "$special" attribute
    >>> R('@')['$special']  # the same
    >>> R('@')['a.b.c']     # get an attribute named "a.b.c"

    See information on scoping rules for more details.

    :param name: The name of the variable (prefixed with `$`),
      implicit attribute (prefixed with `@`), or inner attributes
      (separated by `.`)
    :type name: str
    :returns: :class:`JSONExpression`

    >>> table('users').insert({ 'name': Joe,
                                'age': 30,
                                'address': { 'city': 'Mountain View', 'state': 'CA' }
                              }).run()
    >>> table('users').filter(R('age') == 30) # access attribute age from the implicit row variable
    >>> table('users').filter(R('address.city') == 'Mountain View') # access subattribute city
                                                                    # of attribute address from
                                                                    # the implicit row variable
    >>> table('users').filter(fn('row', R('$row.age') == 30)) # access attribute age from the
                                                              # variable 'row'
    >>> table('users').filter(fn('row', R('$row.address.city') == 'Mountain View')) # access subattribute city
                                                                                     # of attribute address from
                                                                                     # the variable 'row'
    >>> table('users').filter(fn('row', R('age') == 30)) # error - binding a row disables implicit scope
    >>> table('users').filter(fn('row', R('$age') == 30)) # error - no variable 'age' is defined
    >>> table('users').filter(R('$age') == 30) # error - no variable '$age' is defined, use 'age'
    """
    parts = string.split(".")
    if parts[0] == "@":
        raise NotImplementedError("R('@') is not implemented")
    elif parts[0].startswith("$"):
        expr_so_far = JSONExpression(internal.Var(parts[0][1:]))
    else:
        expr_so_far = JSONExpression(internal.ImplicitAttr(parts[0]))
    for part in parts[1:]:
        expr_so_far = expr_so_far[part]
    return expr_so_far

def js(expr=None, body=None):
    if (expr is not None) + (body is not None) != 1:
        raise ValueError('exactly one of expr or body must be passed')
    if body is not None:
        return JSONExpression(internal.Javascript(body))
    else:
        return JSONExpression(internal.Javascript(u'return (%s);' % expr))

def let(*bindings):
    body = bindings[-1]
    bindings = bindings[:-1]
    if len(bindings) == 0:
        raise ValueError("need at least one binding")
    if isinstance(body, MultiRowSelection):
        t = MultiRowSelection
    elif isinstance(body, StreamExpression):
        t = StreamExpression
    elif isinstance(body, RowSelection):
        t = RowSelection
    else:
        body = expr(body)
        t = JSONExpression
    return t(internal.Let(body, bindings))

def fn(*x):
    """Create a function.
    See :func:`Selectable.filter` for examples.

    The last argument is the body of the function,
    and the other arguments are the parameter names.

    :param args: names of parameters
    :param body: body of function
    :type body: :class:`JSONExpression` or :class:`StreamExpression`
    :rtype: :class:`rethinkdb.JSONFunction` or :class:`rethinkdb.StreamFunction`

    >>> fn(3)                           # lambda: 3
    >>> fn("x", R("$x") + 1)            # lambda x: x + 1
    >>> fn("x", "y", R("$x") + R("$y))  # lambda x, y: x + y
    """
    body = x[-1]
    args = x[:-1]
    if isinstance(body, StreamExpression):
        return StreamFunction(body, *args)
    else:
        return JSONFunction(body, *args)

class JSONFunction(object):
    """TODO document me"""
    def __init__(self, body, *args):
        self.body = expr(body)
        self.args = args

    def __str__(self):
        return self._pretty_print(internal.ReprPrettyPrinter(), [])

    def __repr__(self):
        return "<JSONFunction %s>" % str(self)

    def write_mapping(self, mapping):
        assert len(self.args) <= 1
        if self.args:
            mapping.arg = self.args[0]
        else:
            mapping.arg = 'row'     # TODO: GET RID OF THIS
        self.body._inner._write_ast(mapping.body)

    def write_reduction(self, reduction, base):
        assert len(self.args) == 2
        base._inner._write_ast(reduction.base)
        reduction.var1 = self.args[0]
        reduction.var2 = self.args[1]
        self.body._inner._write_ast(reduction.body)

    def _pretty_print(self, printer, backtrace_steps):
        return "fn(%s)" % (", ".join([repr(x) for x in self.args] + [printer.expr_unwrapped(self.body, backtrace_steps)]))

class StreamFunction(object):
    """TODO document me"""
    def __init__(self, body, *args):
        assert isinstance(body, StreamExpression)
        self.body = body
        self.args = args

    def __str__(self):
        return self._pretty_print(internal.ReprPrettyPrinter(), [])

    def __repr__(self):
        return "<StreamFunction %s>" % str(self)

    def write_mapping(self, mapping):
        if self.args:
            mapping.arg = self.args[0]
        else:
            mapping.arg = 'row'     # TODO: GET RID OF THIS
        self.body._inner._write_ast(mapping.body)

    def _pretty_print(self, printer, backtrace_steps):
        return "fn(%s)" % (", ".join([repr(x) for x in self.args] + [printer.expr_unwrapped(self.body, backtrace_steps)]))

class BaseSelection(object):
    """Something which can be read or written."""
    def delete(self):
        """Delete all rows in the selection from the database."""
        return WriteQuery(internal.Delete(self))

    def update(self, mapping):
        """Update all rows in the selection by merging the current contents
        with the value of `mapping`.

        The merge is recursive, see :

        >>> table('users').filter(R('warnings') > 5).update({'banned': True})

        """
        if not isinstance(mapping, JSONFunction):
            mapping = JSONFunction(mapping)
        return WriteQuery(internal.Update(self, mapping))

    def mutate(self, mapping):
        """TODO: get rid of this ?"""
        if not isinstance(mapping, JSONFunction):
            mapping = JSONFunction(mapping)
        return WriteQuery(internal.Mutate(self, mapping))

class RowSelection(JSONExpression, BaseSelection):
    """A single row from a table which can be read or written."""

    def __repr__(self):
        return "<RowSelection %s>" % str(self)

class MultiRowSelection(StreamExpression, BaseSelection):
    """A sequence of rows which can be read or written."""

    def __repr__(self):
        return "<MultiRowSelection %s>" % str(self)

class WriteQuery(BaseQuery):
    """All queries that modify the database are instances of
    :class:`WriteQuery`."""
    def __init__(self, inner):
        assert isinstance(inner, internal.WriteQueryInner)
        self._inner = inner

    def __str__(self):
        return internal.ReprPrettyPrinter().write_query(self, [])

    def __repr__(self):
        return "<WriteQuery %s>" % str(self)

    def _finalize_query(self, root):
        root.type = p.Query.WRITE
        self._inner._write_write_query(root.write_query)

class MetaQuery(BaseQuery):
    """Queries that create, destroy, or examine databases or tables rather than
    working with actual data are instances of :class:`MetaQuery`."""
    def __init__(self, inner):
        self._inner = inner

    def __str__(self):
        return internal.ReprPrettyPrinter().meta_query(self, [])

    def __repr__(self):
        return "<MetaQuery %s>" % str(self)

    def _finalize_query(self, root):
        root.type = p.Query.META
        self._inner._write_meta_query(root.meta_query)

def db_create(db_name):
    """Create a ReQL expression that creates a database within a
    RethinkDB cluster. A RethinkDB database is an object that contains
    related tables as well as configuration options that apply to
    these tables.

    When run via :func:`rethinkdb.net.Connection.run` or :func:`BaseQuery.run`,
    `run` has no return value in case of success, and raises
    :class:`rethinkdb.net.QueryError` in case of failure.

    :param db_name: The name of the database to be created.
    :type db_name: str
    :returns: :class:`MetaQuery` -- a ReQL expression that encodes the database
     creation operation.

    :Example:

    >>> q = db_create('my_database_name')
    """
    return MetaQuery(internal.DBCreate(db_name))

def db_drop(db_name):
    """Create a ReQL expression that drops a database within a
    RethinkDB cluster.

    When run via :func:`rethinkdb.net.Connection.run` or :func:`BaseQuery.run`,
    `run` has no return value in case of success, and raises
    :class:`rethinkdb.net.QueryError` in case of failure.

    :param db_name: The name of the database to be dropped.
    :type db_name: str
    :returns: :class:`MetaQuery` -- a ReQL expression that encodes the database
        dropping operation.

    :Example:

    >>> q = db_drop('testing')
    """
    return MetaQuery(internal.DBDrop(db_name))

def db_list():
    """Create a ReQL expression that lists all databases within a
    RethinkDB cluster.

    When run via :func:`rethinkdb.net.Connection.run` or
    :func:`BaseQuery.run`, `run` returns a list of database name
    strings in case of success, and raises
    :class:`rethinkdb.net.QueryError` in case of failure.

    :returns: :class:`MetaQuery` -- a ReQL expression that encodes
      the database listing operation.

    :Example:

    >>> db_list().run()
    ['Personnel', 'Grades', 'Financial']
    """
    return MetaQuery(internal.DBList())

class Database(object):
    """A ReQL expression that encodes a RethinkDB database. Most
    database-related operations (including table access) can be
    chained off of this object."""
    def __init__(self, db_name):
        """Use :func:`rethinkdb.query.db` to create this object.

        :param db_name: Name of the databases to access.
        :type db_name: str
        """
        self.db_name = db_name

    def __repr__(self):
        return "<Database %r>" % self.db_name

    def table_create(self, table_name, primary_datacenter, primary_key="id"):
        """Create a ReQL expression that creates a table within this
        RethinkDB database. A RethinkDB table is an object that
        contains JSON documents.

        When run via :func:`rethinkdb.net.Connection.run` or
        :func:`Expression.run`, `run` has no return value in case of
        success, and raises :class:`rethinkdb.net.QueryError` in case
        of failure.

        :param table_name: The name of the table to be created.
        :type table_name: str
        :param primary_datacenter: The name of the datacenter to use as the
            primary datacenter for the new table.
        :type primary_datacenter: str
        :param primary_key: An optional name of the JSON attribute
          that will be used as a primary key for the document. If
          missing, defaults to 'id'.
        :type primary_key: str
        :returns: :class:`MetaQuery` -- a ReQL expression that
          encodes the table creation operation.

        :Example:

        >>> q = db('db_name').create('posts', primary_datacenter = "us-west") # uses primary key 'id'
        >>> q = db('db_name').create('users', primary_datacenter = "us-west", primary_key='user_id')
        """
        return MetaQuery(internal.TableCreate(table_name, self, primary_datacenter, primary_key))

    def table_drop(self, table_name):
        """Create a ReQL expression that drops a table within this
        RethinkDB database.

        When run via :func:`rethinkdb.net.Connection.run` or
        :func:`Expression.run`, `run` has no return value in case of
        success, and raises :class:`rethinkdb.net.QueryError` in case
        of failure.

        :param table_name: The name of the table to be dropped.
        :type table_name: str
        :returns: :class:`MetaQuery` -- a ReQL expression that
          encodes the table creation operation.

        :Example:

        >>> q = db('db_name').drop('posts')
        """
        return MetaQuery(internal.TableDrop(table_name, self))

    def table_list(self):
        """Create a ReQL expression that lists all tables within this
        RethinkDB database.

        When run via :func:`rethinkdb.net.Connection.run` or
        :func:`Expression.run`, `run` returns a list of table name
        strings in case of success, and raises
        :class:`rethinkdb.net.QueryError` in case of failure.

        :returns: :class:`MetaQuery` -- a ReQL expression that
          encodes the table creation operation.

        :Example:

        >>> q = db('db_name').list() # returns a list of tables, e.g. ['table1', 'table2']
        """
        return MetaQuery(internal.TableList(self))

    def table(self, table_name):
        """Create a ReQL expression that encodes a table within this
        RethinkDB database. This function is a shortcut for
        constructing the :class:`Table` object.

        Use :func:`rethinkdb.query.table` as a shortcut for this
        method.

        :returns: :class:`Table` -- a ReQL expression that encodes the
          table expression.
        """
        return Table(table_name, self)

def db(db_name):
    """Create a ReQL expression that encodes a database within a
    RethinkDB cluster. This function is a shortcut for constructing
    the :class:`Database` object.

    :returns: :class:`Database` -- a ReQL expression that encodes the
      database expression.

    :Example:

    >>> q = db('db_name')
    """
    return Database(db_name)

class Table(MultiRowSelection):
    """A ReQL expression that encodes a RethinkDB table. Most data
    manipulation operations (such as inserting, selecting, and
    updating data) can be chained off of this object."""

    def __init__(self, table_name, db_expr=None):
        """Use :func:`rethinkdb.query.table` as a shortcut to create
        this object.

        :param table_name: Name of the databases to access.
        :type table_name: str
        :param db_expr: An optional database where this table
          resides. If missing, use default database specified on the
          connection object.
        :type db_expr: :class:`Database`
        """
        ReadQuery.__init__(self, internal.Table(self))
        self.table_name = table_name
        self.db_expr = db_expr

    def __repr__(self):
        if self.db_expr is not None:
            return "<Table %r>" % (self.db_expr.db_name + "." + self.table_name)
        else:
            return "<Table %r>" % self.table_name

    def insert(self, docs):
        """Insert documents into the table.

        :param docs: the document(s) to insert
        :type docs: dict/list(dict)
        :rtype: :class:`WriteQuery`
        """
        if isinstance(docs, dict):
            return WriteQuery(internal.Insert(self, [docs]))
        else:
            return WriteQuery(internal.Insert(self, docs))

    def insert_stream(self, stream):
        return WriteQuery(internal.InsertStream(self, stream))

    def get(self, key, attr_name = "id"):
        """Select the row whose value for `attr_name` is equal to `key`. If no
        row is found, return `null`.

        Currently, `attr_name` must be the primary key for the table.

        :param key: the key to look for
        :type key: JSON value
        :param attr_name: the field to check against `key`
        :type attr_name: str
        :rtype: :class:`RowSelection`

        >>> q = table('users').get(10)  # get user with primary key 10
        """
        return RowSelection(internal.Get(self, key, attr_name))

    def _write_ref_ast(self, parent):
        parent.db_name = self.db_expr.db_name
        parent.table_name = self.table_name

def table(table_ref):
    """Get a reference to a table within a RethinkDB cluster.

    :param table_ref: Either a name of the table, or a name of the
      database followed by a period followed by a name of the table. If
      the database is omitted, the default database specified on
      the connection is used.
    :type table_ref: str
    :returns: :class:`Table` -- a reference to the specified table

    >>> q = table('table_name')         #
    >>> q = table('db_name.table_name') # equivalent to db('db_name').table('table_name')
    """
    if '.' in table_ref:
        db_name, table_name = table_ref.split('.', 1)
        return db(db_name).table(table_name)
    else:
        return Table(table_ref)

# this happens at the end since it's a circular import
import internal
# Copyright 2018 RebirthDB
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
# Copyright 2010-2016 REBIRTHDB, all rights reserved.

from __future__ import print_function

import collections
import copy
import distutils.version
import getpass
import inspect
import optparse
import os
import re
import sys
import threading

from rebirthdb import ast, errors, net, query, version

default_batch_size = 200


class RetryQuery(object):

    __connectOptions = None
    __local = None

    def __init__(self, connect_options):
        if 'host' not in connect_options:
            raise AssertionError('Hostname is a required connection parameter')

        if 'port' not in connect_options:
            raise AssertionError('Port number is a required connection parameter')

        connect_options['port'] = int(connect_options['port'])

        if connect_options <= 0:
            raise AssertionError('Port number can not be less than one')

        self.__connectOptions = copy.deepcopy(connect_options)

        self.__local = threading.local()

    def conn(self, test_connection=True):
        if not hasattr(self.__local, 'connCache'):
            self.__local.connCache = {}

        # check if existing connection is still good
        if os.getpid() in self.__local.connCache and test_connection:
            try:
                ast.expr(0).run(self.__local.connCache[os.getpid()])
            except errors.ReqlError:
                del self.__local.connCache[os.getpid()]

        # cache a new connection
        if not os.getpid() in self.__local.connCache:
            self.__local.connCache[os.getpid()] = net.connect(**self.__connectOptions)

        # return the connection
        return self.__local.connCache[os.getpid()]

    def __call__(self, name, query_str, times=5, run_options=None, test_connection=True):
        # Try a query multiple times to guard against bad connections
        if name is None:
            raise AssertionError('Name can not be none')

        name = str(name)

        if not isinstance(query_str, ast.RqlQuery):
            raise AssertionError('Query must be a ReQL query instead of {value}'.format(value=query_str))

        if not isinstance(times, int) or times < 1:
            raise ValueError('Times must be a positive integer instead of {value}'.format(value=times))

        if run_options is None:
            run_options = {}

        if not isinstance(run_options, dict):
            raise ValueError('Run option must be a dict instead of {value}'.format(value=run_options))

        last_error = None
        test_connection = False

        for _ in range(times):
            try:
                conn = self.conn(test_connection=test_connection)  # we are already guarding for this
            except errors.ReqlError as e:
                last_error = RuntimeError("Error connecting for during '%s': %s" % (name, str(e)))
                test_connection = True
            try:
                return query_str.run(conn, **run_options)
            except (errors.ReqlTimeoutError, errors.ReqlDriverError) as e:
                last_error = RuntimeError("Connection error during '%s': %s" % (name, str(e)))
            # other errors immediately bubble up

        else:
            raise last_error


def print_progress(ratio, indent=0, read=None, write=None):
    total_width = 40
    done_width = min(int(ratio * total_width), total_width)
    sys.stdout.write("\r%(indent)s[%(done)s%(undone)s] %(percent)3d%%%(readRate)s%(writeRate)s\x1b[K" % {
        "indent": " " * indent,
        "done": "=" * done_width,
        "undone": " " * (total_width - done_width),
        "percent": int(100 * ratio),
        "readRate": (" r: %d" % read) if read is not None else '',
        "writeRate": (" w: %d" % write) if write is not None else ''
    })
    sys.stdout.flush()


def check_minimum_version(options, minimum_version='1.6'):
    minimum_version = distutils.version.LooseVersion(minimum_version)
    version_string = options.retryQuery('get server version', query.db(
        'rebirthdb').table('server_status')[0]['process']['version'])

    matches = re.match(r'rebirthdb (?P<version>(\d+)\.(\d+)\.(\d+)).*', version_string)

    if not matches:
        raise RuntimeError("invalid version string format: %s" % version_string)

    if distutils.version.LooseVersion(matches.group('version')) < minimum_version:
        raise RuntimeError("Incompatible version, expected >= %s got: %s" % (minimum_version, version_string))


DbTable = collections.namedtuple('DbTable', ['db', 'table'])
_tableNameRegex = re.compile(r'^(?P<db>\w+)(\.(?P<table>\w+))?$')


class CommonOptionsParser(optparse.OptionParser, object):

    __retryQuery = None
    __connectRegex = re.compile(r'^\s*(?P<hostname>[\w.-]+)(:(?P<port>\d+))?\s*$')

    def format_epilog(self, formatter):
        return self.epilog or ''

    def __init__(self, *args, **kwargs):

        # -- Type Checkers

        def check_tls_option(opt_str, value):
            value = str(value)

            if os.path.isfile(value):
                return {'ca_certs': os.path.realpath(value)}
            else:
                raise optparse.OptionValueError('Option %s value is not a file: %r' % (opt_str, value))

        def check_db_table_option(value):
            res = _tableNameRegex.match(value)

            if not res:
                raise optparse.OptionValueError('Invalid db or db.table name: %s' % value)
            if res.group('db') == 'rebirthdb':
                raise optparse.OptionValueError('The `rebirthdb` database is special and cannot be used here')

            return DbTable(res.group('db'), res.group('table'))

        def check_positive_int(opt_str, value):
            if not isinstance(value, int) or value < 1:
                raise optparse.OptionValueError('%s value must be an integer greater that 1: %s' % (opt_str, value))

            return int(value)

        def check_existing_file(opt_str, value):
            if not os.path.isfile(value):
                raise optparse.OptionValueError('%s value was not an existing file: %s' % (opt_str, value))

            return os.path.realpath(value)

        def check_new_file_location(opt_str, value):
            try:
                real_value = os.path.realpath(value)
            except Exception:
                raise optparse.OptionValueError('Incorrect value for %s: %s' % (opt_str, value))

            if os.path.exists(real_value):
                raise optparse.OptionValueError('%s value already exists: %s' % (opt_str, value))

            return real_value

        def file_contents(opt_str, value):
            if not os.path.isfile(value):
                raise optparse.OptionValueError('%s value is not an existing file: %r' % (opt_str, value))

            try:
                with open(value, 'r') as passwordFile:
                    return passwordFile.read().rstrip('\n')
            except IOError:
                raise optparse.OptionValueError('bad value for %s: %s' % (opt_str, value))

        # -- Callbacks

        def combined_connect_action(value, parser):
            res = self.__connectRegex.match(value)
            if not res:
                raise optparse.OptionValueError("Invalid 'host:port' format: %s" % value)
            if res.group('hostname'):
                parser.values.hostname = res.group('hostname')
            if res.group('port'):
                parser.values.driver_port = int(res.group('port'))

        # -- setup custom Options object

        class CommonOptionChecker(optparse.Option, object):
            TYPES = optparse.Option.TYPES + ('tls_cert', 'db_table', 'pos_int', 'file', 'new_file', 'file_contents')

            TYPE_CHECKER = copy.copy(optparse.Option.TYPE_CHECKER)
            TYPE_CHECKER['tls_cert'] = check_tls_option
            TYPE_CHECKER['db_table'] = check_db_table_option
            TYPE_CHECKER['pos_int'] = check_positive_int
            TYPE_CHECKER['file'] = check_existing_file
            TYPE_CHECKER['new_file'] = check_new_file_location
            TYPE_CHECKER['file_contents'] = file_contents

            ACTIONS = optparse.Option.ACTIONS + ('add_key', 'get_password')
            STORE_ACTIONS = optparse.Option.STORE_ACTIONS + ('add_key', 'get_password')
            TYPED_ACTIONS = optparse.Option.TYPED_ACTIONS + ('add_key',)
            ALWAYS_TYPED_ACTIONS = optparse.Option.ALWAYS_TYPED_ACTIONS + ('add_key',)

            def take_action(self, action, dest, opt, value, values, parser):
                if dest is None:
                    raise AssertionError('Destination can not be none')

                if action == 'add_key':
                    if self.metavar is None:
                        raise AssertionError('Metavar can not be none')

                    values.ensure_value(dest, {})[self.metavar.lower()] = value
                elif action == 'get_password':
                    values[dest] = getpass.getpass('Password for `admin`: ')
                else:
                    super(CommonOptionChecker, self).take_action(action, dest, opt, value, values, parser)

        kwargs['option_class'] = CommonOptionChecker

        # - default description to the module's __doc__
        if 'description' not in kwargs:
            # get calling module
            caller = inspect.getmodule(inspect.stack()[1][0])
            if caller.__doc__:
                kwargs['description'] = caller.__doc__

        # -- add version

        if 'version' not in kwargs:
            kwargs['version'] = "%%prog %s" % version.version

        # -- call super

        super(CommonOptionsParser, self).__init__(*args, **kwargs)

        # -- add common options

        self.add_option(
            "-q",
            "--quiet",
            dest="quiet",
            default=False,
            action="store_true",
            help='suppress non-error messages')
        self.add_option("--debug", dest="debug", default=False, action="store_true", help=optparse.SUPPRESS_HELP)

        connection_group = optparse.OptionGroup(self, 'Connection options')
        connection_group.add_option(
            '-c',
            '--connect',
            dest='driver_port',
            metavar='HOST:PORT',
            help='host and client port of a rebirthdb node to connect (default: localhost:%d)' %
            net.DEFAULT_PORT,
            action='callback',
            callback=combined_connect_action,
            type='string')
        connection_group.add_option(
            '--driver-port',
            dest='driver_port',
            metavar='PORT',
            help='driver port of a rebirthdb server',
            type='int',
            default=os.environ.get(
                'REBIRTHDB_DRIVER_PORT',
                net.DEFAULT_PORT))
        connection_group.add_option(
            '--host-name',
            dest='hostname',
            metavar='HOST',
            help='host and driver port of a rebirthdb server',
            default=os.environ.get(
                'REBIRTHDB_HOSTNAME',
                'localhost'))
        connection_group.add_option(
            '-u',
            '--user',
            dest='user',
            metavar='USERNAME',
            help='user name to connect as',
            default=os.environ.get(
                'REBIRTHDB_USER',
                'admin'))
        connection_group.add_option(
            '-p',
            '--password',
            dest='password',
            help='interactively prompt for a password for the connection',
            action='get_password')
        connection_group.add_option(
            '--password-file',
            dest='password',
            metavar='PSWD_FILE',
            help='read the connection password from a file',
            type='file_contents')
        connection_group.add_option(
            '--tls-cert',
            dest='ssl',
            metavar='TLS_CERT',
            help='certificate file to use for TLS encryption.',
            type='tls_cert')
        self.add_option_group(connection_group)

    def parse_args(self, *args, **kwargs):
        # - validate options

        connect = True
        if 'connect' in kwargs:
            connect = kwargs['connect']
            del kwargs['connect']

        # - validate ENV variables

        if 'REBIRTHDB_DRIVER_PORT' in os.environ:
            driver_port = os.environ['REBIRTHDB_DRIVER_PORT']

            if not isinstance(driver_port, int) or driver_port < 1:
                self.error('ENV variable REBIRTHDB_DRIVER_PORT is not a useable integer: %s'
                           % os.environ['REBIRTHDB_DRIVER_PORT'])

        # - parse options

        options, args = super(CommonOptionsParser, self).parse_args(*args, **kwargs)

        # - setup a RetryQuery instance

        self.__retryQuery = RetryQuery(connect_options={
            'host': options.hostname,
            'port': options.driver_port,
            'user': options.user,
            'password': options.password,
            'ssl': options.ssl
        })
        options.retryQuery = self.__retryQuery

        # - test connection

        if connect:
            try:
                options.retryQuery.conn()
            except errors.ReqlError as e:
                self.error('Unable to connect to server: %s' % str(e))

        # -

        return options, args


_interrupt_seen = False


def abort(pools, exit_event):
    global _interrupt_seen

    if _interrupt_seen:
        # second time
        print("\nSecond terminate signal seen, aborting ungracefully")
        for pool in pools:
            for worker in pool:
                worker.terminate()
                worker.join(.1)
    else:
        print("\nTerminate signal seen, aborting")
        _interrupt_seen = True
        exit_event.set()

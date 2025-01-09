# mldb_wrapper.py
# Copyright (c) 2015 mldb.ai inc.  All rights reserved.
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
# Plugin loader for Python plugins; injectible python part

# https://bugs.python.org/issue32573 ; affects unittest
import sys
if not hasattr(sys, 'argv'):
    sys.argv  = ['']

import unittest

class mldb_wrapper(object):

    import json as jsonlib # noqa

    class MldbBaseException(Exception):
        pass

    class ResponseException(MldbBaseException):
        def __init__(self, response):
            self.response = response

        def __str__(self):
            return ('Response status code: {r.status_code}. '
                    'Response text: {r.text}'.format(r=self.response))

    class TestSuiteFailureException(MldbBaseException):
        def __init__(self, response):
            self.response = response

        def __str__(self):
            return ('Test failed: {s}'.format(s=str(self.response)))

    class TooManyRedirectsException(Exception):
        pass

    class Response(object):

        def __init__(self, url, raw_response):
            self.url         = url
            self.headers     = {k: v for k, v in
                                raw_response.get('headers', {})}
            self.status_code = raw_response['statusCode']
            self.text        = raw_response.get('response', '')
            self.raw         = raw_response

            self.apparent_encoding = 'unimplemented'
            self.close             = 'unimplemented'
            self.conection         = 'unimplemented'
            self.elapsed           = 'unimplemented'
            self.encoding          = 'unimplemented'
            self.history           = 'unimplemented'
            self.iter_content      = 'unimplemented'
            self.iter_lines        = 'unimplemented'
            self.links             = 'unimplemented'
            self.ok                = 'unimplemented'
            self.raise_for_status  = 'unimplemented'
            self.reason            = 'unimplemented'
            self.request           = 'unimplemented'

        def json(self):
            return mldb_wrapper.jsonlib.loads(self.text)

        def __str__(self):
            return self.text

    class StepsLogger(object):

        def __init__(self, mldb):
            self.done_steps = set()
            self.log = mldb.log

        def log_progress_steps(self, progress_steps):
            from datetime import datetime
            from dateutil.tz import tzutc
            from dateutil.parser import parse as parse_date
            now = datetime.now(tzutc())
            for step in progress_steps:
                if 'ended' in step:
                    if step['name'] in self.done_steps:
                        continue
                    self.done_steps.add(step['name'])
                    ran_in = parse_date(step['ended']) - parse_date(step['started'])
                    self.log("{} completed in {} seconds - {} {}"
                            .format(step['name'], ran_in.total_seconds(),
                                    step['type'], step['value']))
                elif 'started' in step:
                    running_since = now - parse_date(step['started'])
                    self.log("{} running since {} seconds - {} {}"
                            .format(step['name'], running_since.total_seconds(),
                                    step['type'], step['value']))

    class wrap(object):
        def __init__(self, mldb):
            print("wrapping mldb", repr(mldb))
            self._mldb = mldb
            import functools
            self.post = functools.partial(self._post_put, 'POST')
            self.put = functools.partial(self._post_put, 'PUT')
            self.post_async = functools.partial(self._post_put, 'POST',
                                                asynch=True)
            self.put_async = functools.partial(self._post_put, 'PUT',
                                               asynch=True)
            self.create_dataset = self._mldb.create_dataset
            
        def _follow_redirect(self, url, counter):
            # somewhat copy pasted from _perform, but gives a nicer stacktrace
            # on failure
            if counter == 0:
                raise mldb_wrapper.TooManyRedirectsException()

            raw_res = self._mldb.perform('GET', url)
            response = mldb_wrapper.Response(url, raw_res)
            if response.status_code < 200 or response.status_code >= 400:
                raise mldb_wrapper.ResponseException(response)
            if response.status_code >= 300:
                return self._follow_redirect(response.headers['location'],
                                             counter - 1)
            return response

        def _perform(self, method, url, *args, **kwargs):
            print("self", repr(self))
            print("method", repr(method))
            print("url", repr(url))
            print("args", repr(args))
            print("kwargs", repr(kwargs))
            raw_res = self._mldb.perform(method, url, *args, **kwargs)
            response = mldb_wrapper.Response(url, raw_res)
            if response.status_code < 200 or response.status_code >= 400:
                raise mldb_wrapper.ResponseException(response)
            if response.status_code >= 300:
                # 10 is the maximum count of redirects to follow
                return self._follow_redirect(response.headers['location'], 10)
            return response

        def perform(self, *args, **kwargs):
            return self._mldb.perform(*args, **kwargs)
        
        def log(self, *args):
            def fix(thing):
                if type(thing) in [dict, list]:
                    thing = mldb_wrapper.jsonlib.dumps(thing, indent=4,
                                                       ensure_ascii=False)
                if not isinstance(thing, (str)):
                    thing = str(thing)

                return thing

            fixed=[fix(thing) for thing in args]
            self._mldb.log(*fixed)

        @property
        def script(self):
            return self._mldb.script

        @property
        def plugin(self):
            return self._mldb.plugin

        def get_http_bound_address(self):
            return self._mldb.get_http_bound_address()

        def get_python_executable(self):
            return self._mldb.get_python_executable()

        def get(self, url, data=None, **kwargs):
            query_string = []
            for k, v in list(kwargs.items()):
                if type(v) in [list, dict]:
                    v = mldb_wrapper.jsonlib.dumps(v)
                query_string.append([str(k), str(v)])
            return self._perform('GET', url, query_string, data)

        def _post_put(self, verb, url, data=None, asynch=False):
            if asynch:
                return self._perform(verb, url, [], data, [['async', 'true']])
            return self._perform(verb, url, [], data)

        def delete(self, url):
            return self._perform('DELETE', url)

        def delete_async(self, url):
            return self._perform('DELETE', url, [], {}, [['async', 'true']])

        def query(self, query):
            return self._perform('GET', '/v1/query', [], {
                'q' : query,
                'format' : 'table'
            }).json()

        def run_tests(self):
            from io import StringIO
            io_stream = StringIO()
            runner = unittest.TextTestRunner(stream=io_stream, verbosity=2,
                                             buffer=True)
            argv = None
            if self.script and self.script.args:
                assert type(self.script.args) is list
                if self.script.args[0]:
                    argv = ['python'] + self.script.args
                else:
                    # avoid the only one empty arg issue
                    argv = None

            res = unittest.main(exit=False, argv=argv,
                                testRunner=runner).result
            self.log(io_stream.getvalue())

            if res.wasSuccessful():
                return("success")

            raise mldb_wrapper.TestSuiteFailureException(res)
            
        def post_run_and_track_procedure(self, payload, refresh_rate_sec=10):
            import threading

            if 'params' not in payload:
                payload['params'] = {}
            payload['params']['runOnCreation'] = False

            res = self.post('/v1/procedures', payload).json()
            proc_id = res['id']
            event = threading.Event()

            def monitor_progress():
                # wrap everything in a try/except because exceptions are not passed to
                # mldb.log by themselves.
                try:
                    # find run id
                    run_id = None
                    sl = mldb_wrapper.StepsLogger(self)
                    while not event.wait(refresh_rate_sec):
                        if run_id is None:
                            res = self.get('/v1/procedures/{}/runs'.format(proc_id)).json()
                            if res:
                                run_id = res[0]
                            else:
                                continue

                        res = self.get('/v1/procedures/{}/runs/{}'.format(proc_id, run_id)).json()
                        if res['state'] == 'executing':
                            sl.log_progress_steps(res['progress']['steps'])
                        else:
                            break

                except Exception as e:
                    self.log(str(e))
                    import traceback
                    self.log(traceback.format_exc())

            t = threading.Thread(target=monitor_progress)
            t.start()

            try:
                return self.post('/v1/procedures/{}/runs'.format(proc_id), {})
            except mldb_wrapper.ResponseException as e:
                return e.response
            finally:
                event.set()
                t.join()



class MldbUnitTest(unittest.TestCase):
    import json # noqa

    longMessage = True # Appends the user message to the normal message

    class _AssertMldbRaisesContext(object):
        """A context manager used to implement TestCase.assertRaises* methods.
        Inspired from python unittests.
        """

        def __init__(self, test_case, expected_regexp=None, status_code=None):
            self.failureException = test_case.failureException
            self.expected_regexp = expected_regexp
            self.status_code = status_code

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, tb):
            if exc_type is None:
                raise self.failureException(
                    "{0} not raised".format(mldb_wrapper.MldbBaseException))
            if not issubclass(exc_type, mldb_wrapper.MldbBaseException):
                # let unexpected exceptions pass through
                return False
            self.exception = exc_value # store for later retrieval
            if self.expected_regexp:
                import re
                expected_regexp = self.expected_regexp
                if isinstance(expected_regexp, str):
                    expected_regexp = re.compile(expected_regexp)
                if not expected_regexp.search(str(exc_value.response.text)):
                    raise self.failureException('"%s" does not match "%s"' %
                        (expected_regexp.pattern,
                         str(exc_value.response.text)))
            if self.status_code:
                if exc_value.response.status_code != self.status_code:
                    raise self.failureException(
                        "Status codes are not equal: {} != {}".format(
                            exc_value.response.status_code, self.status_code))
            return True

    def _get_base_msg(self, res, expected):
        return '{line}Result: {res}{line}Expected: {expected}'.format(
            line='\n' + '*' * 10 + '\n',
            res=MldbUnitTest.json.dumps(res, indent=4),
            expected=MldbUnitTest.json.dumps(expected, indent=4))

    def assertTableResultEquals(self, res, expected, msg=""):
        msg += self._get_base_msg(res, expected)
        self.assertEqual(len(res), len(expected), msg)
        self.assertNotEqual(len(res), 0, msg)
        res_keys = sorted(res[0])
        expected_keys = sorted(expected[0])
        self.assertEqual(res_keys, expected_keys, msg)

        # we'll make the order of `res` match the order of `expected`
        # this is a map that gives us the index in `res` of a column name
        colname_to_idx = {
            colname: index for index, colname in enumerate(res[0])}
        # this gives us the permutation we need to apply to `res`
        res_perm = [colname_to_idx[colname]
            for i, colname in enumerate(expected[0])]

        ordered_res = [[row[i] for i in res_perm] for row in res[1:]]

        for res_row, expected_row in zip(ordered_res, expected[1:]):
            self.assertEqual(res_row, expected_row)

    def assertFullResultEquals(self, res, expected, msg=""):
        msg += self._get_base_msg(res, expected)
        self.assertEqual(len(res), len(expected), msg)
        for res_row, expected_row in zip(res, expected):
            self.assertEqual(res_row["rowName"], expected_row["rowName"], msg)
            res_columns = sorted(res_row["columns"])
            expected_columns = sorted(expected_row["columns"])
            self.assertEqual(res_columns, expected_columns, msg)

    def assertMldbRaises(self, expected_regexp=None, status_code=None):
        return MldbUnitTest._AssertMldbRaisesContext(self, expected_regexp,
                                                     status_code)


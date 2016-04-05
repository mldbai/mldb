#
# MLDBFB-458_jseval_exception_message.py.py
# Mich, 2016-04-05
# Copyright (c) 2016 Datacratic Inc. All rights reserved.
#

from mldb import mldb, MldbUnitTest, ResponseException

class JsevalExceptionMessageTest(MldbUnitTest):  # noqa
    def test_it(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [])
        ds.commit()

        query = """
            SELECT jseval('
                {}
                return {{"foo" : "bar"}};
                ',
                'cols',
                {{*}}
            ) AS *
            FROM ds
                """

        # the query works
        mldb.log(mldb.query(query.format("")))

        # add an exception, good luck understanding what's going on now...
        try:
            mldb.query(query.format('throw "this query is weird";'))

        except ResponseException as exc:
            mldb.log(exc.response.json())
            assert 'this query is weird' in exc.response.json()['error']
        else:
            assert False, 'should not be here'

if __name__ == '__main__':
    mldb.run_tests()

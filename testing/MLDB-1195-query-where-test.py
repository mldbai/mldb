
#
# MLDB-1195-query-where-test.py
# Mich, 2015-12-15
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

def create_ds(name, rowName):
    mldb.put('/v1/datasets/' + name, {
        'type' : 'sparse.mutable'
    })

    mldb.post('/v1/datasets/' + name + '/rows', {
        'rowName' : rowName,
        'columns' : [
            [name, 1, 0],
        ]
    })

    mldb.post('/v1/datasets/' + name + '/commit', {})

    mldb.post('/v1/procedures', {
        'type' : 'transform',
        'params' : {
            'inputData' : 'select rowName(), "{0}" from {0}'.format(name),
            'outputDataset' : name + 'rows',
            'runOnCreation' : True
        }
    })

def run_query(ds_name):
    query = ('SELECT * FROM {} WHERE colA IS NULL AND colB IS NOT NULL'
             .format(ds_name))
    mldb.get('/v1/query', q=query)

class QueryWhereTest(MldbUnitTest):  # noqa

    def test_query_where(self):
        create_ds('ds1', 'row1')
        create_ds('ds2', 'row2')

        mldb.put('/v1/datasets/merged', {
            'type' : 'merged',
            'params' : {
                'datasets' : [
                    {'id' : 'ds1'},
                    {'id' : 'ds1rows'},
                    {'id' : 'ds2'},
                    {'id' : 'ds2rows'}
                ]
            }
        })

        # The query should work whatever the dataset is
        run_query('ds1')
        run_query('merged')

    def test_where_on_unexisting_col(self):
        mldb.create_dataset({'id' : 'ds3', 'type' : 'sparse.mutable'}).commit()
        mldb.query("SELECT * FROM ds3 WHERE someCol IS NOT NULL")
        mldb.query("SELECT * FROM ds3 WHERE \"someCol\" IS NOT NULL")


if __name__ == '__main__':
    mldb.run_tests()

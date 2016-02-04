#
# MLDBFB-1235-temporal-aggregators.py
# 2016-02-04
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

# add this line to testing.mk:
# $(eval $(call mldb_unit_test,MLDBFB-336-sample_test.py,,manual))


import unittest
import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

class SampleTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        now = datetime.datetime.now()
        yesterday = now + datetime.timedelta(days=-1)
        tomorrow = now + datetime.timedelta(days=1)

     # column values at three different times
        ds = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'dataset'})

        for i in xrange(5):
            ds.record_row('row_' + str(i),
                           [['x', -i, yesterday], ['x', 0, now], ['x', i+1, tomorrow]])
        ds.commit()

    def test_min_returns_last_event(self):
        #all expressions are evaluated at latest time
        self.assertQueryResult(
            mldb.query('select min(x) as min_x from dataset order by rowName()'),
            [
                ["_rowName", "min_x"],
                ["[]",  1 ]
            ]
        )
        
    def test_temporal_min_returns_first_event(self):
        #temporal min on one column
        self.assertQueryResult(
            mldb.query('select temporal_min(x) as t_min_x from dataset order by rowName()'),
            [
                ["_rowName", "t_min_x"],
                ["row_0",  0 ],
                ["row_1",  -1 ],
                ["row_2",  -2 ],
                ["row_3",  -3 ],
                ["row_4",  -4 ]
            ]
        )


mldb.run_tests()






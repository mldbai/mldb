#
# MLDBFB-404-case_into_beh_test.py
# Mich, 2016-03-14
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class TestCaseIntoBeh(MldbUnitTest):  # noqa

    query = ('SELECT conv : CASE (behA AND behC) WHEN 1 THEN 1 ELSE 0 END @ 0'
             'FROM ds')

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'beh.binary.mutable'})
        ds.record_row('user1', [['behA', 1, 0]])
        ds.record_row('user2', [['behB', 1, 0]])
        ds.record_row('user3', [['behA', 1, 0], ['behC', 1, 0]])
        ds.commit()

        mldb.log(mldb.query(cls.query))

    def do_it(self, ds_type):
        mldb.post("/v1/procedures", {
            "type" : "transform",
            "params" : {
                    "inputData" : self.__class__.query,
                    "outputDataset" : {"id" : ds_type, "type" : ds_type},
                    'runOnCreation' : True
                }
        })

    def test_beh_mutable(self):
        self.do_it('beh.mutable')

    def test_sparse_mutable(self):
        self.do_it('sparse.mutable')

    def test_beh_binary_mutable(self):
        ds_type = 'beh.binary.mutable'
        mldb.post("/v1/procedures", {
            "type" : "transform",
            "params" : {
                    "inputData" : 'SELECT conv: 1 @ 0 FROM ds '
                                  'WHERE behA AND behC',
                    "outputDataset" : {"id" : ds_type, "type" : ds_type},
                    'runOnCreation' : True
                }
        })

if __name__ == '__main__':
    mldb.run_tests()

#
# MLDBFB-509_pushed_non_printable_char_cant_query.py
# Mich, 2016-05-02
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class PushNonPrintableCharCantReadTest(MldbUnitTest):  # noqa

    def test_unprintable_row_name(self):
        ds = mldb.create_dataset({'id' : 'ds1', 'type' : 'sparse.mutable'})
        barbarous_name = 'coco' + chr(17)
        ds.record_row(barbarous_name, [['colA', 1, 0]])
        ds.commit()

        # http status 400
        mldb.log(mldb.query("SELECT * FROM ds1"))

    def test_unprintable_col_name(self):
        ds = mldb.create_dataset({'id' : 'ds2', 'type' : 'sparse.mutable'})
        barbarous_name = 'coco' + chr(17)
        ds.record_row('row1', [[barbarous_name, 1, 0]])
        ds.commit()

        # http status 400
        mldb.log(mldb.query("SELECT * FROM ds2"))

    def test_unprintable_cell_value(self):
        ds = mldb.create_dataset({'id' : 'ds3', 'type' : 'sparse.mutable'})
        barbarous_name = 'coco' + chr(17)
        ds.record_row('row1', [['colA', barbarous_name, 0]])
        ds.commit()

        # http status 400
        mldb.log(mldb.query("SELECT * FROM ds3"))

if __name__ == '__main__':
    mldb.run_tests()

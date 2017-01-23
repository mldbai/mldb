#
# MLDB-813-rowname_in_join.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

class RownameInJoinTest(MldbUnitTest):

    @classmethod
    def setUpClass(cls):
        ds1 = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'dataset1'})
        ds2 = mldb.create_dataset({
            'type': 'sparse.mutable',
            'id': 'dataset2'})

        for i in xrange(10):
            ds1.record_row('row_' + str(i), [['x', i, 0]])
        for i in xrange(5):
            ds2.record_row('row_' + str(i),
                        [['ds1_row', 'row_' + str(i), 0], ['y', i, 0]])
        ds1.commit()
        ds2.commit()

    def test_join_row_name_eq_x(self):
        res = mldb.query(
            'SELECT d1.x, d2.y FROM dataset1 AS d1 '
            'JOIN dataset2 AS d2 ON d1.rowName() = d2.ds1_row '
            'ORDER BY rowName()')

        expected = [
            [ "_rowName", "d1.x", "d2.y" ],
            [ "[row_0]-[row_0]", 0, 0 ],
            [ "[row_1]-[row_1]", 1, 1 ],
            [ "[row_2]-[row_2]", 2, 2 ],
            [ "[row_3]-[row_3]", 3, 3 ],
            [ "[row_4]-[row_4]", 4, 4 ]
        ]
        self.assertTableResultEquals(res, expected)

    def test_inner_join_rowname_on_row_name(self):
        res = mldb.query(
            'SELECT dataset2.* FROM dataset2 '
            'INNER JOIN dataset1 ON dataset1.rowName() = dataset2.rowName()'
            'ORDER BY dataset2.rowName()'
        )
        expected = [
            ["_rowName", "dataset2.ds1_row", "dataset2.y"],
            ["[row_0]-[row_0]", "row_0", 0],
            ["[row_1]-[row_1]", "row_1", 1],
            ["[row_2]-[row_2]", "row_2", 2],
            ["[row_3]-[row_3]", "row_3", 3],
            ["[row_4]-[row_4]", "row_4", 4]
        ]
        self.assertTableResultEquals(res, expected)


if __name__ == '__main__':
    mldb.run_tests()

#
# MLDB-2045-sparse-3d-grid.py
# Guy Dumais, 2016-09-16
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2045(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({"id": "data",
                                  "type": "sparse.mutable"})
        for i in range(20):
            for j in range(20):
                for k in range(20):
                    ds.record_row('rowname'+str(i)+'_'+str(j)+'_'+str(k), [['x', i, 0],['y', j, 0],['z', k, 0],['value', i*j*k, 0]])

        ds.commit()

    def test_insert(self):
        mldb.log(mldb.put('/v1/functions/getMyValue', {
            'type' : 'sparsegrid.getEmbedding',
            'params' : {
                'trainingData' : 'SELECT x, y, z, value FROM data',
            }
        }))

        res = mldb.query("SELECT getMyValue({[5,12, 3] as pos, 1 as N}) as *")

        self.assertTableResultEquals(res, [[
                "_rowName",
                "v.0.0.0.0",
                "v.0.0.1.0",
                "v.0.0.2.0",
                "v.0.1.0.0",
                "v.0.1.1.0",
                "v.0.1.2.0",
                "v.0.2.0.0",
                "v.0.2.1.0",
                "v.0.2.2.0",
                "v.1.0.0.0",
                "v.1.0.1.0",
                "v.1.0.2.0",
                "v.1.1.0.0",
                "v.1.1.1.0",
                "v.1.1.2.0",
                "v.1.2.0.0",
                "v.1.2.1.0",
                "v.1.2.2.0",
                "v.2.0.0.0",
                "v.2.0.1.0",
                "v.2.0.2.0",
                "v.2.1.0.0",
                "v.2.1.1.0",
                "v.2.1.2.0",
                "v.2.2.0.0",
                "v.2.2.1.0",
                "v.2.2.2.0"
            ],
            [
                "result",
                88,
                132,
                176,
                96,
                144,
                192,
                104,
                156,
                208,
                110,
                165,
                220,
                120,
                180,
                240,
                130,
                195,
                260,
                132,
                198,
                264,
                144,
                216,
                288,
                156,
                234,
                312
            ]
        ])
        

if __name__ == '__main__':
    mldb.run_tests()

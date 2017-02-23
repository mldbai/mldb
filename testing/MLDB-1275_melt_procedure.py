#
# MLDB-1275_melt_procedure.py
# Francois Maillet, 2016-01-24
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import json

mldb = mldb_wrapper.wrap(mldb)  # noqa

def assert_val(res, row_name, col_name, value):
    for row in res:
        if str(row["rowName"]) != row_name:
            continue

        for col in row["columns"]:
            if col[0] == col_name:
                assert col[1] == value
                return True

        # did not find col
        assert False

    # did not find row
    assert False

class MeltProcedureTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : "my_json_dataset"
        }

        dataset = mldb.create_dataset(dataset_config)

        row1 = {
            "name": "bill",
            "age": 25,
            "friends": [{"name": "mich", "age": 20},
                        {"name": "jean", "age": 18}]
        }
        dataset.record_row("row1" , [["data", json.dumps(row1), 0]])

        row2 = {
            "name": "alexis",
            "age": 22,
            "friends": [{"name": "cross", "age": 20},
                        {"name": "fit", "age": 18},
                        {"name": "foot", "region": "south"}]
        }
        dataset.record_row("row2" , [["data", json.dumps(row2), 0]])

        dataset.commit()

        res = mldb.get("/v1/query",
                       q="SELECT parse_json(data, {arrays: 'encode'}) AS * NAMED rowPath() "
                         "FROM my_json_dataset "
                         "WHERE rowName()='row1'").json()
        assert_val(res, "row1", "age", 25)
        assert_val(res, "row1", "friends.1", "{\"age\":18,\"name\":\"jean\"}")

    def run_it(self, output_type):
        conf = {
            "id": "melter",
            "type": "melt",
            "params": {
                "inputData": """
                                SELECT {name, age} as to_fix,
                                    {friends*} as to_melt
                                FROM (select parse_json(data, {arrays: 'encode'}) as * from my_json_dataset)
                                """,
                "runOnCreation": True
            }
        }
        if output_type:
            _id = 'melted_dataset_with_type'
            _type = 'sparse.mutable'
            conf['params']["outputDataset"] = {
                "id": _id,
                "type": _type
            }
        else:
            _id = 'melted_dataset_wo_type'
            _type = 'tabular'
            conf['params']["outputDataset"] = _id

        mldb.put("/v1/procedures/melter", conf)
        res = mldb.get("/v1/query",
                       q="""SELECT name, age, key, parse_json(value, {{arrays: 'encode'}}) AS friends
                            NAMED rowPath()
                            FROM {}
                            WHERE rowName()='row1.friends.1'""".format(_id)).json()
        self.assertFullResultEquals(res, [{
            'rowName' : 'row1.friends.1',
            'columns' : [
                ['age', 25, '1970-01-01T00:00:00Z'],
                ['friends.age', 18, '1970-01-01T00:00:00Z'],
                ['friends.name', 'jean', '1970-01-01T00:00:00Z'],
                ['key', 'friends.1', '1970-01-01T00:00:00Z'],
                ['name', 'bill', '1970-01-01T00:00:00Z']
            ]
        }])

        res = mldb.get('/v1/datasets/' + _id).json()
        self.assertEqual(res['config']['type'], _type)

    def test_key_value_columns(self):
        # Dataset:
        #  rowName  x.0  x.1
        #  0        1    2
        #  1        2    3
        mldb.put('/v1/datasets/patate', {'type': 'tabular'})
        for i in xrange(2):
            mldb.post('/v1/datasets/patate/rows', {
                'rowName': i,
                'columns': [['x.0', i+1, 0],
                            ['x.1', i+2, 0]]
            })
        mldb.post('/v1/datasets/patate/commit')

        mldb.post('/v1/procedures', {
            'type': 'melt',
            'params': {
                'inputData': """
                    SELECT {x.* AS *} AS to_melt,
                           {implicit_cast(rowName()) AS orig_rowName} AS to_fix
                    FROM patate
                    """,
                'outputDataset': 'poil',
                'keyColumnName': 'k',
                'valueColumnName': 'v'
            }
        })

        melted = mldb.query('select * from poil order by rowName()')

        self.assertTableResultEquals(melted, [
            ['_rowName', 'orig_rowName', 'k', 'v'],
            ['0.0', 0, '0', 1],
            ['0.1', 0, '1', 2],
            ['1.0', 1, '0', 2],
            ['1.1', 1, '1', 3]
        ])

    def test_with_output_type(self):
        self.run_it(True)

    def test_wo_output_type(self):
        self.run_it(False)

if __name__ == '__main__':
    mldb.run_tests()

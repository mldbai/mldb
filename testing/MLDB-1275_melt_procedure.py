#
# MLDB-1275_melt_procedure.py
# Francois Maillet, 2016-01-24
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import datetime
import json

mldb = mldb_wrapper.wrap(mldb)  # noqa

def assert_val(res, row_name, col_name, value):
    for row in res:
        if str(row["rowName"]) != row_name: continue

        for col in row["columns"]:
            if col[0] == col_name:
                assert col[1] == value
                return True

        # did not find col
        assert False

    # did not find row
    assert False

class MeltProcedureTest(MldbUnitTest):  # noqa

    def test_it(self):
        dataset_config = {
            'type'    : 'sparse.mutable',
            'id'      : "my_json_dataset"
        }

        dataset = mldb.create_dataset(dataset_config)
        now = datetime.datetime.now()

        row1 = {
            "name": "bill",
            "age": 25,
            "friends": [{"name": "mich", "age": 20}, {"name": "jean", "age": 18}]
        }
        dataset.record_row("row1" , [["data", json.dumps(row1), now]])

        row2 = {
            "name": "alexis",
            "age": 22,
            "friends": [{"name": "cross", "age": 20},
                        {"name": "fit", "age": 18},
                        {"name": "foot", "region": "south"}]
        }
        dataset.record_row("row2" , [["data", json.dumps(row2), now]])

        dataset.commit()



        res = mldb.get("/v1/query",
                       q="SELECT parse_json(data, {arrays: 'encode'}) AS * NAMED rowPath()"
                         "FROM my_json_dataset").json()
        assert_val(res, "row1", "age", 25)
        assert_val(res, "row1", "friends.1", "{\"age\":18,\"name\":\"jean\"}")


        conf = {
            "id": "melter",
            "type": "melt",
            "params": {
                "inputData": """
                                SELECT {name, age} as to_fix,
                                    {friends*} as to_melt
                                FROM (select parse_json(data, {arrays: 'encode'}) as * from my_json_dataset)
                                """,
                "outputDataset": {"id": "melted_dataset", "type": "sparse.mutable" },
                "runOnCreation": True
            }
        }
        mldb.put("/v1/procedures/melter", conf)
        res = mldb.get("/v1/query",
                       q="""SELECT name, age, key, parse_json(value, {arrays: 'encode'}) AS friends
                            NAMED rowPath()
                            FROM melted_dataset""").json()
        assert_val(res, "row1.friends.1", "name", "bill")
        assert_val(res, "row1.friends.1", "key", "friends.1")
        assert_val(res, "row1.friends.1", "friends.age", 18)

if __name__ == '__main__':
    mldb.run_tests()

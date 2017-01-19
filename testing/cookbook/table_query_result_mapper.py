#
# table_query_result_mapper.py
# Mich, 2016-01-13
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#
# MLDB doesn't guarantee column ordering of SQL queries will match the ordering
# of the result. Here is a column mapper example to work around that.
#
mldb = mldb_wrapper.wrap(mldb)  # noqa

class TableQueryResultMapper(object):

    class MappedRow(object):
        def __init__(self, mapping, row):
            self._mapping = mapping
            self._row = row

        def __getattr__(self, name):
            return self._row[self._mapping[name]]

        def __getitem__(self, name):
            return self._row[self._mapping[name]]

        def __str__(self):
            return str({
                k : self._row[v]
            for k, v in self._mapping.iteritems()})

    def __init__(self, result):
        assert result[0][0] == '_rowName'
        self.result = result[1:]
        self._mapping = {
            name: idx for idx, name in enumerate(result[0])
        }

    def __getitem__(self, idx):
        return self.__class__.MappedRow(self._mapping, self.result[idx])

mldb.log("Creating a demo dataset")
ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
ds.record_row('user1', [['colD', 'd', 0], ['colZ', 'zz', 0], ['colA', 'a', 0]])
ds.commit()

query = "SELECT colZ, colD, colA FROM ds"
res = mldb.query("SELECT colZ, colD, colA FROM ds")
mldb.log(query)
mldb.log(res[0])
mldb.log("As you can see, the following result is not in the same order as "
         "the query.")

mapped_res = TableQueryResultMapper(res)
mldb.log("Example usage of the mapper.")
mldb.log("Dot notation: mapped_res[1]._rowName")
mldb.log(mapped_res[0]._rowName)
mldb.log("Bracket notation: mapped_res[1]['colD']")
mldb.log(mapped_res[0]['colD'])

mldb.script.set_return("success")

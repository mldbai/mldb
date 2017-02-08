#
# bisecting_query_error.py
# Mich, 2016-05-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
# In this coobook, bisect is used to find a problematic row when applying a
# COLUMN EXPR. When it's done, it will print the offset of the problematic row.

mldb = mldb_wrapper.wrap(mldb)  # noqa


class Bisect(object):
    def __init__(self, limit):
        self._left = 0
        self._middle = limit / 2
        self._end = limit

        self.limit = self._middle

    def _update_middle(self):
        self._middle = (self._end - self._left) / 2 + self._left
        self.limit = self._middle - self._left

    def good(self):
        self._left = self._middle
        self._update_middle()
        if self.limit == 0:
            mldb.log(self)
            raise Exception("Over")

    def bad(self):
        if self.limit == 1:
            mldb.log(self)
            raise Exception("Over")

        self._end = self._middle
        self._update_middle()

    def __str__(self):
        return 'offset: {} - limit: {}'.format(self.offset, self.limit)

    @property
    def offset(self):
        return self._left

# 1 - Start your bisect with the size of your dataset
bisect = Bisect(20000000)
while True:

    # 2 - In this example, transform works fine. Here we simply reduce our
    # dataset to sie limit and offset given by bisect.
    query = "SELECT * FROM ds LIMIT {} OFFSET {}".format(bisect.limit,
                                                         bisect.offset)
    mldb.log(query)
    mldb.post('/v1/procedures', {
        'type' : 'transform',
        'params' : {
            'inputData' : query,
            'outputDataset' : {
                'id' : 'subset',
                'type' : 'beh.binary.mutable'
            },
            'runOnCreation' : True
        }
    })
    try:
        # 3 - Here we call the query we're having an issue with.
        mldb.query("""
        SELECT COLUMN EXPR (WHERE regex_match(columnName(),
                                            '[[:digit:]]+:seg:21083'))
        FROM subset""")
    except Exception:
        # 4 - We tell bisect that the query failed.
        bisect.bad()
    else:
        # 5 - We tell bisect that the query worked.
        bisect.good()

#
# MLDB-1617-like-precedence.py
# Mathieu Marquis Bolduc, 2016-30-05
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class LikeTest(MldbUnitTest):

    def test_join_no_on_clause(self):

        res1 = mldb.query("select 'apple' like ('%'+'p'+'%')")
        mldb.log(res1)

        res2 = mldb.query("select 'apple' like '%'+'p'+'%'")
        mldb.log(res2)

        self.assertEqual(res1[1], res2[1]);   

mldb.run_tests()
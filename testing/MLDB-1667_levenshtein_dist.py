#
# MLDB-1667
# 2016-05-19
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1667(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        pass

    def test_distance(self):
        def doTestWords(a, b, score):
            self.assertTableResultEquals(
                mldb.query("select levenshtein_distance('%s', '%s') as dist" % (a, b)),
                [
                    ["_rowName", "dist"],
                    [  "result",  score ]
                ]
            )

        doTestWords('patate', 'potato', 2)
        doTestWords('', '', 0)
        doTestWords('abcdef', 'poiuy', 6)
        doTestWords('', 'poiuy', 5)
        doTestWords('asdf', '', 4)

        doTestWords('asdf', 'asffffffffff', 9)
        doTestWords('brrasdfaseve', 'arras', 8)


        text = ("All Good Things... comprises the 25th and 26th episodes of the "
            "seventh season and the series finale of the syndicated American "
            "science fiction television series Star Trek: The Next Generation"
            ". It is the 177th and 178th episodes of the series overall. The "
            "title is derived from the expression All good things must come "
            "to an end, a phrase used by the character Q during the episode "
            "itself. The finale was written as a valentine to the show''s fans"
            ", and is now generally regarded as one of the series'' best "
            "episodes. Hello")
        text2 = ("All Good Things...  comprises the 25th and 26th episodes of the "
            "seventh season and the series finale of the syndicated American "
            "science fiction television series Star Trek: The Next Generation"
            ". It is the 177th and 17 8th episodes of the series overall. The "
            "title is derived from the expression All good things must come "
            "to an end, a phrbse used by the character Q during the episoder "
            "itself. The finale was written as a valentine to the show''s fans"
            ", and is now generally regarded as one of the series'' best "
            "episodes.")
        mldb.log(len(text))
        doTestWords(text, text2, 10)

        
        

mldb.run_tests()

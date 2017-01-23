#
# encode-multiple-png.py
# Mathieu marquis Bolduc, 2017-01-17
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

import unittest

class myTest(MldbUnitTest):

    def test_sequence(self):
        res = mldb.query("""select tf_encodePngs({[[[1],[2]],[[3],[4]]],
                                             [[[5],[6]],[[7],[8]]]})""")
        self.assertTableResultEquals([
                        [
                            "_rowName",
                            "\"tf_encodePngs({[[[1],[2]],[[3],[4]]],\n                                             [[[5],[6]],[[7],[8]]]})\".[[[1],[2]],[[3],[4]]]",
                            "\"tf_encodePngs({[[[1],[2]],[[3],[4]]],\n                                             [[[5],[6]],[[7],[8]]]})\".[[[5],[6]],[[7],[8]]]"
                        ],
                        [
                            "result",
                            {
                                "blob": [
                                    137,
                                    "PNG",
                                    13,
                                    10,
                                    26,
                                    10,
                                    0,
                                    0,
                                    0,
                                    13,
                                    "IHDR",
                                    0,
                                    0,
                                    0,
                                    2,
                                    0,
                                    0,
                                    0,
                                    2,
                                    8,
                                    0,
                                    0,
                                    0,
                                    0,
                                    87,
                                    221,
                                    82,
                                    248,
                                    0,
                                    0,
                                    0,
                                    14,
                                    "IDAT",
                                    8,
                                    153,
                                    "cdddab",
                                    4,
                                    0,
                                    0,
                                    38,
                                    0,
                                    11,
                                    "y ",
                                    231,
                                    23,
                                    0,
                                    0,
                                    0,
                                    0,
                                    "IEND",
                                    174,
                                    "B`",
                                    130
                                ]
                            },
                            {
                                "blob": [
                                    137,
                                    "PNG",
                                    13,
                                    10,
                                    26,
                                    10,
                                    0,
                                    0,
                                    0,
                                    13,
                                    "IHDR",
                                    0,
                                    0,
                                    0,
                                    2,
                                    0,
                                    0,
                                    0,
                                    2,
                                    8,
                                    0,
                                    0,
                                    0,
                                    0,
                                    87,
                                    221,
                                    82,
                                    248,
                                    0,
                                    0,
                                    0,
                                    14,
                                    "IDAT",
                                    8,
                                    153,
                                    "cdedab",
                                    4,
                                    0,
                                    0,
                                    58,
                                    0,
                                    15,
                                    132,
                                    179,
                                    18,
                                    36,
                                    0,
                                    0,
                                    0,
                                    0,
                                    "IEND",
                                    174,
                                    "B`",
                                    130
                                ]
                            }
                        ]
                    ], res)

mldb.run_tests()

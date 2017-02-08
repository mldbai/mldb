#
# deepteach_test.py
# Francois Maillet, 2016-10-14
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class DeepteachTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        mldb.put('/v1/plugins/deepteach', {
            "type": "python",
            "params": {
                "address": "git://github.com/mldbai/deepteach#ip-rndfrst-prob",
            }
        })


    def test_it(self):
        rez = mldb.post("/v1/plugins/deepteach/routes/similar", 
                {"dataset": "realestate_big",
                "prefix": "https://s3.amazonaws.com/public-mldb-ai/datasets/dataset-builder/images",
                "deploy": "false",
                "numBags": 50,
                "numRndFeats": 0.1,
                "input": """{"a":["5fb21ec8b0c3c5bec22e45cdb5a92cda63f4ea0338cad94fe14cbf0d-cour-arriere-triplex-a-vendre-anjou-quebec-province-1600-5579879"],"b":["094ba4bc7f2271efd2daabdc39aa7b945b51584557e19dc12a264cb2-cuisine-condo-a-vendre-villeray-st-michel-parc-extension-quebec-province-1600-5260505"],"ignore": []}"""})

        mldb.log(rez)


if __name__ == '__main__':
    mldb.run_tests()

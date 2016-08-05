#
# MLDB-1886_explain.py
# Francois Maillet, 2016-08-05
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1886Explain(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        mldb.post("/v1/procedures", {
            "type": "import.text",
            "params": {
                'dataFileUrl' : "https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv",
                "outputDataset": {
                    "id": "titanic_raw",
                }
            }
        })

    def test_dt(self):
        mldb.post("/v1/procedures", {
            "type": "classifier.train",
            "params": {
                "mode": "boolean",
                "trainingData": """
                    select 
                        {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} as features,
                        label
                    from titanic_raw
                """,
                "algorithm": "dt",
                "configurationFile": "mldb/container_files/classifiers.json",
                "modelFileUrl": "file:///tmp/mldb_1886_dt.cls"
            }
        })

        mldb.put("/v1/functions/dt_explainer", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": "file:///tmp/mldb_1886_dt.cls"
            }
        })
        
        mldb.log(mldb.query("""
            select dt_explainer({features: {*}, label: 0}) as * from titanic_raw limit 1
        """))


    def test_naive_bayes(self):
        mldb.post("/v1/procedures", {
            "type": "classifier.train",
            "params": {
                "mode": "boolean",
                "trainingData": """
                    select 
                        {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} as features,
                        label
                    from titanic_raw
                """,
                "algorithm": "naive_bayes",
                "configurationFile": "mldb/container_files/classifiers.json",
                "modelFileUrl": "file:///tmp/mldb_1886_naive_bayes.cls"
            }
        })
        
        mldb.put("/v1/functions/naive_bayes_explainer", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": "file:///tmp/mldb_1886_naive_bayes.cls"
            }
        })
        
        # should throw a nice error saying we need a label
        mldb.log(mldb.query("""
            select naive_bayes_explainer({features: {*}}) as * from titanic_raw limit 1
        """))

        mldb.log(mldb.query("""
            select naive_bayes_explainer({features: {*}, label: 0}) as * from titanic_raw limit 1
        """))

    def test_boosed_stumps(self):
        mldb.post("/v1/procedures", {
            "type": "classifier.train",
            "params": {
                "mode": "boolean",
                "trainingData": """
                    select 
                        {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} as features,
                        label
                    from titanic_raw
                """,
                "algorithm": "bs",
                "configurationFile": "mldb/container_files/classifiers.json",
                "modelFileUrl": "file:///tmp/mldb_1886_bs.cls"
            }
        })
        
        mldb.put("/v1/functions/bs_explainer", {
            "type": "classifier.explain",
            "params": {
                "modelFileUrl": "file:///tmp/mldb_1886_bs.cls"
            }
        })

        # should throw a nice error
        mldb.log(mldb.query("""
            select bs_explainer({features: {*}}) as * from titanic_raw limit 1
        """))

        mldb.log(mldb.query("""
            select bs_explainer({features: {*}, label: 0}) as * from titanic_raw limit 1
        """))



if __name__ == '__main__':
    mldb.run_tests()

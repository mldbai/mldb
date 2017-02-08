# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

import json

csv_conf = {
    "type": "import.text",
    "params": {
        'dataFileUrl' : "https://raw.githubusercontent.com/datacratic/mldb-pytanic-plugin/master/titanic_train.csv",
        "outputDataset": {
            "id": "titanic_raw",
        },
        "runOnCreation" : True,
    }
}
mldb.perform("PUT", "/v1/procedures/csv_proc", [], csv_conf) 

result = mldb.perform("PUT", "/v1/procedures/titanic_train_scorer", [], {
    "type": "classifier.experiment",
    "params": {
        "experimentName": "titanic",
        "keepArtifacts": True,
        "inputData": """
            select 
                {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} as features,
                label
            from titanic_raw
        """,
        "modelFileUrlPattern": "file://tmp/MLDB-1304.cls",
        "algorithm": "bbdt",
        "configuration": {
            "bbdt": {
                "type": "bagging",
                "verbosity": 3,
                "weak_learner":  {
                    "type": "boosting",
                    "verbosity": 3,
                    "weak_learner": {
                        "type": "decision_tree",
                        "max_depth": 3,
                        "verbosity": 0,
                        "update_alg": "gentle",
                        "random_feature_propn": 0.5,
                    },
                    "min_iter": 5,
                    "max_iter": 30,
                },
                "num_bags": 5,
            },
        },
        "outputAccuracyDataset": True,
        "equalizationFactor": 1,
        "runOnCreation": True
    }
})
assert result["statusCode"] < 400, result["response"]


result = mldb.perform("PUT", "/v1/functions/titanic_explainer", [], { 
    "type": "classifier.explain",
    "params": { "modelFileUrl": "file://tmp/MLDB-1304.cls" }
})
mldb.log(result)
assert result["statusCode"] < 400, result["response"]


def query(sql):
    result = mldb.perform("GET", "/v1/query", [["q", sql], ["format", "table"]], {})
    assert result["statusCode"] < 400, result["response"]
    return result["response"]

query("""
select label, sum(
    titanic_explainer({
        label: label, 
        features: {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass}
    })[explanation]
) as *
from titanic_raw group by label
""")

mldb.script.set_return("success")


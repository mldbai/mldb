#
# MLDB-2025-st_contains.py
# Francois Maillet, 2016-10-21
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2025StContains(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        mldb.post("/v1/procedures", {
            "type": "import.json",
            "params": {
                "dataFileUrl": "https://s3.amazonaws.com/public-mldb-ai/datasets/mtl_open_data/quartierssociologiques2014-1qpl.json.gz",
                #"dataFileUrl": "file:///home/mailletf/workspace/mldbpro2/mldb/testing/dataset/pwet.json",
                "outputDataset": "quartiers",
                "arrays": "parse"
            }
        })

        mldb.post("/v1/procedures", {
            "type": "import.text",
            "params": {
                "dataFileUrl": "https://s3.amazonaws.com/public-mldb-ai/datasets/mtl_open_data/interventionscitoyendo.csv.gz",
                "outputDataset": "interventions"
            }
        })


    def test_it(self):
        

        mldb.log(mldb.query("""
            SELECT quartiers.properties.Arrondissement, count(*) FROM (
               select interventions.CATEGORIE, interventions.DATE, quartiers.properties.Arrondissement, interventions.LAT, interventions.LONG,
                        try(ST_Contains_Point({quartiers.geometry as *}, interventions.LAT, interventions.LONG)) as contains
                from quartiers
                outer join interventions -- ON ST_Contains_Point({quartiers.geometry as *}, interventions.LAT, interventions.LONG)
                --where quartiers.properties.Q_socio = 'Pointe Saint-Charles' AND interventions.PDQ = 15
                --limit 1
            ) 
            WHERE contains = 1
            group by quartiers.properties.Arrondissement
        """))

        #mldb.log(mldb.query("select * EXCLUDING(geometry*) from quartiers where properties.Q_socio = 'Pointe Saint-Charles'"))

if __name__ == '__main__':
    mldb.run_tests()

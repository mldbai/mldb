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
                "outputDataset": {
                    "id": "quartiers",
                    "type": "structured.mutable"
                },
                "arrays": "parse" 
            }
        })

        mldb.post("/v1/procedures", {
            "type": "import.text",
            "params": {
                "dataFileUrl": "https://s3.amazonaws.com/public-mldb-ai/datasets/mtl_open_data/interventionscitoyendo.csv.gz",
                "outputDataset": "interventions",
                "limit" : 1000 # this is for the sake of keeping the unit tests short enough. Remove to test performance.
            }
        })

    def test_it(self):

        res = mldb.query("""
            SELECT quartiers.properties.Arrondissement, count(*) FROM (
               select interventions.CATEGORIE, interventions.DATE, quartiers.properties.Arrondissement, interventions.LAT, interventions.LONG,
                        try(ST_Contains_Point({quartiers.geometry as *}, interventions.LAT, interventions.LONG)) as contains
                from quartiers
                outer join interventions
            ) 
            WHERE contains = 1
            group by quartiers.properties.Arrondissement
        """)

        self.assertTableResultEquals(res, [
            [
                "_rowName",
                "count(*)",
                "quartiers.properties.Arrondissement"
            ],
            [
                "\"[\"\"Ahuntsic-Cartierville\"\"]\"",
                58,
                "Ahuntsic-Cartierville"
            ],
            [
                "\"[\"\"Anjou\"\"]\"",
                15,
                "Anjou"
            ],
            [
                "\"[\"\"Cte-des-NeigesNotre-Dame-de-Grce\"\"]\"",
                86,
                "Cte-des-NeigesNotre-Dame-de-Grce"
            ],
            [
                "\"[\"\"L'le-BizardSainte-Genevive / Pierrefonds-Roxboro\"\"]\"",
                18,
                "L'le-BizardSainte-Genevive / Pierrefonds-Roxboro"
            ],
            [
                "\"[\"\"LaSalle\"\"]\"",
                24,
                "LaSalle"
            ],
            [
                "\"[\"\"Lachine\"\"]\"",
                36,
                "Lachine"
            ],
            [
                "\"[\"\"Le Plateau-Mont-Royal\"\"]\"",
                86,
                "Le Plateau-Mont-Royal"
            ],
            [
                "\"[\"\"Le Sud-Ouest\"\"]\"",
                50,
                "Le Sud-Ouest"
            ],
            [
                "\"[\"\"MercierHochelaga-Maisonneuve\"\"]\"",
                139,
                "MercierHochelaga-Maisonneuve"
            ],
            [
                "\"[\"\"Montral-Nord\"\"]\"",
                42,
                "Montral-Nord"
            ],
            [
                "\"[\"\"Outremont\"\"]\"",
                6,
                "Outremont"
            ],
            [
                "\"[\"\"Rivire-des-PrairiesPointe-aux-Trembles\"\"]\"",
                33,
                "Rivire-des-PrairiesPointe-aux-Trembles"
            ],
            [
                "\"[\"\"RosemontLa Petite-Patrie\"\"]\"",
                94,
                "RosemontLa Petite-Patrie"
            ],
            [
                "\"[\"\"Saint-Laurent\"\"]\"",
                39,
                "Saint-Laurent"
            ],
            [
                "\"[\"\"Saint-Lonard\"\"]\"",
                37,
                "Saint-Lonard"
            ],
            [
                "\"[\"\"Verdun\"\"]\"",
                38,
                "Verdun"
            ],
            [
                "\"[\"\"Ville-Marie\"\"]\"",
                100,
                "Ville-Marie"
            ],
            [
                "\"[\"\"VilleraySaint-MichelParc-Extension\"\"]\"",
                77,
                "VilleraySaint-MichelParc-Extension"
            ]
        ])

    def test_not_row(self):

        msg = "argument 1 must be a row representing a GeoJson geometry"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            res = mldb.query("""
                SELECT ST_Contains_Point(geometry, 3, 4) as contains FROM 
                    (SELECT 1 as geometry)               
            """)

    def test_missing_column(self):

        msg = "Cound not find required column"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            res = mldb.query("""
                SELECT ST_Contains_Point({geometry}, 3, 4) as contains FROM 
                    (SELECT 1 as geometry)               
            """)

    def test_unknown_type(self):

        msg = "unknown polygon type"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            res = mldb.query("""
                SELECT ST_Contains_Point({'wierd' as type, geometry}, 3, 4) as contains FROM 
                    (SELECT 1 as geometry)               
            """)

    def test_coordinates_shape(self):

        msg = "Attempt to access non-row as row"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            res = mldb.query("""
                SELECT ST_Contains_Point({'Polygon' as type, [0,1] as coordinates}, 3, 4) as contains
            """)

if __name__ == '__main__':
    mldb.run_tests()

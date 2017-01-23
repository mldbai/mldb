#
# MLDB-665_nearest_neighbours.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

import unittest


class Mldb1415Test(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        dataset = mldb.create_dataset({
            "type": "sparse.mutable",
            "id": "iris_dataset"
        })

        with open("./mldb/testing/dataset/iris.data") as f:
            for i, line in enumerate(f):
                cols = []
                line_split = line.split(',')
                if len(line_split) != 5:
                    continue
                cols.append(["sepal length", float(line_split[0]), 0])
                cols.append(["sepal width", float(line_split[1]), 0])
                cols.append(["petal length", float(line_split[2]), 0])
                cols.append(["petal width", float(line_split[3]), 0])
                cols.append(["class", line_split[4], 0])
                dataset.record_row(str(i+1), cols)

        dataset.commit()

        metric = "cosine"
        result = mldb.put("/v1/procedures/kmeans", {
            'type' : 'kmeans.train',
            'params' : {
                'trainingData' : 'select * excluding(class) from iris_dataset',
                'outputDataset' : {'id' : 'iris_kmeans_dataset', 'type' : 'embedding',
                            'params': { 'metric': metric }},
                'centroidsDataset' : {'id' : 'iris_kmeans_centroids', 'type' : 'embedding',
                    'params': {'metric': metric }},
                'numClusters' : 3,
                'modelFileUrl': "file://tmp/mldb-665_kmean.bin",
                'functionName': "kmeans",
                'metric': metric
            }
        })

        result = mldb.post("/v1/procedures/kmeans/runs", {})

        # create a nearest neighbour function
        sql_func_res = mldb.put("/v1/functions/nn", {
            "type": 'embedding.neighbors',
            "params": {
                'dataset': 'iris_kmeans_centroids'
            }
        })


    def test_centroids_get_back(self):
        # pass centroids in kmeans function ane make sure they each get themselves back 
        rez = mldb.query("select kmeans({embedding: {*}})[cluster] as cluster from iris_kmeans_centroids");
        mldb.log(rez)
        assert len(rez) == 4
        for line in rez[1:]:
            assert int(line[0]) == line[1]


    def test_same_cluster_for_any_order_of_values(self):
        # make sure we get the same cluster back depending if we pass the
        # coords in any order
        rez = mldb.query("""
            select kmeans({embedding: {
                                        "sepal length": 0.234932,
                                        "sepal width": 0.038858,
                                        "petal length": 0.801802,
                                        "petal width": 0.546086
                        }})""")
        cluster_with_row = rez[1][1]

        rez = mldb.query("""
            select kmeans({embedding: {
                                        "petal length": 0.801802,
                                        "sepal width": 0.038858,
                                        "petal width": 0.546086,
                                        "sepal length": 0.234932
                        }})""")
        cluster_with_row2 = rez[1][1]

        assert cluster_with_row == cluster_with_row2


    def test_self_centroid_distance_is_zero(self):
        # Compare distance of centroids with other centroids
        rez = mldb.query("""
            select nn({coords: {* excluding (class)}}) as * from iris_kmeans_centroids
        """)
        mldb.log(rez)
        for i in xrange(0):
            assert rez[i+1][i] == 0


    def test_one_neighbour_of_centroid_get_only_himself_back_with_two_nulls(self):
        # make sure if we only ask for 1 neighhbor, we'll get the centroid back and the other
        # cols will be null
        rez = mldb.query("""
            select nn({coords: {* excluding (class)}, numNeighbors:1})[distances] as distances from iris_kmeans_centroids
        """)
        mldb.log("--- only himself")
        mldb.log(rez)
        for i in xrange(2):
            not_null_idx = rez[i+1].index(0)

            # make sure the right column is not null
            assert rez[0][not_null_idx] == "distances.%s" % rez[i+1][0]

            # make sure the others are
            assert any([x for idx, x in enumerate(rez[i+1]) if idx > 0 and not_null_idx != 1]) == False


    def test_nearest_neigbour_in_centroid_space_to_points_is_assigned_cluster(self):
        # make sure that if we take each point and get it's nearest neighbour in the
        # centroids embedding, we get its assigned cluster back
        mldb.log("------------ query");
        rez = mldb.query("""
            select *
            from iris_kmeans_dataset
            join (
                select nn({coords: {* excluding(class)}, numNeighbors:1}) as *
                from iris_dataset
            ) as tbl ON tbl.rowName() = iris_kmeans_dataset.rowName()
        """)
        mldb.log("------------ results");
        mldb.log(rez)
        neighbor_index = rez[0].index("tbl.neighbors.0")
        cluster_index = rez[0].index("iris_kmeans_dataset.cluster")
        for line in rez[1:]:
            not_null_idx = line.index(max(line[2:]))

            # make sure the nearst neighbour is the assigned cluster
            assert line[neighbor_index] == "%d" % line[cluster_index]
            

mldb.run_tests()


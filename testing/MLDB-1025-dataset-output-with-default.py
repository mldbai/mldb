#
# MLDB-1025-dataset-output-with-default.py
# Guy Dumais, 4 November 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_index = 1

def run_transform(output):
    global dataset_index
    dataset_index += 1
    result = mldb.put("/v1/procedures/transform_procedure", {
        "type": "transform",
        "params": {
            "inputData": {
                "from" : {"id": "dataset1"},
                "where": "rowName() = '2'"
            },
            "outputDataset": output,
            "runOnCreation" : True
        }
    })

    mldb.log(result)

    id = output['id'] if 'id' in output else output

    result = mldb.get('/v1/query', q="SELECT * FROM " + id)
    mldb.log(result)
    rows = result.json()
    mldb.log(rows)
    return rows


def load_test_dataset():
    ds1 = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'dataset1'})

    row_count = 10
    for i in xrange(row_count - 1):
        # row name is x's value
        ds1.record_row(str(i), [['x', i, 0], ['y', i, 0]])

    ds1.commit()


def train_svd_with_default():
    svd_procedure = "/v1/procedures/svd"
    # svd procedure configuration
    svd_config = {
        'type' : 'svd.train',
        'params' : {
            "trainingData": "select * from dataset1",

            # first way to specify output dataset using default
            "rowOutputDataset": "svd_row",

            # second way to specify an output dataset using default
            "columnOutputDataset" : {
                "id": "svd_column"
            }
        }
    }

    result = mldb.put(svd_procedure, svd_config)
    mldb.log(result)

    result = mldb.post(svd_procedure + '/runs')
    mldb.log(result)

    result = mldb.get('/v1/datasets/svd_column')
    assert result.json()['type'] == 'embedding', \
        'expected an embedding output dataset'

    result = mldb.get('/v1/datasets/svd_row')
    assert result.json()['type'] == 'embedding', \
        'expected an embedding output dataset'


def train_kmeans_with_default(config):
    kmeans_procedure = "/v1/procedures/kmeans"
    # kmeans procedure configuration

    result = mldb.put(kmeans_procedure, config)
    mldb.log(result.json())

    result = mldb.post(kmeans_procedure + '/runs')
    mldb.log(result.json())

    centroids_id = config['params']['centroidsDataset']['id']
    result = mldb.get('/v1/datasets/' + centroids_id)
    response = result.json()
    mldb.log(response)
    return response['type']


class Mldb1025DatasetOutputWithDefault(MldbUnitTest):

    def test_id(self):

        load_test_dataset()

        # check that the transformed dataset is as expected
        self.assertEqual(
            len(run_transform({"id": "dataset2", "type": "sparse.mutable"})),
            1, 'expected only one row to be returned')

        # check that default type works
        self.assertEqual(len(run_transform({"id": "dataset3"})), 1,
                         'expected only one row to be returned')

        # check that string are interpreted as dataset id
        self.assertEqual(len(run_transform("dataset4")), 1,
                         'expected only one row to be returned')

        # check that the transformed dataset can be overwritten
        self.assertEqual(
            len(run_transform({"id": "dataset2", "type": "sparse.mutable"})),
            1, 'expected only one row to be returned')

        train_svd_with_default()

        metric = 'euclidean'
        kmeans_config = {
            'type' : 'kmeans.train',
            'params' : {
                'trainingData' : 'select * from dataset1',
                'centroidsDataset' : {'id' : 'kmeans_centroids',
                                    'params': {'metric': metric}},
                'numClusters' : 2,
                'metric': metric,
                'runOnCreation' : 0
            }
        }

        # check that the default type is used
        result = mldb.get('/v1/datasets')
        dataset_count_before = len(result.json())
        self.assertEqual(train_kmeans_with_default(kmeans_config), 'embedding',
                         'expected an embedding output dataset')
        result = mldb.get('/v1/datasets')
        dataset_count_after = len(result.json())
        self.assertEqual(dataset_count_before + 1, dataset_count_after,
                         'only the centroids must have been created')

        kmeans_config = {
            'type' : 'kmeans.train',
            'params' : {
                'trainingData' : 'select * from dataset1',
                'centroidsDataset' : {'id' : 'kmeans_centroids_2',
                                    'type' : 'sparse.mutable'},
                'outputDataset': { 'type' : 'embedding'},
                'numClusters' : 2,
                'metric': metric,
                'runOnCreation' : 0
            }
        }

        # check that the type can be changed and that id are auto-generated when not
        # specified
        result = mldb.get('/v1/datasets')
        dataset_count_before = len(result.json())
        self.assertEqual(train_kmeans_with_default(kmeans_config),
                         'sparse.mutable',
                         'expected an sparse.mutable output dataset')
        result = mldb.get('/v1/datasets')
        dataset_count_after = len(result.json())
        self.assertEqual(
            dataset_count_before + 2, dataset_count_after,
            'expect the centroids and the outputDataset to be created')

        mldb.script.set_return('success')

if __name__ == '__main__':
    mldb.run_tests()

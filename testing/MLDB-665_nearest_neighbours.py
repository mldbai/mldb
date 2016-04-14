#
# MLDB-665_nearest_neighbours.py
# datacratic, 2015
# this file is part of mldb. copyright 2015 datacratic. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

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
        'metric': metric
    }
})

result = mldb.post("/v1/procedures/kmeans/runs", {})

result = mldb.get("/v1/datasets/iris_dataset/query",
                  select="* excluding (class)", orderBy = "rowHash()", format="table",
                  rowNames="true")
iris_dataset = result.json()

nearest_centroid = []
headers = iris_dataset[0][1:]
rows = iris_dataset[1:]
for row in rows:
    kwargs = {str(h) : float(v) for (h,v) in zip(headers,row[1:])}
    result = mldb.get("/v1/datasets/iris_kmeans_centroids/routes/neighbours",
                      **kwargs)
    nearest_centroid.append([row[0], result.json()[0][0]])

#mldb.log(nearest_centroid)

result = mldb.get("/v1/datasets/iris_kmeans_dataset/query",
                  select="* excluding (class)", orderBy = "rowHash()", format="table",
                  rowNames="true", headers="false")
kmeans_clusters = result.json()

#mldb.log('kmeans clusters')
#mldb.log(kmeans_clusters)

for n, c in zip(nearest_centroid, kmeans_clusters):
    # if you set the 'metric' variable above to 'euclidean' this works!
    assert n == c, [nearest_centroid, "-------------------", kmeans_clusters]

mldb.script.set_return("success")

import json
from urllib import urlopen


dataset = mldb.create_dataset({ "type": "sparse.mutable", "id": "iris_dataset" })

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
result = mldb.perform("PUT", "/v1/procedures/kmeans", [], {
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
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("POST", "/v1/procedures/kmeans/runs", [], {})
assert result["statusCode"] < 400, result["response"]

result = mldb.perform("GET", "/v1/datasets/iris_dataset/query", [
    ["select", "* excluding (class)" ], ["format", "table"], 
    ["rowNames", "true"]
], {})
assert result["statusCode"] < 400, result["response"]

iris_dataset = json.loads(result["response"])

nearest_centroid = []
headers = iris_dataset[0][1:]
rows = iris_dataset[1:]
for row in rows:
    result = mldb.perform("GET", "/v1/datasets/iris_kmeans_centroids/routes/neighbours", 
         [[str(h), float(v)] for (h,v) in zip(headers,row[1:])], {})
    assert result["statusCode"] < 400, result["response"]
    nearest_centroid.append([row[0], json.loads(result["response"])[0][0]])

#mldb.log(nearest_centroid)

result = mldb.perform("GET", "/v1/datasets/iris_kmeans_dataset/query", [
    ["select", "* excluding (class)" ], ["format", "table"], 
    ["rowNames", "true"], ["headers", "false"]
], {})
assert result["statusCode"] < 400, result["response"]
kmeans_clusters = json.loads(result["response"])

#mldb.log('kmeans clusters')
#mldb.log(kmeans_clusters)

for n, c in zip(nearest_centroid, kmeans_clusters):
    assert n == c, [nearest_centroid, "-------------------", kmeans_clusters] #if you set the 'metric' variable above to 'euclidean' this works!

mldb.script.set_return("success")


# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


# this script should create an assertion error when training the t-SNE

conf = {
    "type":"beh.binary",
    "params": {
        "dataFileUrl": "file:///home/mailletf/first_party.beh.lz4"
    }
}
rez = mldb.perform("PUT", "/v1/datasets/first_party", [], conf)
mldb.log(rez)
assert rez["statusCode"] == 201

# Produce a cut-down version of the dataset that is suitable for running t-SNE.
# We can only handle 100,000 points or so with t-SNE, and even a dataset that
# big will be hard to plot or read.  Here, we take 1/100 of the dataset based
# on the part of the hash not used by the tranching code.
conf = {
    "type":"transform",
    "params": {
        "inputDataset": { "id": "first_party" },
        "outputDataset": { "id": "first_party_reduced", "type": "beh.binary.mutable" },
        "where": "(rowHash() / 32) % 500 = 0"
    }
}
rez = mldb.perform("PUT", "/v1/procedures/reduce", [], conf)
mldb.log(rez)
rez = mldb.perform("POST", "/v1/procedures/reduce/runs")
mldb.log(rez)
assert rez["statusCode"] == 201



conf = {
    "type" : "svd.train",
    "params" : {
        "trainingDataset" : {"id" : "first_party_reduced"},
        "rowOutputDataset" : {"id" : "first_party_svd_embedded", "type" : "embedding"}
    }
}
rez = mldb.perform("PUT", "/v1/procedures/svd_first", [], conf)
mldb.log(rez)
rez = mldb.perform("POST", "/v1/procedures/svd_first/runs")
mldb.log(rez)
assert rez["statusCode"] == 201



conf = {
    "type" : "kmeans.train",
    "params" : {
        "trainingDataset" : {"id" : "first_party_svd_embedded"},
        "outputDataset" : {"id" : "first_party_kmeans_clusters", "type" : "embedding" },
        "numClusters" : 5
    }
}
rez = mldb.perform("PUT", "/v1/procedures/first_party_kmeans", [], conf)
mldb.log(rez)
rez = mldb.perform("POST", "/v1/procedures/first_party_kmeans/runs")
mldb.log(rez)
assert rez["statusCode"] == 201


conf = {
    "type" : "tsne.train",
    "params" : {
        "trainingDataset" : {"id" : "first_party_svd_embedded"},
        "rowOutputDataset" : {"id" : "first_party_tsne_subembedded", "type" : "embedding" }
    }
}
rez = mldb.perform("PUT", "/v1/procedures/first_party_tsne", [], conf)
mldb.log(rez)
rez = mldb.perform("POST", "/v1/procedures/first_party_tsne/runs")
mldb.log(rez)
assert rez["statusCode"] == 201

mldb.script.set_return("success")



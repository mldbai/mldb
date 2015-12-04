import gzip, datetime

#####
# import receipe names
now = datetime.datetime.now()

mldb.perform("DELETE", "/v1/datasets/receipe_names",  [], {})

datasetConfig = {
        "type": "beh.mutable",
        "id": "receipe_names",
        "address": "receipe_names.beh.gz"
    }

dataset = mldb.create_dataset(datasetConfig)

last_id = -1
receipes = []
for i, line in enumerate(gzip.open("/home/mailletf/workspace/platform/mldb_data/receipes/recipes.csv.gz")):
    line = line.strip()
    sep_idx = line.find(",")
    receipe_id = line[:sep_idx]
    receipe_name = line[sep_idx+1:].strip()
    good_id = "user-%d" % (i+1)
    dataset.record_row(good_id, [["name", receipe_name, now]])

dataset.commit()



#####
# import receipes
mldb.perform("DELETE", "/v1/datasets/receipes",  [], {})

datasetConfig = {
        "type": "beh.mutable",
        "id": "receipes",
        "address": "receipes.beh.gz"
    }

dataset = mldb.create_dataset(datasetConfig)

last_id = -1
receipes = []
for i, line in enumerate(gzip.open("/home/mailletf/workspace/platform/mldb_data/receipes/favorites.csv.gz")):
    line_id, receipe = line.strip().split(",")
    if line_id != last_id:
        if len(receipes) > 0:
            good_id = "user-%s" % last_id
            dataset.record_row(good_id, receipes)
            receipes = []
        last_id = line_id
    
    receipes.append([receipe, "yes", now])
#    if i>500:
#        break

dataset.commit()


#####
# run svd
print mldb.perform("DELETE", "/v1/procedures/receipes_svd", [], {})
print mldb.perform("DELETE", "/v1/procedures/receipes_svd_embedding", [], {})
print mldb.perform("DELETE", "/v1/datasets/receipes_svd_embedding", [], {})
print mldb.perform("DELETE", "/v1/datasets/receipes_svd", [], {})

svdConfig = {
    "type": "svd.train",
    "params": {
        "trainingDataset": { "id": "receipes" },
        "columnOutputDataset" : {"id" : "receipes_svd",
                    "type" : "beh.mutable",
                    "address" : "receipes_svd.beh.gz"},
        "rowOutputDataset" : {"id": "receipes_svd_embedding",
                       "type": "embedding",
                       "address" : "receipes_svd.embedding.gz" },
        "select" : "* EXCLUDING (label)"
    }
}

print mldb.perform("PUT", "/v1/procedures/receipes_svd", [], svdConfig)
print mldb.perform("PUT", "/v1/procedures/receipes_svd/runs/1", [], {})

######
# kmeans
kmeanConfig = {
    "type": "kmeans.train",
    "params": {
        "trainingData": "select * from receipes_svd",
        "outputDataset": {"id" : "receipes_svd_kmeans",
                   "type" : "beh.mutable",
                   "address" : "receipes_svd_kmeans.beh.gz"},
        "centroidsDataset": {"id" : "receipes_svd_kmeans_centrois",
                   "type" : "beh.mutable",
                   "address" : "receipes_svd_kmeans_centrois.beh.gz"},
        "numClusters": 20
    }
}

mldb.perform("DELETE", "/v1/procedures/receipes_kmeans", [], {})
mldb.perform("DELETE", "/v1/datasets/receipes_svd_kmeans_centrois", [], {})
mldb.perform("DELETE", "/v1/datasets/receipes_svd_kmeans", [], {})

print mldb.perform("PUT", "/v1/procedures/receipes_kmeans", [], kmeanConfig)
print mldb.perform("PUT", "/v1/procedures/receipes_kmeans/runs/1", [], {})

#######
# tsne
tsneConfig = {
    "type": "tsne.train",
    "params": {
        "trainingDataset": {"id":"receipes_svd"},
        "rowOutputDataset": {"id" : "receipes_svd_tsne",
                   "type" : "beh.mutable",
                   "address" : "receipes_svd_tsne.beh.gz"},
        "numOutputDimensions": 2,
        "perplexity": 6,
        "select": "svd*",
        "where": "true"
    }
}

mldb.perform("DELETE", "/v1/procedures/receipes_svd_tsne", [], {})
print "pwet"
print mldb.perform("PUT", "/v1/procedures/receipes_svd_tsne", [], tsneConfig)
print "prout"
print mldb.perform("PUT", "/v1/procedures/receipes_svd_tsne/runs/1", [], {})
print "pawow"

mldb.perform("DELETE", "/v1/procedures/receipes_svd_tsne", [], {})
print "pwet"
print mldb.perform("PUT", "/v1/procedures/receipes_svd_tsne", [], tsneConfig)
print "prout"
print mldb.perform("PUT", "/v1/procedures/receipes_svd_tsne/runs/1", [], {})
print "pawow"




data_base_url = 'https://raw.githubusercontent.com/jmcohen/taste/master/data/'

def get_favorites():
    mldb.log('creating dataset')
    mldb.perform('PUT', '/v1/datasets/rcp_raw', [], {
        'type': 'text.csv.tabular',
        'params': {
            'limit': 1000,
            'headers': ['user_id', 'recipe_id'],
            'dataFileUrl': data_base_url + 'favorites.csv'}})

    mldb.log('pivoting')
    mldb.perform('PUT', '/v1/procedures/rcp_import', [], {
        "type": "transform",
        "params": {
            "inputData": "select pivot(recipe_id, 1) as * named user_id from rcp_raw group by user_id",
            "outputDataset": "recipes",
            "runOnCreation": True
        }
    })

def do_svd():
    mldb.log('svd')
    mldb.perform('PUT', '/v1/procedures/rcp_do_svd', [], {
        "type" : "svd.train",
        "params" : {
            "trainingData": "select * from recipes",
            "columnOutputDataset" : "rcp_svd_embedding_raw",
            "runOnCreation": True
        }
    })

    mldb.log('clean_svd')
    mldb.perform('PUT', '/v1/procedures/rcp_clean_svd', [], {
        'type': 'transform',
        'params': {
            'inputData': '''select * named jseval(
                                'return s.substr(0, s.indexOf("|"))',
                                's', rowName()) from rcp_svd_embedding_raw''',
            'outputDataset': 'rcp_svd_embedding',
            'runOnCreation': True}})

def do_kmeans():
    mldb.log('kmeans')
    mldb.perform('PUT', '/v1/procedures/rcp_kmeans', [], {
        "type" : "kmeans.train",
        "params" : {
            "trainingData" : "select * from rcp_svd_embedding",
            "outputDataset" : "rcp_kmeans_clusters",
            "centroidsDataset" : "rcp_kmeans_centroids",
            "numClusters" : 20,
            "runOnCreation": True
        }
    })


def get_recipes():
    mldb.log('creating name dataset')
    mldb.perform('PUT', '/v1/datasets/rcp_names_raw', [], {
        'type': 'text.line',
        'params': {
            'dataFileUrl': data_base_url + 'recipes.csv'}})

    mldb.log('transforming name dataset')
    mldb.perform('PUT', '/v1/procedures/rcp_names_import', [], {
        'type': 'transform',
        'params': {
            'inputData': '''select
                jseval('return s.substr(s.indexOf(",")+1).toLowerCase();',
                    's', regex_replace(lineText, '&#34;', '"')) as name
                named implicit_cast(rowName()) - 1
                from rcp_names_raw''',
            'outputDataset': 'rcp_names',
            'runOnCreation': True}})

def do_tfidf():
    mldb.perform('PUT', '/v1/functions/stem', [], {
        'type': 'stemmer',
        'params': {
            'language': 'english'}})

    q = '''
    SELECT sum(stem({words: {tokenize(name, {splitChars:' '}) as *}})[words]) as *
    NAMED cluster
    FROM merge(rcp_names, rcp_kmeans_clusters)
    GROUP BY cluster
    '''
    mldb.log('this query wont run!')
    mldb.perform('PUT', '/v1/procedures/make_word_cluster', [], {
        'type': 'transform',
        'params': {
            'inputData': q,
            'outputDataset': 'rcp_cluster_word',
            'runOnCreation': True}})


get_favorites()
get_recipes()
do_svd()
do_kmeans()
do_tfidf()

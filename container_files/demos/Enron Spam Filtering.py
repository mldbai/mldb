
# coding: utf-8

# # Spam Filtering Using [Euron's Dataset][1]
# [1]: [http://www.aueb.gr/users/ion/data/enron-spam/]

# In[1]:

from pymldb import Connection
mldb = Connection('http://localhost:8087')


# First let's load the 1st of Enron's datasets (there are 6) into MDLB, using a separate script.

# In[2]:

NB_TRAIN = 2500
NB_TEST = 2500

def import_data():
    mldb.put('/v1/datasets/enron_data', {'type': 'sparse.mutable'})
    # get_ipython().magic(u'run -n load_enron.py')
    from load_enron import add_enron_file_to_dataset
    add_enron_file_to_dataset(mldb, '/v1/datasets/enron_data', 1, max_msg=NB_TRAIN + NB_TEST)
    mldb.post('/v1/datasets/enron_data/commit')


# MEGA HACK to prevent de |1 in the svd
def add_0_column(dataset_name):
    # BROKEN
    mldb.put("/v1/datasets/" + dataset_name + '_t', {
        "type": "transposed",
        "params": {
            "dataset": {'id': dataset_name}
        }
    })

    cols = mldb.get('/v1/query', q='select rowName() as row_name from '
                       + dataset_name + '_t', rowNames=0, format='soa') \
        .json()['row_name']
    for col in cols:

        mldb.post('/v1/datasets/' + dataset_name + '/rows', {
            'rowName': 'hack_email',
            'columns': [[col, 0, 0]]})
    mldb.post('/v1/datasets/' + dataset_name + '/commit')


def import_w2v():
    """ import data like google's word2vec """
    # FIXME write somewhere how to download the data from google, it's at
    # https://drive.google.com/file/d/0B7XkCwpI5KDYNlNUTTlSS21pQmM/edit?usp=sharing
    mldb.put("/v1/procedures/word2vec_importer", {
        "type": 'import.word2vec',
        "params": {
            "dataFileUrl":
                'file:///mldb_data/GoogleNews-vectors-negative300.bin.gz',
            "outputDataset": 'w2v',
            "runOnCreation": True
        }
    })
# This is what the dataset looks like.
#
# *index*: order in which the emails arrived in the user's inbox
# *msg*: actual content of the email
# *label*: was the email legitimate (*ham*) or not (*spam*)

# In[3]:

# mldb.query('select index, msg, label from enron_data order by index limit 10')


# Let's create a *sql.expression* that will simply tokenize the emails into a bag of words. Those will be our features on which we will train a classifier.

# In[4]:

def fg_bow():
    print mldb.put('/v1/functions/bow', {
        'type': 'sql.expression',
        'params': {
            'expression': """
                tokenize(msg, {splitchars: ' \n', quotechar: ''}) as bow
                """
        }
    })

    print mldb.put('/v1/functions/stemmerdoc', {
        "type": "stemmerdoc",
        "params": {
            "language": "english"
        }
    })

    # FIXME min_token_length 2
    # FIXME ngram_range:[1,3]
    print mldb.put('/v1/functions/bow2', {
        'type': 'sql.expression',
        'params': {
            'expression': """
                tokenize(stemmerdoc({document:msg})[document], {
                    splitchars: ' \n', quotechar: '', ngram_range:[1,2],
                    min_token_length:3}) as bow2
                """
        }
    })

    if 0:
        for which in ['bow', 'bow2']:
            print mldb.put('/v1/procedures/generate_{}'.format(which), {
                'type': 'transform',
                'params': {
                    'inputData': """
                        select {0}({{msg:msg}})[{0}] as *
                        from enron_data
                        """.format(which),
                    'outputDataset': 'enron_{}'.format(which),
                    'runOnCreation': True
                }
            })


def fg_svd():
    # TODO use the names of the features with a  features.bow2.* as * ou qqch
    # quand ce sera supporte
    print mldb.put('/v1/procedures/train_svd', {
        'type': 'svd.train',
        'params': {
            'trainingData': """
                select column expr (
                    order by rowCount() desc
                    limit 1000
                )
                from enron_bow2
                limit {}
            """.format(NB_TRAIN),
            'numSingularValues': 50,
            'numDenseBasisVectors': 2000,
            'modelFileUrl': 'file://enron_bow2.svd',
            'runOnCreation': True,
            'columnOutputDataset': 'enron_bow2_svd_output'
        }
    })

    print mldb.put('/v1/procedures/apply_svd', {
        'type': 'transform',
        'params': {
            'inputData': """
                select svd({ {*} as row}) from enron_bow2
            """,
            'outputDataset': 'enron_bow2_svd',
            'runOnCreation': True
        }
    })


def fg_w2v():
    print mldb.put('/v1/functions/pooler', {
        "type": "pooling",
        "params": {
            "embeddingDataset": "w2v"
        }
    })

    print mldb.put('/v1/functions/bow_embed', {
        "type": "sql.expression",
        "params": {
            'expression': 'pooler({words: {bow({msg})[bow] as *}}) as bow_w2v'
        }
    })


def fg_stats_table():
    print mldb.put('/v1/procedures/train_stats_tables', {
        'type': 'statsTable.bagOfWords.train',
        'params': {
            'trainingData': 'select bow2({msg:msg})[bow2] as * from enron_data',
            'outcomes': [['label', "label = 'spam'"]],
            'statsTableFileUrl': 'file://bow2_stats_table.st',
            'outputDataset': 'bow2_st',
            'runOnCreation': True
        }
    })

    print mldb.put('/v1/functions/bow2_posneg', {
        'type': 'statsTable.bagOfWords.posneg',
        'params': {
            'numPos': 5000,
            'numNeg': 5000,
            'minTrials': 10,
            'outcomeToUse': 'label',
            'statsTableFileUrl': 'file://bow2_stats_table.st'
        }
    })

    print mldb.put('/v1/functions/apply_bow2_posneg', {
        'type': 'sql.expression',
        'params': {
            'expression': 'bow2_posneg({words: {bow2({msg:msg})[bow2] as *}})[probs] as *'
        }
    })

    print mldb.put('/v1/functions/agg_bow2_posneg', {
        'type': 'sql.expression',
        'params': {
            'expression': 'horizontal_avg({apply_bow2_posneg({msg:msg}) as *})'
        }
    })


def fg_all():
    # print mldb.put('/v1/datasets/all_feats', {
    #     'type': 'merged',
    #     'params':{
    #         'datasets': ['enron_bow_embed']
    #     }
    # })

    print mldb.put('/v1/procedures/generate_raw_feats', {
        'type': 'transform',
        'params': {
            'inputData': """
            select {bow_embed({msg}) as *
                    } as features,
                    label = 'spam' as label
                from enron_data
                """,
            'outputDataset': 'enron_features',
            'runOnCreation': True
        }
    })

# Finally, let's train a very simple classifier, by training on the first half of the messages, and testing on the second half. This classifier will give a score to every email, and we can then choose a threshold where everything above the threshold is classified as spam, and every thing below as ham.

# In[6]:


def train():
    # n = mldb.get('/v1/query', q='select count(*) as n from enron_features',
    #             format='aos').json()[0]['n']
    res = mldb.put('/v1/procedures/experiment', {
        'type': 'classifier.experiment',
        'params': {
            'experimentName': 'enron_experiment1',
            # 'trainingData': 'select {features.* as *} as features, label from enron_features',
            'trainingData': 'select {features.*} as features, label from enron_features',

            # for now 50/50 split in time, but we might do something more
            # fancy later!
            'datasetFolds': [{
                'training_limit': NB_TRAIN,
                'testing_offset': NB_TRAIN,
                'testing_limit': NB_TEST,
                'orderBy': 'index',
            }],
            'modelFileUrlPattern': 'file://enron_model.cls',
            'algorithm': 'bbdt',
            'runOnCreation': True
        }
    })
    print res


    # In[7]:

    from pprint import pprint
    pprint(res.json())
    print 'AUC =', res.json()['status']['firstRun']['status']['aggregatedTest']['auc']['mean']


# Not a bad AUC for a model that simple. But [the AUC score of a classifier is only a very generic measure of performance][1]. When having a specific problem like spam filtering, we're better off using a performance metric that truly matches our intuition about what a good spam filter ought to be. Namely, a good spam filtering algorithm should almost never flag as spam a legitime email, while keeping your inbox as spam-free as possible. This is what should be used to choose the threshold for the classifier, and then to measure its performance.
#
# So instead of the AUC (that doesn't pick a specific threshold but uses all of them), let's use as our performance metric the best [$F_{0.05}$ score][2], which gives 20 times more importance to precision than recall. In other words, this metric represents the fact that classifying as spam **only** what is really spam is 20 times more important than finding all the spam.
#
# Let's see how we are doing with that metric.
# [1]: http://mldb.ai/blog/posts/2016/01/ml-meets-economics/
# [2]: https://en.wikipedia.org/wiki/F1_score

# In[8]:

def test():
    print mldb.put('/v1/functions/enron_score', {
        'type': 'sql.expression',
        'params': {
            'expression': """
                (1 + pow(ratio, 2)) * (precision * recall) / (precision * pow(ratio, 2) + recall) as enron_score
                """
        }
    })


    # In[9]:

    print mldb.query("""
        select "truePositives", "trueNegatives", "falsePositives", "falseNegatives", precision, recall, score,
            enron_score({precision, recall, ratio:0.05}) as *
        from enron_experiment1_results_0
        order by enron_score desc
        limit 3
    """)

    print mldb.put('/v1/functions/classify', {
        'type': 'classifier',
        'params': {
            'modelFileUrl': 'file://enron_model.cls'
        }
    })


#     As you can see, the best threshold is the one where in case of doubt, everything is classified as "ham". This leads to 615 spam messages in the inbox, but no ham wrongly filtered as spam. Clearly this can be improved!

# import_data()
# import_w2v()
# fg_bow()
# fg_w2v()
fg_stats_table()
# fg_all()
# train()
# test()

# add_0_column('enron_bow2')
# fg_svd() # waiting for the svd to get fixed

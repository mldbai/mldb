# coding: utf-8

# # Spam Filtering Using [Euron's Dataset][1]
# [1]: [http://www.aueb.gr/users/ion/data/enron-spam/]

# In[1]:

from pymldb import Connection
mldb = Connection('http://localhost:8087')

# First let's load the 1st of Enron's datasets (there are 6) into MDLB, using a separate script.

# In[2]:

def import_data():
    print mldb.post('/v1/procedures', {
        'type': 'import.text',
        'params': {
            'dataFileUrl': 'http://public.mldb.ai/datasets/enron.csv.gz',
            'outputDataset': 'enron_all',
            'named': "'enron_' + dataset + '_mail_' + index",
            # 'where': 'dataset = 1',
            'runOnCreation': True
        }
    })

    n = mldb.get('/v1/query', q='select count(*) as n from enron_all',
                              format='aos').json()[0]['n']
    print('there are', n, 'data points')
    nb_train = n * 5 // 6
    nb_test = n - nb_train

    # train test split
    for where, id in [
        ('limit ' + str(nb_train), 'enron_train'),
        ('limit {} offset {} '.format(nb_test, nb_train), 'enron_test')]:
        # ('dataset != 6', 'enron_train'),
        # ('dataset = 6', 'enron_test')]:
        print mldb.post('/v1/procedures', {
            'type': 'transform',
            'params': {
                'inputData': """
                    select msg, label from enron_all
                    order by index, rowHash()
                    {}
                    """.format(where),
                'outputDataset': {
                    'type': 'tabular',
                    'id': id
                },
                'runOnCreation': True
            }
        })


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

    print mldb.put('/v1/functions/bow2', {
        'type': 'sql.expression',
        'params': {
            'expression': """
                tokenize(stemmerdoc({document:msg})[document], {
                    splitchars: ' \n', quotechar: '', ngram_range:[1,3],
                    min_token_length:2}) as bow2
                """
        }
    })

    print mldb.post('/v1/procedures', {
        'type': 'transform',
        'params': {
            'inputData': """
                select bow2({msg})[bow2] as *
                from enron_train
                """,
            'outputDataset': 'enron_train_bow2',
            'runOnCreation': True
        }
    })


def fg_svd():
    # FIXME the column expr here is really slow
    # do we really need it??? I could just uste everything
    print mldb.put('/v1/procedures/train_svd', {
        'type': 'svd.train',
        'params': {
            'trainingData': """
                select column expr (
                    order by rowCount() desc
                    limit 10000
                )
                from enron_train_bow2
            """,
            'numSingularValues': 100,
            'numDenseBasisVectors': 2000,
            'modelFileUrl': 'file://enron_bow2.svd',
            'runOnCreation': True,
            'columnOutputDataset': 'enron_bow2_svd_output',
            'functionName': 'bow2_svd_embed_row'
        }
    })

    print mldb.put('/v1/functions/bow2_svd_embed_msg', {
        'type': 'sql.expression',
        'params': {
            'expression': 'bow2_svd_embed_row({row: {bow2({msg})[bow2] as *}}) as *'
        }
    })

    # print mldb.put('/v1/procedures/apply_svd', {
    #     'type': 'transform',
    #     'params': {
    #         'inputData': """
    #             select svd({ {*} as row}) from enron_bow2
    #         """,
    #         'outputDataset': 'enron_bow2_svd',
    #         'runOnCreation': True
    #     }
    # })


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
            'trainingData': 'select bow2({msg})[bow2] as * from enron_train',
            'outcomes': [['label', "label = 'spam'"]],
            'statsTableFileUrl': 'file://bow2_stats_table.st',
            # 'outputDataset': 'bow2_st',
            'runOnCreation': True
        }
    })

    print mldb.put('/v1/functions/bow2_posneg', {
        'type': 'statsTable.bagOfWords.posneg',
        'params': {
            'numPos': 10000,
            'numNeg': 10000,
            'minTrials': 5,
            'outcomeToUse': 'label',
            'statsTableFileUrl': 'file://bow2_stats_table.st'
        }
    })

    print mldb.put('/v1/functions/apply_bow2_posneg', {
        'type': 'sql.expression',
        'params': {
            'expression': 'bow2_posneg({words: {bow2({msg})[bow2] as *}})[probs] as *'
        }
    })

    print mldb.put('/v1/functions/agg_bow2_posneg', {
        'type': 'sql.expression',
        'params': {
            'expression': 'horizontal_avg({apply_bow2_posneg({msg}) as *}) as bow2_posneg_avg,'
                          'horizontal_max({apply_bow2_posneg({msg}) as *}) as bow2_posneg_max,'
                          'horizontal_min({apply_bow2_posneg({msg}) as *}) as bow2_posneg_min'
        }
    })


def fg_all():
    for dataset in ['train', 'test']:
        print mldb.put('/v1/procedures/generate_raw_feats', {
            'type': 'transform',
            'params': {
                'inputData': """
                    select {
                        bow_embed({msg}) as *,
                        agg_bow2_posneg({msg}) as *,
                        bow2_svd_embed_msg({msg}) as *
                        } as features,
                        label = 'spam' as label
                    from enron_""" + dataset,
                'outputDataset': 'enron_{}_features'.format(dataset),
                'runOnCreation': True
            }
        })

# Finally, let's train a very simple classifier, by training on the first half of the messages, and testing on the second half. This classifier will give a score to every email, and we can then choose a threshold where everything above the threshold is classified as spam, and every thing below as ham.

# In[6]:


def train():
    res = mldb.put('/v1/procedures/experiment', {
        'type': 'classifier.train',
        'params': {
            'trainingData': 'select {features.*} as features, label from enron_train_features',
            'modelFileUrl': 'file://enron_model.cls',
            'algorithm': 'bbdt',
            'functionName': 'score',
            'runOnCreation': True
        }
    })
    print res

    # In[7]:

    from pprint import pprint
    pprint(res.json())
    # print 'AUC =', res.json()['status']['firstRun']['status']['aggregatedTest']['auc']['mean']


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

    for dataset in ['train', 'test']:
        res = mldb.post('/v1/procedures', {
            'type': 'classifier.test',
            'params': {
                'testingData': 'select score({{features: {{features.*}}}})[score] as score, label from enron_{}_features'.format(dataset),
                'mode': 'boolean',
                'outputDataset': 'enron_{}_test_results'.format(dataset),
                'runOnCreation': True
            }
        })
        print(res)


    print mldb.query("""
        select "truePositives", "trueNegatives", "falsePositives", "falseNegatives", precision, recall, score,
            enron_score({precision, recall, ratio:0.05}) as *
        from enron_test_test_results
        order by enron_score desc
        limit 3
    """)

def explain():
    mldb.put('/v1/functions/explain', {
        'type': 'classifier.explain',
        'params': {
            'modelFileUrl': 'file://enron_model.cls'
        }
    })

    # mldb.post('/v1/procedures', {
    #     'type': 'procedures.transform'

#     As you can see, the best threshold is the one where in case of doubt, everything is classified as "ham". This leads to 615 spam messages in the inbox, but no ham wrongly filtered as spam. Clearly this can be improved!

print('import')
import_data()
# import_w2v()
print('fg bow')
fg_bow()
print('fg w2v')
fg_w2v()
print('fg svd')
fg_svd()
fg_stats_table()
print('generate features')
fg_all()
print('train')
train()
print('test')
test()
explain()

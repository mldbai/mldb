import argparse
from pymldb import Connection
import rec.settings as _
from rec.utils import dataset_conf

def get_where_train(date):
    return ["when(verb) <= timestamp('{}')".format(date),
                   "rowHash() % 2 = 0"]
def get_where_test(date):
    return ["when(verb) > timestamp('{}')".format(date),
                   "rowHash() % 2 = 1"]


def setup_train_pipeline(mldb, date, nb_train, nb_test):
    # ################################ SVD ####################################
    # Item svd
    select_item_row_for_svd = '* EXCLUDING (title)'
    mldb.create_procedure(
        _.ITEM_SVD.ID, 'svd.train',
        dataset=dataset_conf(_.ITEM_DATASET),
        # output=dataset_conf(_.ITEM_SVD.OUTPUT, 'embedding'),
        # rowOutput=dataset_conf(_.ITEM_SVD.ROW_OUTPUT, 'embedding'),
        modelFileUrl=_.ITEM_SVD.FILE,
        numSingularValues=_.ITEM_SVD.NUM_SING_VALUES,
        numDenseBasisVectors=_.ITEM_SVD.NUM_DENSE,
        select=select_item_row_for_svd)

    # svd on the user/item matrix
    mldb.create_procedure(
        _.USER_ITEM_SVD.ID, 'svd.train',
        dataset=dataset_conf(_.USER_ITEM_DATASET),
        # output=dataset_conf(_.USER_ITEM_SVD.OUTPUT, 'embedding'),
        # rowOutput=dataset_conf(_.USER_ITEM_SVD.ROW_OUTPUT, 'embedding'),
        modelFileUrl=_.USER_ITEM_SVD.FILE,
        numSingularValues=_.USER_ITEM_SVD.NUM_SING_VALUES,
        numDenseBasisVectors=_.USER_ITEM_SVD.NUM_DENSE,
        select='*')

    # ############################ TRAIN/TEST SET #############################
    # Build the train and test set
    where_commun = ["verb='rate'",
                    "(compl >= 4 OR compl <= 2.5)"]
    where_train = where_commun + get_where_train(date)
    where_test = where_commun + get_where_test(date)
    params = dict(
        inputDataset=dataset_conf(_.ACTION_DATASET),
        outputDataset=dataset_conf(_.TRAIN_DATASET, 'beh.mutable'),
        select='user_id, item_id, (compl >= 4) as label',
        where=' AND '.join(where_train),
        limit=nb_train)
    mldb.create_procedure(_.DATASET_MANAGER, 'transform', **params)
    params['outputDataset']=dataset_conf(_.TEST_DATASET, 'beh.mutable')
    params['where'] = ' AND '.join(where_test)
    params['limit'] = nb_test
    mldb.create_procedure(_.DATASET_MANAGER_TEST, 'transform', **params)

    # ######################## FEATURE GENERATION #############################
    # Feature generation
    # direct item features
    mldb.create_function(
        _.FEAT_GEN.ITEM, 'sql.query',
        select = 'year, decade, "genre:"*',
        from_ = dataset_conf(_.ITEM_DATASET),
        where = 'rowName() = $item_id')

    # item svd feat gen
    mldb.create_function(
        _.FEAT_GEN.ITEM_SVD, 'sql.query',
        select='APPLY FUNCTION {} WITH (object(select {}) as row) EXTRACT (embedding as item_embedding)'
        .format(_.ITEM_SVD.FUNC_ID, select_item_row_for_svd),
        from_=dataset_conf(_.ITEM_DATASET),
        where='rowName() = $item_id')

    # diff comparing features of a user's history vs the item we are showing
    # him
    history_from = """
        {0} as actions
        JOIN {1} as history ON actions.item_id = history.item_id
        JOIN {1} as item ON item.item_id = $item_id""".format(
        _.ACTION_DATASET, _.ITEM_DATASET)

    history_params = dict(
        from_ = history_from,
        where = 'actions.user_id = $user_id',
        groupBy = 'actions.user_id'
    )

    mldb.create_function(
        _.FEAT_GEN.DIFF_NUM, 'sql.query',
        select = 'min(history.year - item.year) as diff_year',
        **history_params)

    # TODO in theory this (or something similar) should be possible
    if False:
        query = """
        min(vector_diff(
            APPLY FUNCTION {0} WITH(hist.item_id as item_id) EXTRACT(*),
            APPLY FUNCTION {0} WITH(item.item_id as item_id) EXTRACT(*)))
        """.format(_.FEAT_GEN.ITEM_SVD)
        # same but comparing the embeddings
        mldb.create_function(
            _.FEAT_GEN.DIFF_ITEM_SVD, 'sql.query',
            select = query,
            **history_params)

    # user/item svd feat gen
    mldb.create_function(
        _.FEAT_GEN.USER_ITEM_SVD, 'sql.query',
        select='APPLY FUNCTION {} WITH (object(select *) as row)'
               ' EXTRACT (embedding as user_item_embedding)'
        .format(_.USER_ITEM_SVD.FUNC_ID),
        from_=dataset_conf(_.USER_ITEM_DATASET_ALL),
        where='rowName() = $user_id')
    # TODO essayer la diff entre le user et le movie dans l'espace des deux
    # avec embedCol ou qqch

    # TODO some time features
    # mldb.create_function(
    #     _.FEAT_GEN.TEMPORAL, 'sql.query',
    #     select='DAYOFWEEK($ts), MONTH_OF_YEAR($ts), IS_FULL_MOON($ts), etc.',
    #     from_='no from!!')

    # all feature generators put together
    mldb.create_function(
        _.GENERATE_FEATURES, 'sql.expression',
        expression=','.join(
            ['APPLY FUNCTION {} WITH(user_id, item_id, when(label) as ts) EXTRACT(*)'
             .format(getattr(_.FEAT_GEN, fg))
             for fg in ['ITEM', 'ITEM_SVD', 'USER_ITEM_SVD']]))
             # for fg in ['ITEM', 'ITEM_SVD', 'USER_ITEM_SVD', 'DIFF_NUM']]))

    # ########################## WRITING THE FEATURES #########################
    # Build the features train set
    mldb.create_procedure(
        _.FEATURE_GENERATION, 'transform',
        inputDataset=dataset_conf(_.TRAIN_DATASET),
        outputDataset=dataset_conf(_.TRAIN_FEATS_DATASET, 'beh.mutable'),
        select='label, APPLY FUNCTION {} WITH (*) EXTRACT (*)'.format(
            _.GENERATE_FEATURES))

    # Build the features test set
    mldb.create_procedure(
        _.FEATURE_GENERATION_TEST, 'transform',
        inputDataset=dataset_conf(_.TEST_DATASET),
        outputDataset=dataset_conf(_.TEST_FEATS_DATASET, 'beh.mutable'),
        select='label, APPLY FUNCTION {} WITH (*) EXTRACT (*)'.format(
            _.GENERATE_FEATURES))

    # ######################## CLASSIFIER #####################################
    # Classifier
    mldb.create_procedure(
        _.CLASSIFIER, 'classifier.train',
        dataset=dataset_conf(_.TRAIN_FEATS_DATASET),
        modelFileUrl='file://' + _.CLASSIFIER + '.cls',
        algorithm='bbdt',
        label='label',
        select='* EXCLUDING (label)')

    # ############################ TESTING ####################################
    # Testing the classifier
    mldb.create_procedure(
        _.CLASSIFIER_TESTING, 'classifier.test',
        dataset=dataset_conf(_.TEST_FEATS_DATASET),
        output=dataset_conf(_.CLASSIFIER_TESTING_SCORES, 'beh.mutable'),
        score='APPLY FUNCTION {} WITH(object(select * EXCLUDING (label)) as features) EXTRACT'
              ' (score)'.format(_.SCORE),
        label='label')


def run_train_pipeline(mldb):
    # Train the item svd
    mldb.datasets(_.ITEM_SVD.OUTPUT).delete()
    mldb.datasets(_.ITEM_SVD.ROW_OUTPUT).delete()
    mldb.procedures(_.ITEM_SVD.ID).runs.post_json({})
    mldb.create_function(
        _.ITEM_SVD.FUNC_ID, 'svd.embedRow',
        modelFileUrl=_.ITEM_SVD.FILE)

    # Train the user/item svd
    mldb.datasets(_.USER_ITEM_SVD.OUTPUT).delete()
    mldb.datasets(_.USER_ITEM_SVD.ROW_OUTPUT).delete()
    mldb.procedures(_.USER_ITEM_SVD.ID).runs.post_json({})
    mldb.create_function(
        _.USER_ITEM_SVD.FUNC_ID, 'svd.embedRow',
        modelFileUrl=_.USER_ITEM_SVD.FILE,
        acceptUnknownValues=True)

    # Make the train/test set
    mldb.datasets(_.TRAIN_DATASET).delete()
    mldb.procedures(_.DATASET_MANAGER).runs.post_json({})
    print('nb train examples', mldb.datasets(_.TRAIN_DATASET)
          .get()['status']['rowCount'])
    mldb.datasets(_.TRAIN_FEATS_DATASET).delete()
    mldb.procedures(_.FEATURE_GENERATION).runs.post_json({})

    # Train a classifier
    mldb.procedures(_.CLASSIFIER).runs.post_json({})

    # Scorer
    mldb.create_function(
        _.SCORE, 'classifier',
        modelFileUrl='file://' + _.CLASSIFIER + '.cls')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--date', '-d', required=True, help='date YYYY-MM-DD that separates'
        ' training and testing')
    parser.add_argument('--nb-train', '-n', default=2000, type=int,
                        help='nb training examples')
    args = parser.parse_args()

    mldb = Connection(_.HOST)
    # missing bit of datasets, the user/item sparse matrix
    where_commun = ["verb='rate'"]

    params = dict(
        select='scatter(item_id, compl) AS *',
        groupBy = 'user_id',
        rowName = 'user_id',
        inputDataset=dataset_conf(_.ACTION_DATASET))

    # the one for training the SVD, only on the train set
    mldb.create_procedure(
        _.USER_ITEM_DATASET_MAKER, 'transform',
        where = ' AND '.join(where_commun + get_where_train(args.date)),
        outputDataset=dataset_conf(_.USER_ITEM_DATASET, 'beh.mutable'),
        **params)
    mldb.procedures(_.USER_ITEM_DATASET_MAKER).runs.post_json({})

    # the one for testing, on everything
    mldb.create_procedure(
        _.USER_ITEM_DATASET_MAKER_ALL, 'transform',
        where = ' AND '.join(where_commun),
        outputDataset=dataset_conf(_.USER_ITEM_DATASET_ALL, 'beh.mutable'),
        **params)
    mldb.procedures(_.USER_ITEM_DATASET_MAKER_ALL).runs.post_json({})

    setup_train_pipeline(mldb, args.date, args.nb_train, args.nb_train)
    run_train_pipeline(mldb)

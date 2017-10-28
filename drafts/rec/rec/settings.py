import json, os

ENV_VAR = 'MLDB_REC_CONFIG'
if ENV_VAR not in os.environ:
    raise Exception('You must set the {} environment variable'.format(ENV_VAR))
config = json.load(open(os.environ[ENV_VAR]))

HOST = config['host']
PREFIX = config['prefix']
DATA_DIR = config['data_dir']  # 'file://data'

ITEM_DATASET = PREFIX + '_items'
ACTION_DATASET = PREFIX + '_actions'

USER_ITEM_DATASET_MAKER = PREFIX + '_user_item_maker'
USER_ITEM_DATASET_MAKER_ALL = PREFIX + '_user_item_maker_all'
USER_ITEM_DATASET = PREFIX + '_user_item'
USER_ITEM_DATASET_ALL = PREFIX + '_user_item_all'
TRAIN_DATASET = PREFIX + '_train'
TEST_DATASET = PREFIX + '_test'
TRAIN_FEATS_DATASET = PREFIX + '_train_feats'
TEST_FEATS_DATASET = PREFIX + '_test_feats'

class ITEM_SVD:
    ID = PREFIX + '_item_svd'
    OUTPUT = PREFIX + '_item_svd_output'
    ROW_OUTPUT = PREFIX + '_item_svd_row_output'
    NUM_SING_VALUES = 4
    NUM_DENSE = 1000
    FILE = 'file://' + ID + '.svd'
    FUNC_ID = PREFIX + '_item_svd_embed'

class USER_ITEM_SVD:
    ID = PREFIX + '_user_item_svd'
    OUTPUT = PREFIX + '_user_item_svd_output'
    ROW_OUTPUT = PREFIX + '_user_item_svd_row_output'
    NUM_SING_VALUES = 100
    NUM_DENSE = 2000
    FILE = 'file://' + ID + '.svd'
    FUNC_ID = PREFIX + '_user_item_svd_embed'

DATASET_MANAGER = PREFIX + '_dataset_manager'
DATASET_MANAGER_TEST = PREFIX + '_dataset_manager_test'
GENERATE_FEATURES = PREFIX + '_generate_features'
FEATURE_GENERATION = PREFIX + '_feature_generation'
FEATURE_GENERATION_TEST = PREFIX + '_feature_generation_test'

class FEAT_GEN:
    ITEM = PREFIX + '_item_feat_gen'
    ITEM_SVD = PREFIX + '_item_svd_feat_gen'
    USER_ITEM_SVD = PREFIX + '_user_item_svd_feat_gen'
    DIFF_NUM = PREFIX + '_diff_num_feat_gen'
    DIFF_ITEM_SVD = PREFIX + '_diff_item_svd_feat_gen'

CLASSIFIER = PREFIX + '_classifier'
CLASSIFIER_TESTING = PREFIX + '_classifier_testing'
CLASSIFIER_TESTING_SCORES = PREFIX + '_classifier_testing_scores'
SCORE = PREFIX + '_score'

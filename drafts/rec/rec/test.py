from pprint import pprint
from pymldb import Connection
import rec.settings as _


def run_test_pipeline(mldb):
    mldb.datasets(_.TEST_DATASET).delete()
    mldb.procedures(_.DATASET_MANAGER_TEST).runs.post_json({})
    print('nb test examples', mldb.datasets(_.TEST_DATASET)
          .get()['status']['rowCount'])
    mldb.datasets(_.TEST_FEATS_DATASET).delete()
    mldb.procedures(_.FEATURE_GENERATION_TEST).runs.post_json({})
    runid = 'lastest'
    mldb.datasets(_.CLASSIFIER_TESTING_SCORES).delete()
    mldb.procedures(_.CLASSIFIER_TESTING).runs(runid).delete()
    mldb.procedures(_.CLASSIFIER_TESTING).runs.post_json({'id': runid})
    r = mldb.procedures(_.CLASSIFIER_TESTING).runs(runid).get_query()
    print('auc', r['status']['auc'])
    # r = mldb.datasets(_.CLASSIFIER_TESTING_SCORES).query.get_query(
    #     format='aos')
    # pprint(r)


if __name__ == '__main__':
    mldb = Connection(_.HOST)
    run_test_pipeline(mldb)

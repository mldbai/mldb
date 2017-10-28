from pprint import pprint
from pymldb import Connection
import rec.settings as _


def run_score_pipeline(mldb):
    # mldb.procedures(_.DATASET_MANAGER_TEST).runs.post_json({})
    # mldb.procedures(_.FEATURE_GENERATION_TEST).runs.post_json({})
    # r = mldb.datasets(_.TEST_FEATS_DATASET).query.get_query(
    #     select='APPLY FUNCTION {} WITH(object(select *) as features) EXTRACT(*)'.format(_.SCORE),
    #     limit=100,
    #     format='aos')
    # pprint(r)

    r = mldb.datasets(_.ACTIONS_DATASET).query.get_query(
        select='user_id, apply function {} with(*) extract(*), apply function',
        limit=10,
        format='aos')
    pprint(r)


if __name__ == '__main__':
    mldb = Connection(_.HOST)
    run_score_pipeline(mldb)

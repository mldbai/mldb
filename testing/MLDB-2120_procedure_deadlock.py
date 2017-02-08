#
# MLDB-2120_procedure_deadlock.py
# Francois-Michel L'Heureux, 2017-01-20
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#
# The deadlock set forth by this test was in rest_collection_impl when it was
# exposed to procedure creation bombardment.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

import time

class Mldb2120ProcedureDeadlock(MldbUnitTest):  # noqa

    def test_it(self):

        # create the dataset
        files = [
            "file://container_files/public_html/doc/builtin/img/AWS_LOGO_RGB_300px.jpg",
            "file://testing/logo-new.jpg",
            "file://mldb_data/plugins/mlpaint/static/6.png",
            "file://mldb_data/plugins/mlpaint/static/3.png",
            "file://mldb_data/plugins/mlpaint/static/5.png",
            "file://mldb_data/plugins/mlpaint/static/0.png",
            "file://mldb_data/plugins/mlpaint/static/8.png",
            "file://mldb_data/plugins/mlpaint/static/1.png",
            "file://mldb_data/plugins/mlpaint/static/4.png",
            "file://mldb_data/plugins/mlpaint/static/2.png",
            "file://mldb_data/plugins/mlpaint/static/7.png",
            "file://mldb_data/plugins/mlpaint/static/9.png",
            "file://container_files/public_html/doc/builtin/img/virtualbox_logo.png",
            "file://container_files/public_html/doc/builtin/img/logo_docker.png",
            "file://container_files/public_html/doc/builtin/img/gaussian.png",
            "file://container_files/public_html/resources/images/mldb_ipython_logo.png",
            "file://container_files/public_html/resources/js/tipuesearch/img/search.png"]
        ds = mldb.create_dataset({'id' : 'files', 'type' : 'tabular'})
        for f in files:
            ds.record_row(f, [['url', f, 0]])
        ds.commit()

        # prepare inception, a function slow enough to trigger the issue
        INCEPTION_URL = \
            "https://public.mldb.ai/models/inception_dec_2015.zip"

        mldb.put('/v1/functions/inceptionJpeg', {
            "type": 'tensorflow.graph',
            "params": {
                "modelFileUrl": 'archive+' + INCEPTION_URL + '#tensorflow_inception_graph.pb',
                "inputs": 'content AS "DecodeJpeg/contents"',
                "outputs": "pool_3"
            }
        })
        mldb.put('/v1/functions/inceptionPng', {
            "type": 'tensorflow.graph',
            "params": {
                "modelFileUrl": 'archive+' + INCEPTION_URL + '#tensorflow_inception_graph.pb',
                "inputs": 'content AS "DecodePng/contents"',
                "outputs": "pool_3"
            }
        })


        BATCH_SIZE = 1
        query = """
            SELECT try(CASE
                        WHEN regex_match(image_url, '.*\.[jJ][pP][eE]?[gG]$') THEN inceptionJpeg({{content}})
                        WHEN regex_match(image_url, '.*\.[pP][nN][gG]$') THEN inceptionPng({{content}})
                        ELSE {{'NOT SUPPORTED FORMAT'}}
                        END,
                    {{ERROR}}) AS *,
                    image_url,
                    cei
                    NAMED image_url
            FROM (
                SELECT content, image_url, cei FROM (
                    SELECT fetcher(image_url) AS *, image_url, cei
                    FROM (
                        SELECT * FROM files
                        ORDER BY rowName() LIMIT {limit} OFFSET {offset}
                    )
                ) WHERE error IS NULL)
        """
        urls = []
        for it in xrange(300):
            for idx in xrange(len(files)):
                res = mldb.put_async('/v1/procedures/proc_{}_{}'.format(it, idx), {
                    'type' : 'transform',
                    'params' : {
                        'inputData' : query.format(offset=idx * BATCH_SIZE, limit=BATCH_SIZE),
                        'outputDataset' : 'out{}_{}'.format(it, idx)
                    }
                }).json()
                urls.append('/v1/procedures/{}/runs/{}'.format(
                    res['id'], res['status']['firstRun']['id']))
                mldb.log('Created: ' + urls[-1])

        sleep_counter = 0
        for url in urls:
            while True:
                mldb.log('Waiting for: ' + url)
                res = mldb.get(url).json()
                if res['state'] == 'finished':
                    break
                if res['state'] == 'error':
                    raise Exception('There should be no error')
                sleep_counter += 1
                if sleep_counter == 100:
                    raise Exception('Too long, it should be finished by now')
                time.sleep(1)



if __name__ == '__main__':
    mldb.run_tests()

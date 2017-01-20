#
# MLDB-1873_encoding_unknown_column.py
# Francois Maillet, 2016-08-03
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1873EncodingUnknownColumn(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        mldb.post('/v1/procedures', {
            'type': 'import.text',
            'params': {
                'dataFileUrl': 'http://public.mldb.ai/datasets/enron.csv.gz',
                'outputDataset': 'enron_data',
                'named': "'enron_' + dataset + '_mail_' + index",
                'where': 'dataset = 1'
                }
            })
        
        mldb.put('/v1/functions/bow', {
            'type': 'sql.expression',
            'params': {
                'expression': """
                    tokenize(msg, {splitChars: ' :.-!?''"()[],', quoteChar: ''}) as bow
                    """
            }
        })

        mldb.post('/v1/procedures', {
            'type': 'transform',
            'params': {
                'inputData': """
                    select bow({msg})[bow] as *, label = 'spam' as message_is_spam
                    from enron_data
                    """,
                'outputDataset': 'enron_features'
            }
        })

    def test_it(self):
        # make sure training can run
        mldb.post('/v1/procedures', {
            'type': 'classifier.experiment',
            'params': {
                'experimentName': 'enron_experiment1',
                'inputData': '''
                    select
                        {* excluding(message_is_spam)} as features, 
                        message_is_spam as label 
                    from enron_features''',
                'modelFileUrlPattern': 'file://enron_model_$runid.cls',
                'configurationFile': 'file://mldb/container_files/classifiers.json',
                'algorithm': 'dt'
            }
        })


if __name__ == '__main__':
    mldb.run_tests()

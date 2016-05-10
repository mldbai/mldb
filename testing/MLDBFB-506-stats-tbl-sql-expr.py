mldb = mldb_wrapper.wrap(mldb) # noqa


class BowSqlExprTest(MldbUnitTest):

    def check_true(self, q, truth):
        data = mldb.get('/v1/query', q=q, format='aos', rowNames=0).json()
        try:
            self.assertEqual(len(data), len(truth))
            for x,y in zip(data, truth):
                self.assertEqual(x,y)
        except AssertionError as e:
            mldb.log(e)
            mldb.log(data)
            mldb.log('not equal to')
            mldb.log(truth)
            raise

    def setUp(self):
        dataset = '/v1/datasets/veggies'
        mldb.put(dataset, {'type': 'sparse.mutable'})

        # dataset:
        #
        #    txt    label
        #   =============
        #    a,c      0
        #    b,c      1
        #    a,d      0
        #    b,d      1

        for i in range(4):
            mldb.post(dataset + '/rows', dict(
                rowName=i,
                columns = [
                    ['txt', ['a', 'b'][i%2] + ',' + ['c', 'd'][i//2], 0],
                    ['label', [0,1][i%2], 0]
                ]))
        mldb.post(dataset + '/commit')

        self.check_true('select * from veggies order by rowName()',
                        [{'txt': 'a,c', 'label':0},
                         {'txt': 'b,c', 'label':1},
                         {'txt': 'a,d', 'label':0},
                         {'txt': 'b,d', 'label':1},
        ])


    def test_all(self):
        mldb.put('/v1/functions/bow', {
            'type': 'sql.expression',
            'params': {
                'expression': 'tokenize(txt) as bow'
            }
        })

        self.check_true('select bow({txt})[bow] as * from veggies order by rowName()',
                        [{'a': 1, 'c': 1},
                         {'b': 1, 'c': 1},
                         {'a': 1, 'd': 1},
                         {'b': 1, 'd': 1}
        ])


        mldb.put("/v1/procedures/train_bow", {
            "type": "statsTable.bagOfWords.train",
            "params": {
                "trainingData": 'select bow({txt})[bow] as * from veggies',
                "outcomes": [['label', 'label']],
                "statsTableFileUrl": 'file://bow.st',
                "runOnCreation": True,
                'outputDataset': 'patate'
            }
        })

        mldb.put('/v1/functions/posneg', {
            'type': 'statsTable.bagOfWords.posneg',
            'params': {
                'numPos': 2,
                'numNeg': 2,
                'minTrials': 1,
                'outcomeToUse': 'label',
                'statsTableFileUrl': 'file://bow.st'
            }
        })

        truth = [{'a.label': 0, 'c.label': .5}]
        self.check_true(
            "select posneg({words: {bow({txt:'a,c'})[bow] as *}})[probs] as *",
            truth)

        # put the last query in a sql.expression
        mldb.put('/v1/functions/apply_bow', {
            'type': 'sql.expression',
            'params': {
                'expression': 'posneg({words: {bow({txt})[bow] as *}})[probs] as posneg'
            }
        })

        # this should be equivalent to the last check
        self.check_true("select apply_bow({txt:'a,c'})[posneg] as *", truth)


mldb.run_tests()

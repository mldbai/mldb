/* MLDB-896_table_expression_serialised_as_string.cc
   Francois Maillet, 14 septembre 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Check that we're able to start from a JSON, put it into an Any, then
   convert it to JSON and back again to its structured type.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <string>
#include <vector>
#include <boost/test/unit_test.hpp>
#include "arch/exception.h"
#include "mldb/utils/string_functions.h"
#include "mldb/utils/vector_utils.h"
#include "mldb/types/value_description.h"
#include "mldb/types/json.h"
#include "mldb/types/any_impl.h"
#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/optional_description.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/builtin/sql_config_validator.h"

using namespace std;
using namespace MLDB;

// Copied from JML plugin to avoid core test having a dependency on a plugin
struct AccuracyConfig : public ProcedureConfig {
    static constexpr const char * name = "classifier.test";

    /// Sql query to select the testing data
    InputQuery testingData;

    bool uniqueScoresOnly = false;

    //check if label is among the 'N' top scores
    std::vector<size_t> accuracyOverN;

    /// Dataset we output to
    Optional<PolyConfigT<Dataset> > outputDataset;
    static constexpr char const * defaultOutputDatasetType = "tabular";
};

DECLARE_STRUCTURE_DESCRIPTION(AccuracyConfig);

DEFINE_STRUCTURE_DESCRIPTION(AccuracyConfig);


AccuracyConfigDescription::
AccuracyConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optionalOutputDataset;
    optionalOutputDataset.emplace(PolyConfigT<Dataset>().
                                  withType(AccuracyConfig::defaultOutputDatasetType));

    addField("testingData", &AccuracyConfig::testingData,
             "SQL query which specifies the scores, labels and optional weights for evaluation. "
             "The query is usually of the form: "
             "`select classifier_function({features: {f1, f2}})[score] as score, x as label from ds`.\n\n"
             "The select expression must contain these two columns: \n\n"
             "  * `score`: one scalar expression which evaluates to the score a classifier "
             "has assigned to the given row, and \n"
             "  * `label`: one scalar expression to identify the row's label, and whose type "
             "must match that of the classifier mode. Rows with null labels will be ignored. \n"
             "     * `boolean` mode: a boolean (0 or 1)\n"
             "     * `regression` mode: a real number\n"
             "     * `categorical` mode: any combination of numbers and strings for\n\n"
             "The select expression can contain an optional `weight` column. The weight "
             "allows the relative importance of examples to be set. It must "
             "be a real number. If the `weight` is not specified each row will have "
             "a weight of 1. Rows with a null weight will cause a training error. \n\n"
             "The query must not contain `GROUP BY` or `HAVING` clauses. ");
    addField("outputDataset", &AccuracyConfig::outputDataset,
             "Output dataset for scored examples. The score for the test "
             "example will be written to this dataset. Examples get grouped when "
              "they have the same score when `mode` is `boolean`. Specifying a "
             "dataset is optional.", optionalOutputDataset);
    addField("uniqueScoresOnly", &AccuracyConfig::uniqueScoresOnly,
              "If `outputDataset` is set and `mode` is set to `boolean`, setting this parameter "
              "to `true` will output a single row per unique score. This is useful if the "
              "test set is very large and aggregate statistics for each unique score is "
              "sufficient, for instance to generate a ROC curve. This has no effect "
              "for other values of `mode`.", false);
    addField("recallOverN", &AccuracyConfig::accuracyOverN,
              "Calculate a recall score over the top scoring labels."
              "Does not apply to boolean or regression modes.");
    
    addParent<ProcedureConfig>();

    onPostValidate = validateQuery(&AccuracyConfig::testingData,
                                   NoGroupByHaving(),
                                   PlainColumnSelect(),
                                   ScoreLabelSelect(),
                                   MustContainFrom());

}


BOOST_AUTO_TEST_CASE( tbl_expression_serialised_as_string )
{
    // create config
    string jsConf = "{\"testingData\": { \"from\": {\"id\": \"patate\"},   \
                                        \"select\": \"{*} as features, CLICK IS NOT NULL as label, SCORE as score\" }, \
                      \"outputDataset\": {\"type\": \"beh.mutable\", \"id\":\"output_tbl\"} \
                     }";

    Json::Value conf;
    Json::Reader reader;
    if (!reader.parse(jsConf, conf))
        throw MLDB::Exception("can't parse js conf");
    
    // convert it to an Any, the way it would be received in
    // the params field of a procedure config struct
    Any params(conf);
    auto classifierConfig = params.convert<AccuracyConfig>();

    // send it to json
    auto nowInJson = jsonEncode<AccuracyConfig>(classifierConfig);

    cout << nowInJson.toStyledString() << endl;
    cout << " --- " << endl;

    // convert it back to it's structured form. this should not fail
    BOOST_CHECK_NO_THROW(jsonDecode<AccuracyConfig>(nowInJson));


}


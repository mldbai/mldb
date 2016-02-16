/** csv_dataset.cc
    Jeremy Barnes, 11 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
    Dataset that reads a tabular CSV file into an indexed dataset.
*/

#include "csv_dataset.h"
#include "tabular_dataset.h"
#include "for_each_line.h"
#include "mldb/jml/utils/lightweight_hash.h"

#include "mldb/arch/thread_specific.h"
#include "mldb/arch/bit_range_ops.h"

#include "mldb/jml/utils/vector_utils.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"

#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/compact_vector_value_description.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/parallel_merge_sort.h"
#include "mldb/base/parse_context.h"
#include <mutex>

using namespace std;

namespace Datacratic {
namespace MLDB {

/*DEFINE_STRUCTURE_DESCRIPTION(CsvDatasetConfig);

CsvDatasetConfigDescription::CsvDatasetConfigDescription()
{
    addParent<PersistentDatasetConfig>();
    addField("headers", &CsvDatasetConfig::headers,
             "List of headers for when first row doesn't contain headers",
             vector<Utf8String>());
    addField("quotechar", &CsvDatasetConfig::quoter,
             "Character to enclose strings", string("\""));
    addField("delimiter", &CsvDatasetConfig::delimiter,
             "Delimiter for column separation", string(","));
    addField("limit", &CsvDatasetConfig::limit,
             "Maximum number of lines to process.  Bad lines including empty lines "
             "contribute to the limit.  As a result, it is possible for the dataset "
             "to contain less rows that the requested limit.");
    addField("offset", &CsvDatasetConfig::offset,
            "Skip the first n lines (excluding the header if present).", int64_t(0));
    addField("encoding", &CsvDatasetConfig::encoding,
             "Character encoding of file: 'us-ascii', 'ascii', 'latin1', 'iso8859-1', 'utf8' or 'utf-8'",
             string("utf-8"));
    addField("ignoreBadLines", &CsvDatasetConfig::ignoreBadLines,
             "If true, any line causing an error will be skipped. "
             "Empty lines are considered bad lines.", false);
    addField("replaceInvalidCharactersWith",
             &CsvDatasetConfig::replaceInvalidCharactersWith,
             "If this is set, it should be a single Unicode character that badly "
             "encoded characters within the CSV file will be replaced with. "
             "The default is nothing, which will cause lines with badly "
             "encoded characters to throw an error.");
    addField("select", &CsvDatasetConfig::select,
             "What to select from the dataset",
             SelectExpression::STAR);
    addField("where", &CsvDatasetConfig::where,
             "Row filter for CSV dataset",
             SqlExpression::TRUE);
    addField("named", &CsvDatasetConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name.",
             SqlExpression::parse("lineNumber()"));
    addField("timestamp", &CsvDatasetConfig::timestamp,
             "Expression for row timestamp.",
             SqlExpression::parse("fileTimestamp()"));

    onUnknownField = [] (CsvDatasetConfig * config,
                         JsonParsingContext & context)
        {
            if (context.fieldName() == "rowNameColumn") {
                context.exception("rowNameColumn has been removed.  Please use "
                                  "'named' for the row name and "
                                  "'select ... excluding (column)' to exclude it");
            }
            else if (context.fieldName() == "rowNamePrefix") {
                context.exception("rowNamePrefix has been removed.  Please use "
                                  "'select * as prefix* ' to rename columns");
            }
            else {
                context.exception("Unknown field '" + context.fieldName()
                                  + " parsing CSV dataset configuration");
            }
        };
}*/


/*****************************************************************************/
/* CSV DATASET                                                               */
/*****************************************************************************/

/*struct CsvDataset::Itl: public TabularDataStore {

    CsvDatasetConfig config;

    

    Itl(MldbServer * server, const CsvDatasetConfig & conf, const CsvDataset* parentDataset)
    {
        // For now... later we can memory map and only keep the offsets,
        // or stream through on a query
        config = conf;
        
    }*/

 /*   struct LineError {
        uint64_t lineNumber;
        int column;
        const char * message;
    };

    int64_t numLineErrors;
    std::vector<LineError> lineErrors;*/

//};


/*****************************************************************************/
/* CSV DATASET                                                               */
/*****************************************************************************/

/*CsvDataset::
CsvDataset(MldbServer * owner,
           PolyConfig config,
           const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    ExcAssert(!config.id.empty());
    
    auto params = config.params.convert<CsvDatasetConfig>();
    
    itl.reset(new Itl(server, params, this));
}

CsvDataset::
CsvDataset(MldbServer * owner)
    : Dataset(owner)
{
}



namespace {

RegisterDatasetType<CsvDataset, CsvDatasetConfig>
regCsv(builtinPackage(),
       "text.csv.tabular",
       "Exposes a dense CSV file as a dataset, with one line per row",
       "datasets/TabularCsvDataset.md.html");

} // file scope*/

} // namespace MLDB
} // namespace Datacratic


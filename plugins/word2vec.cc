// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** word2vec.cc
    Jeremy Barnes, 14 October 2015
    Copyright (c) mldb.ai inc.  All rights reserved.
*/

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/url.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/stats/distribution.h"
#include <boost/algorithm/string.hpp>
#include "mldb/utils/log.h"
#include "mldb/server/dataset_context.h"
#include "mldb/http/http_exception.h"

using namespace std;



namespace MLDB {

/*****************************************************************************/
/* SQL Word2Vec SCOPE                                                        */
/*****************************************************************************/

/** This allows an SQL expression to be bound to a parsed Word2Vec row
*/

struct SqlWord2VecScope: public SqlExpressionMldbScope {

    struct RowScope: public SqlRowScope {
        RowScope(std::string word, Date ts)
            : word_((Utf8String)word)
        {
        }

        CellValue word_;
        Date ts;
    };

    SqlWord2VecScope(MldbServer * server,
                Date fileTimestamp)
        : SqlExpressionMldbScope(server),
          fileTimestamp(fileTimestamp)
    {

    }

    Date fileTimestamp;

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                     const ColumnPath & columnName)
    {
        if (!tableName.empty()) {
            throw HttpReturnException(400, "Unknown table name in import.word2vec procedure",
                                      "tableName", tableName);
        }

        if (columnName.toUtf8String() != "word")
            throw HttpReturnException(400, "Unknown column name in import.word2vecprocedure",
                                      "columnName", columnName);

        return {[=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = scope.as<RowScope>();
                    return storage = ExpressionValue(row.word_, row.ts);
                },
                std::make_shared<StringValueInfo>()};
    }

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep)
    {
         throw HttpReturnException(400, "Cannot use wildcard in import.word2vec context");
    }

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope)
    {
        if (functionName == "fileTimestamp") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                    {
                        return ExpressionValue(fileTimestamp, fileTimestamp);
                    },
                    std::make_shared<TimestampValueInfo>()
                };
        }

        return SqlBindingScope::doGetFunction(tableName, functionName, args,
                                              argScope);
    }

    static RowScope bindRow(const std::string& word, Date ts)
    {
        return RowScope(word, ts);
    }
};



/*****************************************************************************/
/* WORD2VEC IMPORTER                                                         */
/*****************************************************************************/

struct Word2VecImporterConfig : ProcedureConfig {
    static constexpr const char * name = "import.word2vec";

    Word2VecImporterConfig()
        : offset(0), limit(-1), named(SqlExpression::parse("word"))
    {
        output.withType("embedding");
    }

    Url dataFileUrl;
    PolyConfigT<Dataset> output;
    uint64_t offset;
    int64_t limit;
    std::shared_ptr<SqlExpression> named;
};

DECLARE_STRUCTURE_DESCRIPTION(Word2VecImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(Word2VecImporterConfig);

Word2VecImporterConfigDescription::
Word2VecImporterConfigDescription()
{
    addField("dataFileUrl", &Word2VecImporterConfig::dataFileUrl,
             "URL to load Excel workbook from");
    addField("outputDataset", &Word2VecImporterConfig::output,
             "Output dataset for result",
             PolyConfigT<Dataset>().withType("embedding"));
    addField("offset", &Word2VecImporterConfig::offset,
             "Start at word number (0 = start)", (uint64_t)0);
    addField("limit", &Word2VecImporterConfig::limit,
             "Limit of number of rows to record (-1 = all)", (int64_t)-1);
    addField("named", &Word2VecImporterConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name.",  SqlExpression::parse("word"));
    addParent<ProcedureConfig>();
}

struct Word2VecImporter: public Procedure {

    Word2VecImporter(MldbServer * owner,
                 PolyConfig config_,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<Word2VecImporterConfig>();
    }

    Word2VecImporterConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);
        auto info = getUriObjectInfo(
            runProcConf.dataFileUrl.toDecodedString());

        filter_istream stream(runProcConf.dataFileUrl);

        std::string header;
        getline(stream, header);

        vector<string> fields;
        boost::split(fields, header, boost::is_any_of(" "));

        ExcAssertEqual(fields.size(), 2);

        int numWords = std::stoi(fields[0]);
        int numDims  = std::stoi(fields[1]);

        std::shared_ptr<Dataset> output;
        if (!runProcConf.output.type.empty() || !runProcConf.output.id.empty()) {
            output = createDataset(server, runProcConf.output, nullptr, true /*overwrite*/);
        }

        vector<ColumnPath> columnNames;
        for (unsigned i = 0;  i < numDims;  ++i) {
            columnNames.emplace_back(PathElement(i));
        }

        vector<tuple<RowPath, vector<float>, Date> > rows;
        int64_t numRecorded = 0;

        SqlWord2VecScope scope(server, info.lastModified);
        auto namedBound = config.named->bind(scope);

        for (unsigned i = 0;  i < numWords;  ++i) {
            std::string word;
            getline(stream, word, ' ');

            std::vector<float> vec(numDims);
            stream.read((char *)&vec[0], numDims * sizeof(float));

            if (i < runProcConf.offset)
                continue;
            if (runProcConf.limit != -1 && numRecorded >= runProcConf.limit)
                break;

            auto row = scope.bindRow(word, info.lastModified);
            ExpressionValue nameStorage;
            RowPath rowName(namedBound(row, nameStorage, GET_ALL)
                                .toUtf8String());

            rows.emplace_back(rowName, std::move(vec), info.lastModified);
            ++numRecorded;

            if (rows.size() == 10000) {
                if (output)
                    output->recordEmbedding(columnNames, rows);
                rows.clear();
                INFO_MSG(logger) << "recorded " << (i+1) << " of " << numWords << " words";
            }

            TRACE_MSG(logger) << "got word " << word;
        }

        if (output) {
            output->recordEmbedding(columnNames, rows);
            output->commit();
        }

        RunOutput result;
        return result;
    }

    virtual Any getStatus() const
    {
        return Any();
    }
};

RegisterProcedureType<Word2VecImporter, Word2VecImporterConfig>
regScript(builtinPackage(),
          "Import a word2vec file into MLDB",
          "procedures/Word2VecImporter.md.html");


} // namespace MLDB


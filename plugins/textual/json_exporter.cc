/**
 * json_export_procedure.cc
 * Jeremy, 2022-02-27
 * This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
 **/

#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/builtin/sql_config_validator.h"
#include "mldb/core/bound_queries.h"
#include "mldb/core/dataset_scope.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/any_impl.h"

#if 0
#include "mldb/core/mldb_engine.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/base/parallel.h"
#include "mldb/sql/table_expression_operations.h"
#include "mldb/sql/join_utils.h"
#include "mldb/sql/execution_pipeline.h"
#include "mldb/arch/backtrace.h"
#include "mldb/base/per_thread_accumulator.h"
#include "mldb/types/date.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/utils/vector_utils.h"
#include "json_writer.h"
#include <memory>
#endif

using namespace std;



namespace MLDB {

struct JsonWriter {
    JsonWriter(std::ostream & stream, bool prettyPrint, bool asArray, bool lineBreaks)
        : stream(stream), prettyPrint(prettyPrint), asArray(asArray), lineBreaks(lineBreaks)
    {
        if (asArray)
            stream << "[";
        endl();
    }

    ~JsonWriter()
    {
        if (asArray && !std::uncaught_exceptions()) {
            endl();
            stream << "]";
            endl();
        }
    }

    std::ostream & stream;
    bool prettyPrint = false;
    bool asArray = false;
    bool lineBreaks = true;

    template<typename T>
    JsonWriter & operator << (const T & t)
    {
        stream << t;
        return *this;
    }

    void endl()
    {
        if (lineBreaks)
            stream << "\n";
    }
};

struct JsonExportProcedureConfig : ProcedureConfig {
    static constexpr const char * name = "export.json";
    InputQuery exportData;
    Url dataFileUrl;
    bool prettyPrint = false;
    bool asArray = false;
    bool lineBreaks = true;
};

DEFINE_STRUCTURE_DESCRIPTION_INLINE(JsonExportProcedureConfig)
{
    addField("exportData", &JsonExportProcedureConfig::exportData,
             "An SQL query to select the data to be exported.  This could "
             "be any query on an existing dataset.");
    addField("dataFileUrl", &JsonExportProcedureConfig::dataFileUrl,
             "URL where the json file should be written to. If a file already "
             "exists, it will be overwritten.");
    addAuto("prettyPrint", &JsonExportProcedureConfig::prettyPrint,
            "Pretty-print the file, with JSON objects split over multiple "
            "indented lines.");
    addAuto("asArray", &JsonExportProcedureConfig::asArray,
            "Serialize as one big array rather than multiple concatenated JSON objects.");
    addAuto("lineBreaks", &JsonExportProcedureConfig::lineBreaks,
            "Insert line breaks between records.");
    addParent<ProcedureConfig>();

    onPostValidate = [&] (JsonExportProcedureConfig * cfg,
                          JsonParsingContext & context)
    {
        MustContainFrom()(cfg->exportData, JsonExportProcedureConfig::name);
    };
}

struct JsonExportProcedure: public Procedure {

    JsonExportProcedure(
        MldbEngine * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        procedureConfig = config.params.convert<JsonExportProcedureConfig>();
    }

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(procedureConfig, run);
        SqlExpressionMldbScope context(engine);
        filter_ostream out(runProcConf.dataFileUrl);
        JsonWriter json(out, runProcConf.prettyPrint, runProcConf.asArray, runProcConf.lineBreaks);
        ConvertProgressToJson convertProgressToJson(onProgress);
        auto boundDataset = runProcConf.exportData.stm->from->bind(context, convertProgressToJson);

        vector<shared_ptr<SqlExpression> > calc;
        BoundSelectQuery bsq(runProcConf.exportData.stm->select,
                            *boundDataset.dataset,
                            boundDataset.asName,
                            runProcConf.exportData.stm->when,
                            *runProcConf.exportData.stm->where,
                            runProcConf.exportData.stm->orderBy,
                            calc);

        const auto columnNames = bsq.getSelectOutputInfo()->allAtomNames();

        bool isFirst = true;
        auto outputJsonLine = [&] (NamedRowValue & row,
                                   const vector<ExpressionValue> & calc)
        {
            StreamJsonPrintingContext context(json.stream);
            if (!isFirst) {
                if (runProcConf.asArray)
                    json << ',';
                json.endl();
            }
            isFirst = false;

            json << "{";
            bool first = true;
            for (auto & [key, value]: row.columns) {
                if (!first)
                    json << ",";
                first = false;

                json << '\"';
                if (key.isIndex())
                    json << key.toIndex();
                else if (key.hasStringView()) {
                    auto [start, length] = key.getStringView();
                    jsonEscape(start, length, json.stream);
                }
                else 
                    json << jsonEscape(key.toUtf8String().rawString());
                json << '\"';
                json << ':';

                value.extractJson(context);
            }
            json << '}';
            return true;
        };

        bsq.execute({outputJsonLine, false/*processInParallel*/},
                    runProcConf.exportData.stm->offset,
                    runProcConf.exportData.stm->limit,
                    convertProgressToJson);
        RunOutput output;
        return output;

    }

    virtual Any getStatus() const
    {
        return Any();
    }

    JsonExportProcedureConfig procedureConfig;
};

static RegisterProcedureType<JsonExportProcedure, JsonExportProcedureConfig>
regJsonExportProcedure(
    builtinPackage(),
    "Exports a dataset to a target location as a JSON",
    "procedures/JsonExportProcedure.md.html");

} // namespace MLDB


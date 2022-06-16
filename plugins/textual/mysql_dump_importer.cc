/* mysql_dump_importer.cc
   Francois Maillet, 19 janvier 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Importer for text files from the mysqldump command.
*/

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/utils/progress.h"
#include "mldb/types/any_impl.h"
#include "mldb/utils/for_each_line.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/sql/builtin_functions.h"
#include "mldb/base/per_thread_accumulator.h"
#include "mldb/base/parallel.h"
#include "mldb/arch/timers.h"
#include "mldb/base/parse_context.h"
#include "mldb/rest/cancellation_exception.h"
#include "mldb/core/dataset_scope.h"
#include "mldb/utils/log.h"
#include "mldb/base/thread_pool.h"
#include "mldb/base/scope.h"
#include "mldb/sql/tokenize.h"
#include "dataset_builder.h"
#include "sql_csv_scope.h"
#include "mldb/utils/possibly_dynamic_buffer.h"


using namespace std;



namespace MLDB {

namespace SqlParsing {

bool match_whitespace(ParseContext & ctx);
bool matchKeyword(ParseContext & context, const char * keyword);
void expectKeyword(ParseContext & context, const char * keyword);

} // namespace SqlParsing

using namespace SqlParsing;


/*****************************************************************************/
/* MYSQL DUMP IMPORTER                                                       */
/*****************************************************************************/

struct MysqlDumpImporterConfig : ProcedureConfig, public DatasetBuilderConfig {

    static constexpr const char * name = "import.mysqldump";

    Url dataFileUrl;

    int64_t limit = -1;
    int64_t offset = 0;
    bool ignoreBadLines = false;
};

DECLARE_STRUCTURE_DESCRIPTION(MysqlDumpImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(MysqlDumpImporterConfig);

MysqlDumpImporterConfigDescription::
MysqlDumpImporterConfigDescription()
{
    addField("dataFileUrl", &MysqlDumpImporterConfig::dataFileUrl,
             "URL to load text file from");

    addParent<DatasetBuilderConfig>();

    addField("limit", &MysqlDumpImporterConfig::limit,
             "Maximum number of lines to process");
    addField("offset", &MysqlDumpImporterConfig::offset,
             "Skip the first n lines.", int64_t(0));
    addField("ignoreBadLines", &MysqlDumpImporterConfig::ignoreBadLines,
             "If true, any line causing an error will be skipped. Any line "
             "with an invalid JSON object will cause an error.", false);
    addParent<ProcedureConfig>();
}

bool skipSqlWhitespace(ParseContext & context)
{
    bool result = false;
    while (context) {
        if (context.get_col() == 1 && context.match_literal("--")) {
            result = true;
            context.skip_line();
            continue;
        }
        if (isspace(*context)) {
            ++context;
            result = true;
            continue;
        }
        break;
    }
    return result;
}

void expectSqlWhitespace(ParseContext & context)
{
    if (!skipSqlWhitespace(context))
        context.exception("expected SQL whitespace");
}

Utf8String expectSqlName(ParseContext & context)
{
    skipSqlWhitespace(context);
    context.expect_literal('`');
    Utf8String result = context.expect_text("`");
    context.expect_literal('`');
    return result;
}

std::optional<Utf8String> matchSqlName(ParseContext & context)
{
    ParseContext::Revert_Token token(context);
    skipSqlWhitespace(context);
    std::string text;
    if (!context.match_literal('`') || !context.match_text(text, "`") || !context.match_literal('`'))
        return std::nullopt;
    token.ignore();
    return std::move(text);
}

std::vector<int> expectSqlTypeParameters(ParseContext & context, int num)
{
    skipSqlWhitespace(context);
    context.expect_literal('(');
    skipSqlWhitespace(context);

    std::vector<int> result;

    for (size_t i = 0;  i < num;  ++i) {
        int n = context.expect_int();
        result.push_back(n);
        skipSqlWhitespace(context);
        if (i != num - 1)
            context.expect_literal(',');
    }
    skipSqlWhitespace(context);
    context.expect_literal(')');

    return result;
}

std::tuple<Utf8String, CellValue::CellType, bool>
expectSqlType(ParseContext & context)
{
    skipSqlWhitespace(context);

    auto print_params = [] (const std::vector<int> & p) -> std::string
    {
        std::string result = "(";
        for (size_t i = 0;  i < p.size();  ++i) {
            if (i != 0)
                result += ",";
            result += std::to_string(p[i]);
        }
        return result;
    };

    auto notnull = [&] () -> bool
    {
        return context.match_literal(" NOT NULL");
    };

    if (context.match_literal("varchar")) {
        auto p = expectSqlTypeParameters(context, 1);
        return { "varchar" + print_params(p), CellValue::CellType::UTF8_STRING, notnull()};
    }
    if (context.match_literal("char")) {
        auto p = expectSqlTypeParameters(context, 1);
        return { "char" + print_params(p), CellValue::CellType::UTF8_STRING, notnull()};
    }
    else if (context.match_literal("datetime")) {
        return { "datetime", CellValue::CellType::TIMESTAMP, notnull()};
    }
    else if (context.match_literal("mediumtext")) {
        return { "mediumtext", CellValue::CellType::UTF8_STRING, notnull()};
    }
    else if (context.match_literal("double")) {
        auto p = expectSqlTypeParameters(context, 2);
        return { "double" + print_params(p), CellValue::CellType::FLOAT, notnull()};
    }
    else if (context.match_literal("int")) {
        auto p = expectSqlTypeParameters(context, 1);
        return { "int" + print_params(p), CellValue::CellType::INTEGER, notnull()};
    }
    else if (context.match_literal("tinyint")) {
        auto p = expectSqlTypeParameters(context, 1);
        return { "tinyint" + print_params(p), CellValue::CellType::INTEGER, notnull()};
    }
    else {
        context.exception("unknown sql type");
    }
    MLDB_THROW_UNIMPLEMENTED();
}

CellValue expectMysqlString(ParseContext & context)
{
    context.expect_literal('\'', "expected MySQL string starting with '");

    // MySQL escapes strings with backslashes not double quotes
    std::string val;
    val.reserve(128);
    bool hasUnicode = false;

    for (;;) {
        unsigned char c = *context;
        //cerr << "got char " << *context << endl;
        if (c > 127) {
            int n = utf8::internal::sequence_length(&c);
            for (size_t i = 0;  i < n;  ++i) {
                val += *context++;
            }
            hasUnicode = true;
        } else {
            ++context;
            if (c == '\\') {
                val += *context++;
            }
            else if (c == '\'') {
                break;
            }
            else val += c;
        }
    }

    return CellValue(val.data(), val.length(),
                        hasUnicode ? MLDB::StringCharacteristics::STRING_UNKNOWN
                                : MLDB::StringCharacteristics::STRING_IS_VALID_ASCII);
}

CellValue expectSqlLiteral(ParseContext & context)
{
    // MySQL escapes strings with backslashes not double quotes
    if (*context == '\'') {
        return expectMysqlString(context);
    }

    auto expr = SqlExpression::parse(context, 0 /* precedence */, true /* allow UTF 8*/);
    return expr->constantValue().getAtom();
}

CellValue expectSqlDefault(ParseContext & context)
{
    expectSqlWhitespace(context);
    context.expect_literal("DEFAULT");
    expectSqlWhitespace(context);    
    return expectSqlLiteral(context);

#if 0
    skipSqlWhitespace(context);
    if (context.match_literal("NULL")) {
        return CellValue();
    }
    else if (context.match_literal("''")) {
        return CellValue("");
    }
    context.exception("unknown default value for column");
#endif
}

std::vector<Utf8String> expectSqlNameList(ParseContext & context)
{
    skipSqlWhitespace(context);
    context.expect_literal('(');
    skipSqlWhitespace(context);

    std::vector<Utf8String> result;
    if (context.match_literal(')'))
        return result;

    for (;;) {
        result.emplace_back(expectSqlName(context));
        skipSqlWhitespace(context);
        if (context.match_literal(','))
            continue;
        else if (context.match_literal(')'))
            break;
        else context.exception("expected , or ) for column list");
    }

    return result;
  
}

struct SqlColumn {
    Utf8String name;
    CellValue def;
    CellValue::CellType type;
    Utf8String sqlType;
    bool notNull = false;

    static void parseString(CellValue & val, ParseContext & context)
    {
        skipSqlWhitespace(context);
        val = expectMysqlString(context);
    }

    static void parseStringOrNull(CellValue & val, ParseContext & context)
    {
        skipSqlWhitespace(context);
        switch (*context) {
            case '\'':
                val = expectMysqlString(context);
                return;
            case 'n':
            case 'N':
                expectKeyword(context, "null");
                val = CellValue();
                return;
        }
        context.exception("Expected MySQL string or NULL");
    }

    static void parseTimestamp(CellValue & val, ParseContext & context)
    {
        parseString(val, context);
        val = val.coerceToTimestamp();
        if (val.empty())
            context.exception("Expected MySQL timestamp");
    }

    static void parseTimestampOrNull(CellValue & val, ParseContext & context)
    {
        parseStringOrNull(val, context);
        if (val.empty())
            return;
        val = val.coerceToTimestamp();
        if (val.empty())
            context.exception("Expected MySQL timestamp");
    }

    static void parseFloat(CellValue & val, ParseContext & context)
    {
        skipSqlWhitespace(context);
        val = context.expect_double();
    }

    static void parseFloatOrNull(CellValue & val, ParseContext & context)
    {
        skipSqlWhitespace(context);
        if (*context == 'n' || *context == 'N') {
            // Could be NaN or NULL
            if (matchKeyword(context, "null")) {
                val = CellValue();
                return;
            }
        }
        val = context.expect_double();
    }

    static void parseInteger(CellValue & val, ParseContext & context)
    {
        skipSqlWhitespace(context);
        if (*context == '-') {
            // Negative (signed) number
            val = context.expect_long_long();
        }
        else {
            val = context.expect_unsigned_long_long();
        }
    }

    static void parseIntegerOrNull(CellValue & val, ParseContext & context)
    {
        skipSqlWhitespace(context);
        if (*context == 'n' || *context == 'N') {
            expectKeyword(context, "null");
            val = CellValue();
        }
        else if (*context == '-') {
            // Negative (signed) number
            val = context.expect_long_long();
        }
        else {
            val = context.expect_unsigned_long_long();
        }
    }

    static void parseSql(CellValue & val, ParseContext & context)
    {
        skipSqlWhitespace(context);
        val = expectSqlLiteral(context);
    }

    std::function<void (CellValue &, ParseContext &)> getParser() const
    {
        switch (type) {
            case CellValue::CellType::UTF8_STRING:
                return notNull ? parseString : parseStringOrNull;
            case CellValue::CellType::TIMESTAMP:
                return notNull ? parseTimestamp : parseTimestampOrNull;
            case CellValue::CellType::FLOAT:
                return notNull ? parseFloat : parseFloatOrNull;
            case CellValue::CellType::INTEGER:
                return notNull ? parseInteger : parseIntegerOrNull;
            default:
                return parseSql;
        }
    }
};

enum struct SqlKeyType {
    UNIQUE,
    PRIMARY,
    INDEX
};

struct SqlKey {
    SqlKeyType type;
    Utf8String name;
    std::vector<Utf8String> columns;
};

struct SqlSchema {
    Utf8String tableName;
    std::vector<SqlColumn> columns;
    std::vector<SqlKey> keys;
    std::string charset;
};

SqlSchema expectMysqlSchema(ParseContext & context)
{
    if (context.get_col() != 1)
        context.exception("Schema should start on new line");
    while (context) {
        if (context.match_eol())
            continue;  // skip empty line
        if (context.match_literal("--") || context.match_literal("/*!")) {
            context.skip_line();
            continue;  // skip comment
        }
        if (context.match_literal("DROP TABLE")) {
            context.skip_line();
            continue;
        }
        break;
    }

    //cerr << "done preamble" << endl;

    SqlSchema schema;

    context.expect_literal("CREATE TABLE");
    context.expect_whitespace();
    //cerr << "expect table" << endl;
    schema.tableName = expectSqlName(context);
    //cerr << "table name " << schema.tableName << endl;
    skipSqlWhitespace(context);
    context.expect_literal('(');
    skipSqlWhitespace(context);

    for (;;) {
        skipSqlWhitespace(context);
        if (auto nameo = matchSqlName(context)) {
            //cerr << "Column named " << *nameo << endl;
            SqlColumn col;
            col.name = std::move(*nameo);
            std::tie(col.sqlType, col.type, col.notNull) = expectSqlType(context);
            //cerr << "type " << col.sqlType << endl;
            col.def = expectSqlDefault(context);
            schema.columns.emplace_back(std::move(col));
        }
        else if (context.match_literal("PRIMARY KEY")) {
            //cerr << "PRIMARY KEY" << endl;
            SqlKey key;
            key.type = SqlKeyType::PRIMARY;
            key.name = "<<<PRIMARY>>>";
            key.columns = expectSqlNameList(context);
            schema.keys.emplace_back(std::move(key));
        }
        else if (context.match_literal("UNIQUE KEY")) {
            //cerr << "UNIQUE KEY" << endl;
            SqlKey key;
            key.type = SqlKeyType::UNIQUE;
            key.name = expectSqlName(context);
            key.columns = expectSqlNameList(context);
            schema.keys.emplace_back(std::move(key));
        }
        else if (context.match_literal("KEY")) {
            //cerr << "KEY" << endl;
            SqlKey key;
            key.type = SqlKeyType::INDEX;
            key.name = expectSqlName(context);
            key.columns = expectSqlNameList(context);
            schema.keys.emplace_back(std::move(key));
        }
        else {
            context.exception("unknown CREATE TABLE parameter");
        }
        skipSqlWhitespace(context);
        if (context.match_literal(','))
            continue;
        else if (context.match_literal(')'))
            break;
        context.exception("unknown create table command");
    }

    for (;;) {
        skipSqlWhitespace(context);
        if (context.match_literal(';'))
            break;
        else if (context.match_literal("ENGINE=")) {
            context.expect_text(" \n;");
        }
        else if (context.match_literal("DEFAULT CHARSET=")) {
            schema.charset = context.expect_text(" \n");
        }
        else if (context.match_literal("ROW_FORMAT=")) {
            context.expect_text(" \n;");
        }
        else context.exception("unknown create table trailing clause");
    }

    return schema;
}

struct MysqlDumpImporter: public Procedure {

    MysqlDumpImporter(MldbEngine * owner,
                      PolyConfig config_,
                      const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<MysqlDumpImporterConfig>();
    }

    MysqlDumpImporterConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);
        Progress progress;

        std::shared_ptr<Step> iterationStep = progress.steps({
            make_pair("iterating", "lines")
        });

        std::string filename = runProcConf.dataFileUrl.toDecodedString();
        filter_istream stream(filename, { { "mapped", "true" } });
        Date timestamp = stream.info().lastModified;

        Timer timer;

        DatasetBuilder builder;
        builder.initialize(*engine, logger, runProcConf, onProgress);

        Date zeroTs;

        std::atomic<int64_t> errors(0);
        std::atomic<int64_t> recordedLines(0);

        ParseContext parseContext(filename, stream);

        SqlSchema schema = expectMysqlSchema(parseContext);
        parseContext.match_eol();

        int64_t lineOffset = parseContext.get_line();
        uint64_t startOffset = parseContext.get_offset();

        stream = filter_istream(filename, { { "mapped", "true" } });
        for (size_t i = 0;  i < startOffset;  ++i)
            stream.get();

        //stream.seekg(startOffset);


        std::vector<ColumnPath> knownColumns;
        std::vector<std::function<void (CellValue &, ParseContext &)>> columnParsers;
        for (const auto & col: schema.columns) {
            knownColumns.emplace_back(col.name);
            columnParsers.emplace_back(col.getParser());
        }

        auto handleError = [&](const std::string & message,
                               int64_t lineNumber,
                               const std::string& line) {
            if (config.ignoreBadLines) {
                ++errors;
                return true;
            }

            throw AnnotatedException(400, "Error parsing SQL row: "
                                      + message,
                                      "filename", filename,
                                      "lineNumber", lineNumber,
                                      "line", line.substr(0, 1000));
        };

        struct ThreadAccum {
            /// Dataset builder
            DatasetBuilder * datasetBuilder = nullptr;

            /// Info used in the binding scope
            Date fileTimestamp;
            Utf8String dataFileUrl;

            /// Dataset builder bound to our current predictor
            BoundDatasetBuilder boundBuilder;

            /// List of known (fixed) columns.  The SqlCSVScope has a reference to
            /// this, so if we change it we need to reinitialize the recordScope.
            std::vector<ColumnPath> columns;

            /// Scope (specialized to the learned input distribution)
            SqlCsvScope recordScope;

            /// Recorder object for this thread that the dataset gives us
            /// to record into the dataset.
            DatasetBuilderChunkRecorder recorder;

            /// Lines done in this thread
            uint64_t linesDone = 0;
            
            /// Bytes done in this thread
            uint64_t bytesDone = 0;

            /// Have we been initialized yet?
            bool isInitialized = false;

            std::span<const ColumnPath> knownColumns;
            std::vector<std::function<void (CellValue &, ParseContext &)>> columnParsers;

            void initialize(DatasetBuilder & builder,
                            std::span<const ColumnPath> knownColumns,
                            std::vector<std::function<void (CellValue &, ParseContext &)>> columnParsers,
                            Date fileTimestamp,
                            Utf8String dataFileUrl)
            {
                this->knownColumns = knownColumns;
                this->columnParsers = std::move(columnParsers);
                this->datasetBuilder = &builder;
                this->fileTimestamp = fileTimestamp;
                this->dataFileUrl = std::move(dataFileUrl);
                this->bind();
                isInitialized = true;
            }

            void bind()
            {
                recordScope = SqlCsvScope(datasetBuilder->engine, knownColumns, fileTimestamp, dataFileUrl, false /* canHaveExtra */);
                this->boundBuilder = datasetBuilder->bind(recordScope, knownColumns);
                this->recorder = this->boundBuilder.newChunk(-1 /*chunkNumber*/); // TODO: figure out chunk number
                //cerr << "finished with bind()" << endl;
            }

            /// Start a new chunk
            void newChunk(int64_t chunkNumber)
            {
                recorder = boundBuilder.newChunk(chunkNumber);
            }

            /// Finish a chunk
            void finishChunk()
            {
                recorder.finish();
                recorder = {};
            }

            void recordLine(const char * line, size_t len,
                            Date timestamp, 
                            uint64_t lineNumber, uint64_t lineOffset)
            {
                PossiblyDynamicBuffer<CellValue> fixedValues(knownColumns.size());
 
                ParseContext pcontext(dataFileUrl.rawString(), line, line + len, lineNumber, lineOffset);

                SqlParsing::match_whitespace(pcontext);
                pcontext.match_literal(';');
                SqlParsing::match_whitespace(pcontext);
                if (!pcontext)
                    return;
                if (pcontext.match_literal("LOCK TABLE"))
                    return;

                if (!pcontext.match_literal("INSERT INTO") && !pcontext.match_literal("NSERT INTO"))
                    pcontext.exception("expected INSERT INTO statement");
                expectSqlWhitespace(pcontext);
                expectSqlName(pcontext);
                expectSqlWhitespace(pcontext);
                pcontext.expect_literal("VALUES");

                int numRowsInLine = 0;
                for (;;) {
                    pcontext.skip_whitespace();
                    pcontext.expect_literal('(');
                    //static std::mutex mutex;
                    //std::unique_lock guard { mutex };
                    //const char * c = line + pcontext.get_offset();
                    for (size_t i = 0;  i < fixedValues.size();  ++i) {
                        if (i != 0)
                            pcontext.expect_literal(',');
                        fixedValues[i] = expectSqlLiteral(pcontext);
                        //if (knownColumns[i] == PathElement("url"))
                        //cerr << knownColumns[i] << " = " << fixedValues[i] << endl;
                        //columnParsers[i](fixedValues[i], pcontext);
                    }
                    //cerr << endl;
                    pcontext.expect_literal(')');

                    auto rowScope = recordScope.bindRow(fixedValues.data(), fixedValues.size(), nullptr, 0,
                                                        timestamp, lineNumber, lineOffset);

                    recorder.recordRow(rowScope, fixedValues, {} /*extraValues*/, lineNumber);
                    ++numRowsInLine;
                    if (pcontext.match_literal(','))
                        continue;
                    else if (pcontext.match_literal(';'))
                        break;
                    else pcontext.exception("unknown continuation after record");
                }

                linesDone += numRowsInLine;
                bytesDone += len;
                //cerr << "got " << numRowsInLine << " rows in line at offset " << lineOffset << endl;
            }
        };

        PerThreadAccumulator<ThreadAccum> accum;

        auto startChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                auto & threadAccum = accum.get();
                if (!threadAccum.isInitialized) {
                    threadAccum.initialize(builder, knownColumns, columnParsers, timestamp,
                                           runProcConf.dataFileUrl.toDecodedUtf8String());
                }
                threadAccum.newChunk(chunkNumber);
                return true;
            };

        auto doneChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                auto & threadAccum = accum.get();
                threadAccum.finishChunk();
                return true;
            };

        bool keepGoing = true;
        mutex progressMutex;

        atomic<ssize_t> lineCount(0);
        atomic<ssize_t> byteCount(0);

        auto onLine = [&] (const char * line,
                           size_t lineLength,
                           int64_t blockNumber,
                           int64_t lineNumber)
        {
            //cerr << "got line " << lineNumber << " with " << lineLength << " characters" << endl;
            auto & threadAccum = accum.get();

            threadAccum.linesDone += 1;
            threadAccum.bytesDone += lineLength + 1;

            if (threadAccum.linesDone > 100 || threadAccum.bytesDone > 65536) {
                byteCount += threadAccum.bytesDone;
                uint64_t linesDone
                    = lineCount.fetch_add(threadAccum.linesDone)
                    + threadAccum.linesDone;

                if (linesDone % PROGRESS_RATE_LOW < PROGRESS_RATE_LOW) {
                    iterationStep->value = linesDone;
                    onProgress(jsonEncode(iterationStep));
                }
                
                // Look for the wraparound of the modulus
                if (linesDone % 100000 < threadAccum.linesDone) {
                    double wall = timer.elapsed_wall();
                    INFO_MSG(this->logger)
                        << "done " << linesDone << " in " << wall
                        << "s at " << linesDone / wall * 0.000001
                        << "M lines/second on "
                        << timer.elapsed_cpu() / timer.elapsed_wall()
                        << " CPUs";
                }
                threadAccum.bytesDone = 0;
                threadAccum.linesDone = 0;
            }

            uint64_t actualLineNum = lineNumber + lineOffset;

            try {
                threadAccum.recordLine(line, lineLength, timestamp, actualLineNum, lineOffset);
            } catch (const std::exception & exc) {
                return handleError(exc.what(), actualLineNum, string(line, lineLength));
            }

            int numLines = recordedLines.fetch_add(1);
            if (numLines % PROGRESS_RATE_LOW == 0) {
                lock_guard<mutex> l(progressMutex);
                if (numLines > iterationStep->value) {
                    iterationStep->value = numLines;
                }
                keepGoing = onProgress(jsonEncode(progress));
            }

            return keepGoing;
        };

        forEachLineBlock(stream, onLine, runProcConf.limit, numCpus(),
                         startChunk, doneChunk, newLineSplitter);
        if (!keepGoing) {
            throw MLDB::CancellationException("Procedure import.mysqldump cancelled");
        }

        double wall = timer.elapsed_wall();
        INFO_MSG(logger)
            << "imported " << lineCount << " in " << wall
            << "s at " << lineCount / wall * 0.000001
            << "M lines/second on "
            << timer.elapsed_cpu() / timer.elapsed_wall() << " CPUs";
        INFO_MSG(logger)
            << "done " << byteCount * 0.000001 << " megabytes at "
            << byteCount / timer.elapsed_wall() * 0.000001 << " megabytes/sec";
        INFO_MSG(logger) << "processed " << lineCount << " lines";

        DEBUG_MSG(logger) << "committing dataset";

        timer.restart();

        builder.commit();

        INFO_MSG(logger) << "Committing took " << timer.elapsed();

        Json::Value result;
        result["rowCount"] = (int64_t)recordedLines;
        result["numLineErrors"] = (int64_t)errors;
        return RunOutput(result);
    }

    virtual Any getStatus() const
    {
        return Any();
    }

    MysqlDumpImporterConfig procConfig;
};

static RegisterProcedureType<MysqlDumpImporter, MysqlDumpImporterConfig>
regJSON(builtinPackage(),
        "Import a text file with one JSON per line into MLDB",
        "procedures/MysqlDumpImporter.md.html");


} // namespace MLDB


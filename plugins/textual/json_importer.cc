/* json_importer.cc
   Francois Maillet, 19 janvier 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Importer for text files containing a JSON per line
*/

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/utils/progress.h"
#include "mldb/vfs/filter_streams.h"
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
#include "mldb/types/json_parsing_impl.h"

#define SIMDJSON_DEVELOPMENT_CHECKS 1
#include "mldb/ext/simdjson.h"

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* JSON IMPORTER                                                             */
/*****************************************************************************/

struct JSONImporterConfig : ProcedureConfig {

    static constexpr const char * name = "import.json";

    Url dataFileUrl;
    PolyConfigT<Dataset> outputDataset = DefaultType("tabular");

    int64_t limit = -1;
    int64_t offset = 0;
    bool ignoreBadLines = false;
    SelectExpression select = SelectExpression::STAR;
    std::shared_ptr<SqlExpression> where = SqlExpression::TRUE;
    std::shared_ptr<SqlExpression> named = SqlExpression::TRUE;
    JsonArrayHandling arrays = PARSE_ARRAYS;
    bool oneRecordPerLine = true;
};

DECLARE_STRUCTURE_DESCRIPTION(JSONImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(JSONImporterConfig);

JSONImporterConfigDescription::
JSONImporterConfigDescription()
{
    addField("dataFileUrl", &JSONImporterConfig::dataFileUrl,
             "URL to load text file from");
    addField("outputDataset", &JSONImporterConfig::outputDataset,
             "Configuration for output dataset",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("limit", &JSONImporterConfig::limit,
             "Maximum number of lines to process");
    addField("offset", &JSONImporterConfig::offset,
             "Skip the first n lines.", int64_t(0));
    addField("ignoreBadLines", &JSONImporterConfig::ignoreBadLines,
             "If true, any line causing an error will be skipped. Any line "
             "with an invalid JSON object will cause an error.", false);
    addField("select", &JSONImporterConfig::select,
             "Which columns to use.",
             SelectExpression::STAR);
    addField("where", &JSONImporterConfig::where,
             "Which lines to use to create rows.",
             SqlExpression::TRUE);
    addField("named", &JSONImporterConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name and that names cannot be objects.",
             SqlExpression::parse("lineNumber()"));
    addField("arrays", &JSONImporterConfig::arrays,
            "Describes how arrays are encoded in the JSON output.  For "
            "''parse' (default), the arrays become structured values. "
            "For 'encode', "
            "arrays containing atoms are sparsified with the values "
            "representing one-hot "
            "keys and boolean true values", PARSE_ARRAYS);
    addAuto("oneRecordPerLine", &JSONImporterConfig::oneRecordPerLine,
            "Data file is newline-separated with exactly one record per line.  Used to"
            "speed record splitting logic.");

    addParent<ProcedureConfig>();

    onPostValidate = [] (JSONImporterConfig * config,
                         JsonParsingContext & context)
    {
        if (config->dataFileUrl.empty()) {
            throw AnnotatedException(
                400,
                "dataFileUrl is a required property and must not be empty");
        }
    };
}

struct JsonRowScope : public SqlRowScope {
    JsonRowScope(const ExpressionValue & expr, ssize_t lineNumber)
        : expr(expr), lineNumber(lineNumber) {}
    const ExpressionValue & expr;
    ssize_t lineNumber;
};

/* BlockSplitter that splits on the newline character. */
struct JsonSplitterState {

};

bool skipJson(ParseContext & context);

struct JsonSplitter: public BlockSplitterT<JsonSplitterState> {
    JsonSplitter(std::string filename,
                 int startLine,
                 int startChar)
        : filename(std::move(filename)), startLine(startLine), startChar(startChar)
    {
    }

    std::string filename;
    int startLine;
    int startChar;

    virtual std::pair<const char *, JsonSplitterState>
    nextBlockT(const char * block1, size_t n1, const char * block2, size_t n2,
               const JsonSplitterState & state) const override
    {
        auto truncated = [] () -> std::pair<const char *, JsonSplitterState>
        {
            return { nullptr, {} };
        };

        ExcAssertEqual(n2, 0);
        StringJsonParsingContext context(block1, n1, filename);
        try {
            MLDB_TRACE_EXCEPTIONS(false);
            if (!skipJson(*context.context))
                return truncated();
            //context.skip();
            return { block1 + context.context->get_offset(), {} };
        }
        catch (const std::exception & exc) {
            return truncated();
        }


        //cerr << "trying block with " << n1 << " bytes at " << (void *)block1 << endl;
        //cerr << "first char is " << (int)block1[0] << endl;
        cerr << string(block1, std::min<size_t>(n1, 100)) << endl;
        using namespace simdjson;
        ondemand::parser parser;
        padded_string_view document(block1, n1, n1 + SIMDJSON_PADDING);
        size_t length;

#if 0
        auto docs = parser.iterate_many(document);
        if (docs.error()) {
            cerr << "documents error: " << docs.error() << endl;
            MLDB_THROW_UNIMPLEMENTED();
        }

        auto it = docs.value().begin();
        auto parsed = *it;

#else
        auto parsed = parser.iterate(document);
        length = parsed.current_location() - document.data();
#endif
        cerr << "parsed.error() = " << parsed.error() << endl;
        if (!parsed.error()) {
            //auto begin = parsed.begin();
            //auto end = parsed.end();
            //auto location = parsed.current_location();
            //auto source = it.source().data();
            //auto length = it.source().size();
            cerr << "length " << length << endl;

            return { block1 + length, {} };
        }

        if (parsed.error()) {


            cerr << "Parsing error: " << parsed.error() << endl;
            //MLDB_THROW_UNIMPLEMENTED();

            switch (parsed.error()) {
                // Fatal errrors... not caused by a truncated document
                case simdjson::error_code::CAPACITY:                   ///< This parser can't support a document that big
                case simdjson::error_code::UNSUPPORTED_ARCHITECTURE:   ///< unsupported architecture
                case simdjson::error_code::MEMALLOC:                   ///< Error allocating memory, most likely out of memory
                case simdjson::error_code::TAPE_ERROR:                 ///< Something went wrong while writing to the tape (stage 2), this is a generic error
                case simdjson::error_code::DEPTH_ERROR:                ///< Your document exceeds the user-specified depth limitation
                case simdjson::error_code::IO_ERROR:                   ///< Error reading a file
                case simdjson::error_code::INVALID_JSON_POINTER:       ///< Invalid JSON pointer reference
                case simdjson::error_code::INVALID_URI_FRAGMENT:       ///< Invalid URI fragment
                case simdjson::error_code::UNEXPECTED_ERROR:           ///< indicative of a bug in simdjson
                case simdjson::error_code::PARSER_IN_USE:              ///< parser is already in use.
                case simdjson::error_code::INSUFFICIENT_PADDING:       ///< The JSON doesn't have enough padding for simdjson to safely parse it.
                case simdjson::error_code::OUT_OF_BOUNDS:              ///< Attempted to access location outside of document.
                case simdjson::error_code::INCORRECT_TYPE:             ///< JSON element has a different type than user expected
                case simdjson::error_code::INDEX_OUT_OF_BOUNDS:        ///< JSON array index too large
                case simdjson::error_code::NO_SUCH_FIELD:              ///< JSON field not found in object
                case simdjson::error_code::OUT_OF_ORDER_ITERATION:     ///< tried to iterate an array or object out of order
                case simdjson::error_code::UNINITIALIZED:              ///< unknown error, or uninitialized document
                    throw MLDB::Exception("parsing of document: %s", simdjson::error_message(parsed.error()));

                case simdjson::error_code::STRING_ERROR:               ///< Problem while parsing a string
                case simdjson::error_code::T_ATOM_ERROR:               ///< Problem while parsing an atom starting with the letter 't'
                case simdjson::error_code::F_ATOM_ERROR:               ///< Problem while parsing an atom starting with the letter 'f'
                case simdjson::error_code::N_ATOM_ERROR:               ///< Problem while parsing an atom starting with the letter 'n'
                case simdjson::error_code::NUMBER_ERROR:               ///< Problem while parsing a number
                case simdjson::error_code::UTF8_ERROR:                 ///< the input is not valid UTF-8
                case simdjson::error_code::UNESCAPED_CHARS:            ///< found unescaped characters in a string.
                case simdjson::error_code::UNCLOSED_STRING:            ///< missing quote at the end
                case simdjson::error_code::NUMBER_OUT_OF_RANGE:        ///< JSON number does not fit in 64 bits
                case simdjson::error_code::INCOMPLETE_ARRAY_OR_OBJECT: ///< The document ends early.
                case simdjson::error_code::SCALAR_DOCUMENT_AS_VALUE:   ///< A scalar document is treated as a value.
                    return truncated();

                case simdjson::error_code::EMPTY:                      ///< no structural element found
                    //cerr << "index " << it.current_index() << endl;
                    //cerr << "current_location " << (void *)parsed.current_location().value() << endl;
                    return truncated();

                case simdjson::error_code::SUCCESS:
                    MLDB_THROW_LOGIC_ERROR("success value in switch");
                case simdjson::error_code::NUM_ERROR_CODES:
                    MLDB_THROW_LOGIC_ERROR("Got guard value; are SIMDJson library and headers out of sync?");
            }

            // This will happen if we update the simdjson library and new error
            // codes are created
            const char * msg = simdjson::error_message(parsed.error());
            MLDB_THROW_UNIMPLEMENTED("parsing of new SIMDJson error code %s", msg);
        }
        //auto begin = parsed.begin();
        //auto end = parsed.end();
        //simdjson::error_code::  code;
        //parsed.end();
        auto end = parsed.current_location();
        if (end.error()) {
            cerr << "End error: " << end.error() << endl;

            MLDB_THROW_UNIMPLEMENTED("TODO");
        }
        return { end.value(), {} };
    }

    virtual size_t requiredBlockPadding() const override { return simdjson::SIMDJSON_PADDING; }
};

/*****************************************************************************/
/* STRUCTURED JSON PARSING CONTEXT                                           */
/*****************************************************************************/

/** This allows an already parsed generic JSON object to be presented to
    a JSON parser to interpret its contents.
*/
struct SimdJsonParsingContext: public JsonParsingContext {

    SimdJsonParsingContext(simdjson::ondemand::document & doc)
        : doc(doc)
    {
        values.reserve(64);
        values.emplace_back(doc.get_value());
    }

    simdjson::ondemand::document & doc;
    mutable std::vector<simdjson::ondemand::value> values; 

    virtual void exception(const std::string & message) const
    {
        throw MLDB::Exception(message);
    }
    
    virtual std::string getContext() const
    {
        return "";
    }

    virtual int expectInt()
    {
        return values.back().get_int64().value();
    }

    virtual unsigned int expectUnsignedInt()
    {
        return values.back().get_uint64().value();
    }

    virtual long expectLong()
    {
        return values.back().get_int64().value();
    }

    virtual unsigned long expectUnsignedLong()
    {
        return values.back().get_uint64().value();
    }

    virtual long long expectLongLong()
    {
        return values.back().get_int64().value();
    }

    virtual unsigned long long expectUnsignedLongLong()
    {
        return values.back().get_uint64().value();
    }

    virtual float expectFloat()
    {
        return values.back().get_double().value();
    }

    virtual double expectDouble()
    {
        return values.back().get_double().value();
    }

    virtual bool expectBool()
    {
        return values.back().get_bool().value();
    }

    virtual void expectNull()
    {
        if (!values.back().is_null())
            exception("expected null");
    }

    virtual bool matchUnsignedLongLong(unsigned long long & val)
    {
        if (!values.back().is_integer())
            return false;
        val = expectUnsignedLongLong();
        return true;
    }

    virtual bool matchLongLong(long long & val)
    {
        if (!values.back().is_integer())
            return false;
        val = expectLongLong();
        return true;
    }

    virtual bool matchDouble(double & val)
    {
        return values.back().get(val) == simdjson::error_code::SUCCESS;
    }

    virtual std::string expectStringAscii()
    {
        return std::string(values.back().get_string().value());
    }

    virtual ssize_t expectStringAscii(char * value, size_t maxLen)
    {
        auto str = values.back().get_string().value();
        if (str.length() > maxLen)
            exception("ASCII string too long for buffer");
        Utf8String result(str.data(), str.length(), false /* check */);
        if (!result.isAscii())
            exception("Expected ASCII string");
        memcpy(value, result.rawData(), str.length());
        return str.length();
    }

    virtual Utf8String expectStringUtf8()
    {
        auto str = values.back().get_string().value();
        Utf8String result(str.data(), str.length(), false /* check */);
        return result;
    }

    virtual ssize_t expectStringUtf8(char * value, size_t maxLen)
    {
        auto str = values.back().get_string().value();
        if (str.length() > maxLen)
            exception("ASCII string too long for buffer");
        Utf8String result(str.data(), str.length(), false /* check */);
        memcpy(value, result.rawData(), str.length());
        return str.length();
    }

    virtual Json::Value expectJson()
    {
        MLDB_THROW_UNIMPLEMENTED_ON_THIS("TODO");
    }

    virtual bool isObject() const
    {
        return values.back().type() == simdjson::ondemand::json_type::object;
    }

    virtual bool isString() const
    {
        return values.back().type() == simdjson::ondemand::json_type::string;
    }

    virtual bool isArray() const
    {
        return values.back().type() == simdjson::ondemand::json_type::array;
    }

    virtual bool isBool() const
    {
        return values.back().type() == simdjson::ondemand::json_type::boolean;
    }

    virtual bool isInt() const
    {
        return values.back().type() == simdjson::ondemand::json_type::number
            && values.back().get_number_type() == simdjson::ondemand::number_type::signed_integer;
    }

    virtual bool isUnsigned() const
    {
        return values.back().type() == simdjson::ondemand::json_type::number
            && values.back().get_number_type() == simdjson::ondemand::number_type::unsigned_integer;
    }

    virtual bool isNumber() const
    {
        return values.back().type() == simdjson::ondemand::json_type::number;
    }

    virtual bool isNull() const
    {
        return values.back().is_null();
    }

    virtual void skip()
    {
        // no-op
    }

    virtual void forEachMember(const std::function<void ()> & fn)
    {
        auto obj = values.back().get_object();
        if (obj.is_empty())
            return;
        values.emplace_back();
        Scope_Exit(values.pop_back());
        pushPath(string_view());
        Scope_Exit(this->popPath());

        for (auto it = obj.begin(), end = obj.end();  it != end;  ++it) {
            auto && element = *it;
            auto key = element.unescaped_key().value();
            replacePath(key);
            values.back() = element.value();
            fn();
        }
    }

    virtual void forEachElement(const std::function<void ()> & fn)
    {
        auto arr = values.back().get_array();
        if (arr.is_empty())
            return;
        values.emplace_back();
        Scope_Exit(values.pop_back());
        pushPath(0);
        Scope_Exit(this->popPath());

        size_t i = 0;
        for (auto element: arr) {
            replacePath(i++);
            values.back() = element.value();
            fn();
        }
    }

    virtual std::string printCurrent()
    {
        return "";
    }

    virtual bool eof() const
    {
        return !doc.is_alive();
    }
};


struct JsonScope : public SqlExpressionMldbScope {


    JsonScope(MldbEngine * engine) : SqlExpressionMldbScope(engine){}

    ColumnGetter doGetColumn(const Utf8String & tableName,
                                const ColumnPath & columnName) override
    {
        return {[=] (const SqlRowScope & scope, ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
            {
                const auto & row = scope.as<JsonRowScope>();
                const ExpressionValue * res =
                    row.expr.tryGetNestedColumn(columnName, storage, filter);
                if (res) {
                    return *res;
                }
                return storage = ExpressionValue();
            },
            std::make_shared<AtomValueInfo>()
        };
    }

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep) override
    {
        std::vector<KnownColumn> columnsWithInfo;

        auto exec = [=] (const SqlRowScope & scope, const VariableFilter & filter)
        {
            const auto & row = scope.as<JsonRowScope>();
            StructValue result;
            result.reserve(row.expr.rowLength());

            const auto onCol = [&] (const PathElement & columnName,
                                    const ExpressionValue & val)
            {
                const auto & newColName = keep(columnName);
                if (!newColName.empty()) {
                    result.emplace_back(newColName.front(), val);
                }
                return true;
            };
            row.expr.forEachColumn(onCol);
            result.shrink_to_fit();
            return result;
        };

        GetAllColumnsOutput result;
        result.exec = exec;
        result.info = std::make_shared<RowValueInfo>(std::move(columnsWithInfo),
                                                     SCHEMA_OPEN);
        return result;
    }

    BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope) override
    {
        if (functionName == "lineNumber") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                {
                    const auto & row = scope.as<JsonRowScope>();
                    return ExpressionValue(row.lineNumber,
                                           Date::negativeInfinity());
                },
                std::make_shared<IntegerValueInfo>()
            };
        }
        return SqlBindingScope::doGetFunction(tableName, functionName, args,
                                              argScope);
    }

};

struct JSONImporter: public Procedure {

    JSONImporter(MldbEngine * owner,
                 PolyConfig config_,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<JSONImporterConfig>();
    }

    JSONImporterConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);
        Progress progress;

        std::shared_ptr<Step> iterationStep = progress.steps({
            make_pair("iterating", "lines")
        });

        // Create the output dataset
        std::shared_ptr<Dataset> outputDataset;

        if (runProcConf.outputDataset.type == "tabular") {
            if (runProcConf.outputDataset.params == nullptr) {
                 Json::Value params;
                 params["unknownColumns"] = "add";
                 runProcConf.outputDataset.params = params;
            }
            else {
                auto params =
                    runProcConf.outputDataset.params.as<Json::Value>();
                if (!params.isMember("unknownColumns")) {
                    params["unknownColumns"] = "add";
                    runProcConf.outputDataset.params = params;
                }
            }
        }
        outputDataset = createDataset(engine, runProcConf.outputDataset,
                                      onProgress, true);

        if(!outputDataset) {
            throw MLDB::Exception("Unable to obtain output dataset");
        }

        Date zeroTs;

        std::atomic<int64_t> errors(0);
        std::atomic<int64_t> recordedLines(0);
        int64_t lineOffset = 1;
        std::string line;
        std::string filename = runProcConf.dataFileUrl.toDecodedString();

        filter_istream stream(filename, { { "mapped", "true" } });

        cerr << "stream size is " << stream.info().size << endl;

        bool isArray = false;
        bool separatedWithNewlines = false;
        bool oneRecordPerLine = false;

        ParseContext parseContext(filename, stream);
        StreamingJsonParsingContext jsonContext(parseContext);

        // Scan the beginning of the file, trying to figure out how it's organized
        // In particular, look for the first JSON marker to be an array opening
        // which would indicate that the file is organized as a big array

        jsonContext.skipJsonWhitespace();

        Json::Value firstRecord;
        ssize_t startChar = parseContext.get_offset();

        if (parseContext.eof()) {
            // no data in file
        }
        else if (parseContext.match_literal('[')) {
            cerr << "*** starts with array" << endl;
            // Starts with an array, assume for now it's an array document
            isArray = true;
            jsonContext.skipJsonWhitespace();
            if (parseContext.match_literal(']')) {
                cerr << "*** array has ended" << endl;
                // starts with an empty array... either an empty file or one per line
                bool hasNewlineBefore = jsonContext.hasEmbeddedNewlines;
                jsonContext.skipJsonWhitespace();
                if (parseContext.eof()) {
                    // Nothing to do... just an empty array
                    isArray = false;
                }
                else if (!hasNewlineBefore && jsonContext.hasEmbeddedNewlines) {
                    // Not an array; there is a newline separator
                    isArray = false;
                }
                else {
                    cerr << "junk char is " << (int)*parseContext << endl;
                    throw MLDB::Exception("Excess junk at end of JSON array");
                }
            }
            else {
                startChar = parseContext.get_offset();
            }
        }
        else {
            startChar = parseContext.get_offset();
            jsonContext.hasEmbeddedNewlines = false;
            firstRecord = jsonContext.expectJson();
            bool hadEmbeddedNewlines = jsonContext.hasEmbeddedNewlines;
            jsonContext.skipJsonWhitespace();
            if (jsonContext.hasEmbeddedNewlines && !hadEmbeddedNewlines)
                oneRecordPerLine = true;
        }

        cerr << "firstRecord " << firstRecord << endl;
#if 0
        while (stream) {
            int c = stream.peek();
            char discard;
            if (c == '[') {
                isArray = true;
                stream.get(discard);
                break;
            }
            if (c == '\n') {
                stream.get(discard);
                continue;
            }
            if (!isspace(c)) {
                break;
            }
        };

        {
            ParseContext parseContext(filename, stream);
            StreamingJsonParsingContext jsonContext(parseContext);
            auto firstRecord = jsonContext.expectJson();
            cerr << "firstRecord = " << firstRecord << endl;
            if (isArray) {
                if (jsonContext.eof()) {
                    throw MLDB::Exception("Truncated JSON array");
                }
            }
            else {
                bool hasNewlineInternal = jsonContext.hasEmbeddedNewlines;
                jsonContext.skipJsonWhitespace();
                separatedWithNewlines = (!hasNewlineInternal) && jsonContext.hasEmbeddedNewlines;
            }
        
            oneRecordPerLine = oneRecordPerLine && (!jsonContext.hasEmbeddedNewlines);
        }
#endif

        cerr << "isArray = " << isArray << endl;
        cerr << "separatedWithNewlines = " << separatedWithNewlines << endl;
        cerr << "oneRecordPerLine = " << oneRecordPerLine << endl;
        cerr << "startChar = " << startChar << endl;

        if (stream.isRandomSeekable()) {
            stream.seekg(startChar);
        }
        else {
            cerr << "*** NOT RANDOM SEEKABLE" << endl;
            cerr << "seeking to " << startChar << endl;
            stream = filter_istream(filename, { { "mapped", "true" } });
            if (startChar != 0) {
                stream.seekg(startChar);
                ExcAssertEqual(stream.tellg(), startChar);
            }
        }

        //while (parseContext) {
        //    parseContext.skip_whitespace();
        //    //char next = *parseContext;
        //}

        //size_t endChar = stream.tellg();

        Date timestamp = stream.info().lastModified;

        Timer timer;

        if (config.offset != 0)
            MLDB_THROW_UNIMPLEMENTED_ON_THIS("non-zero offsets");

        auto handleError = [&](const std::string & message,
                               int64_t lineNumber,
                               const std::string& line) {
            if (config.ignoreBadLines) {
                ++errors;
                return true;
            }

            throw AnnotatedException(400, "Error parsing JSON row: "
                                      + message,
                                      "filename", filename,
                                      "lineNumber", lineNumber,
                                      "line", line);
        };

        Dataset::MultiChunkRecorder recorder
            = outputDataset->getChunkRecorder();

        struct ThreadAccum {
            /// Recorder object for this thread that the dataset gives us
            /// to record into the dataset.
            std::unique_ptr<Recorder> threadRecorder;

            /// Lines done in this thread
            uint64_t linesDone = 0;
            
            /// Bytes done in this thread
            uint64_t bytesDone = 0;

            // JSON parser for this thread
            simdjson::ondemand::parser parser;
        };

        PerThreadAccumulator<ThreadAccum> accum;

        auto startChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                auto & threadAccum = accum.get();
                threadAccum.threadRecorder = recorder.newChunk(chunkNumber);
                return true;
            };

        auto doneChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                auto & threadAccum = accum.get();
                ExcAssert(threadAccum.threadRecorder.get());
                threadAccum.threadRecorder->finishedChunk();
                threadAccum.threadRecorder.reset(nullptr);
                return true;
            };

        bool useSelect = config.select != SelectExpression::STAR;
        bool useWhere = config.where != SqlExpression::TRUE;

        // using incorrect default value to ease check
        bool useNamed = config.named != SqlExpression::TRUE;

        JsonScope jsonScope(engine);
        const auto whereBound = config.where->bind(jsonScope);
        const auto selectBound = config.select.bind(jsonScope);
        const auto namedBound = config.named->bind(jsonScope);
        bool keepGoing = true;
        mutex progressMutex;

        std::atomic<int64_t> lastLine = -1;

        atomic<ssize_t> lineCount(0);
        atomic<ssize_t> byteCount(0);

        auto onLine = [&] (const char * line,
                           size_t lineLength,
                           int64_t blockNumber,
                           int64_t lineNumber)
        {
            //cerr << "got line " << lineNumber << " with " << lineLength << " characters" << endl;
            const char * e = line + lineLength;
            while (line < e && isspace(*line)) {
                ++line;
            }
            if (line < e && isArray && *line == ']') {
                // closing of the array
                if (lastLine != -1 && lastLine != lineNumber - 1) {
                    //cerr << "lastLine = " << lastLine << endl;
                    //cerr << "lineNumber = " << lineNumber << endl;
                    throw MLDB::Exception("JSON array closed multiple times");
                }
                for (++line; line < e; ++line) {
                    if (!isspace(*line))
                        throw MLDB::Exception("Extra junk at end of JSON array");
                }
                lastLine = lineNumber - 1;
                return true;
            }

            if (lastLine != -1 && lineNumber > lastLine) {
                throw MLDB::Exception("Excess junk at end of JSON array");
            }

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

            // MLDB-1111 empty lines are treated as error
            if(lineLength == 0)
                return handleError("empty line", actualLineNum, "");

            ExpressionValue expr;
            StreamingJsonParsingContext parser(filename, line, lineLength,
                                            actualLineNum);

            if (true) {
                size_t len = e - line;
                //cerr << "parsing line of length " << len << endl;
                try {
                    simdjson::padded_string_view lineView(line, len, len + simdjson::SIMDJSON_PADDING);
                    auto parsed = threadAccum.parser.iterate(lineView);
                    //cerr << "parsing error: " << parsed.error() << endl;
                    SimdJsonParsingContext context(parsed.value());
                    expr = ExpressionValue::parseJson(context, timestamp, config.arrays);
                    //cerr << "returned: " << expr << endl;
                } catch (const std::exception & exc) {
                    return handleError(exc.what(), actualLineNum, string(line, lineLength));
                }
            }
            else {
                skipJsonWhitespace(*parser.context);
                if (parser.context->eof()) {
                    return handleError("empty line", actualLineNum, "");
                }

                try {
                    expr = ExpressionValue::parseJson(parser, timestamp,
                                                    config.arrays);
                } catch (const std::exception & exc) {
                    return handleError(exc.what(), actualLineNum, string(line, lineLength));
                }

                if (isArray) {
                    skipJsonWhitespace(*parser.context);
                    if (parser.context->match_literal(',')) ;
                    else if (parser.context->match_literal(']')) {
                        if (lastLine != -1 && lastLine != lineNumber) {
                            throw MLDB::Exception("multiple last lines (without comma separators)");
                        }
                        lastLine = lineNumber;
                    }
                }

                skipJsonWhitespace(*parser.context);
                if (!parser.context->eof()) {
                    return handleError("extra characters at end of line", actualLineNum, "");
                }
            }

            RowPath rowName(actualLineNum);
            ExpressionValue storage;
            const ExpressionValue * selectOutput = &expr;

            if (useWhere || useSelect || useNamed) {
                JsonRowScope row(expr, actualLineNum);
                if (useWhere) {
                    if (!whereBound(row, storage, GET_ALL).isTrue()) {
                        return true;
                    }
                }

                if (useNamed) {
                    rowName = RowPath(
                        namedBound(row, storage, GET_ALL).toUtf8String());
                }

                if (useSelect) {
                    selectOutput = &selectBound(row, storage, GET_ALL);
                }
            }

            int numLines = recordedLines.fetch_add(1);
            if (numLines % PROGRESS_RATE_LOW == 0) {
                lock_guard<mutex> l(progressMutex);
                if (numLines > iterationStep->value) {
                    iterationStep->value = numLines;
                }
                keepGoing = onProgress(jsonEncode(progress));
            }

            if (selectOutput == &expr)
                threadAccum.threadRecorder->recordRowExprDestructive(
                    std::move(rowName), std::move(expr));
            else if (selectOutput == &storage)
                threadAccum.threadRecorder->recordRowExprDestructive(
                    std::move(rowName), std::move(storage));
            else threadAccum.threadRecorder->recordRowExpr(std::move(rowName), *selectOutput);

            return keepGoing;
        };

        JsonSplitter jsonSplitter(filename, 0, 0);
        const auto  & splitter = oneRecordPerLine
            ? (const BlockSplitter &)newLineSplitter
            : (const BlockSplitter &)jsonSplitter;

        forEachLineBlock(stream, onLine, runProcConf.limit, numCpus(),
                         startChunk, doneChunk, splitter);
        if (!keepGoing) {
            throw MLDB::CancellationException("Procedure import.json cancelled");
        }

        if (isArray && lastLine == -1) {
            throw MLDB::Exception("Whole-file JSON array never terminated");
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

        recorder.commit();

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

    JSONImporterConfig procConfig;
};

static RegisterProcedureType<JSONImporter, JSONImporterConfig>
regJSON(builtinPackage(),
        "Import a text file with one JSON per line into MLDB",
        "procedures/JSONImporter.md.html");


} // namespace MLDB


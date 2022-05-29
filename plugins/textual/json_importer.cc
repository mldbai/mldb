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
#include "dataset_builder.h"
#include "sql_csv_scope.h"
#include "mldb/utils/lightweight_hash.h"
#include "mldb/sql/decompose.h"
#include "mldb/utils/possibly_dynamic_buffer.h"

//#define SIMDJSON_DEVELOPMENT_CHECKS 1
#include "mldb/ext/simdjson.h"

using namespace std;



namespace MLDB {


/*****************************************************************************/
/* JSON IMPORTER                                                             */
/*****************************************************************************/

struct JSONImporterConfig : ProcedureConfig, public DatasetBuilderConfig {

    static constexpr const char * name = "import.json";

    Url dataFileUrl;

    int64_t limit = -1;
    int64_t offset = 0;
    bool ignoreBadLines = false;
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

    addParent<DatasetBuilderConfig>();

    addField("limit", &JSONImporterConfig::limit,
             "Maximum number of lines to process");
    addField("offset", &JSONImporterConfig::offset,
             "Skip the first n lines.", int64_t(0));
    addField("ignoreBadLines", &JSONImporterConfig::ignoreBadLines,
             "If true, any line causing an error will be skipped. Any line "
             "with an invalid JSON object will cause an error.", false);
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
               bool noMoreData, const JsonSplitterState & state) const override
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
            return { block1 + context.context->get_offset(), {} };
        }
        catch (const std::exception & exc) {
            // TODO: verify that context's position is at the end otherwise pass
            // on the exception
            return truncated();
        }
    }

    virtual std::span<const char> fixupBlock(std::span<const char> block) const
    {
        const char * p = block.data();
        const char * e = p + block.size();
        // Skip spaces and trailing commas that separate the records
        while (e > p && (isspace(e[-1]) || e[-1] == ','))
            --e;
        return { p, size_t(e - p) };
    }

    virtual size_t requiredBlockPadding() const override { return 0; }
};

/*****************************************************************************/
/* SIMDJSON PARSING CONTEXT                                                  */
/*****************************************************************************/

/** This allows an already parsed generic JSON object to be presented to
    a JSON parser to interpret its contents.
*/
struct SimdJsonParsingContext: public JsonParsingContext {

    SimdJsonParsingContext(simdjson::ondemand::document & doc)
        : doc(doc)
    {
        values.reserve(64);  // recursion depth
        values.emplace_back(doc.get_value());
    }

    simdjson::ondemand::document & doc;
    mutable std::vector<simdjson::ondemand::value> values; 

    virtual void exception(const Utf8String & message) const
    {
        throw MLDB::Exception(message);
    }

    virtual std::string getContext() const
    {
        return "";
    }

    virtual JsonNumber expectNumber()
    {
        using nt = simdjson::ondemand::number_type;
        JsonNumber result;
        switch (values.back().get_number_type()) {
            case nt::unsigned_integer:      result.type = JsonNumber::UNSIGNED_INT;   result.uns = values.back().get_uint64();  break;
            case nt::signed_integer:        result.type = JsonNumber::SIGNED_INT;     result.sgn = values.back().get_int64();   break;
            case nt::floating_point_number: result.type = JsonNumber::FLOATING_POINT; result.fp  = values.back().get_double();  break;
            default:
                exception("expected JSON number");
        }
        return result;
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

    virtual std::any savePosition()
    {
        MLDB_THROW_UNIMPLEMENTED_ON_THIS("TODO");
    }

    virtual void restorePosition(const std::any & token)
    {
        MLDB_THROW_UNIMPLEMENTED_ON_THIS("TODO");
    }
};

struct PredictiveExpressionValueParser {

    virtual ~PredictiveExpressionValueParser() = default;

    virtual bool apply(std::span<CellValue> fixed,
                       std::vector<std::pair<Path, CellValue>> & extra,
                       JsonParsingContext & context,
                       const Path & prefix, 
                       Date timestamp, JsonArrayHandling arrays) = 0;

    virtual PredictiveExpressionValueParser * clone() const = 0;

    virtual std::shared_ptr<PredictiveExpressionValueParser> learn()
    {
        return nullptr;
    }

    virtual size_t getFixedColumnCount() const
    {
        return 1;
    }

    virtual std::vector<ColumnPath> getFixedColumnNames() const
    {
        return { {} };
    } 

    virtual bool canReturnValue() const
    {
        return true;
    }

    virtual bool hasNestedColumns() const
    {
        return false;
    }

    static std::shared_ptr<PredictiveExpressionValueParser>
    create(std::span<CellValue> fixedValues,
           std::vector<std::pair<Path, CellValue>> & extraValues,
           JsonParsingContext & context,
           const Path & path,
           Date timestamp, JsonArrayHandling arrays);
};

struct StructureExpressionValueParser: public PredictiveExpressionValueParser {

    /// Information about a field we're parsing
    struct FieldEntry {
        /// Key we're expecting to find
        PathElement key;

        /// Specialized parser for when we encounter it
        std::shared_ptr<PredictiveExpressionValueParser> parser;

        /// Position of the output at which this field starts.  It will produce
        /// fields from startPosition to numFixedOutputFields.
        int startPosition = -1;

        /// How many fixed output fields do we produce?
        int numFixedOutputFields = -1;

        struct OutEdge {
            PathElement key;
            int fieldNumber = -1;
            uint32_t takenCount = 0;
        };

        /// Next possible fields, in order of probability.
        std::vector<OutEdge> outEdges;

        /// How many times have we seen it present
        uint32_t isPresentCount = 0;

        /// How many times did we successfully parse it?
        uint32_t parserSuccessCount = 0;

        void learn()
        {
            if (parser && parserSuccessCount < isPresentCount) {
                auto newParser = parser->learn();
                if (newParser)
                    parser = newParser;
                parserSuccessCount = isPresentCount;
            }

            auto compareEdges = [] (const OutEdge & e1, const OutEdge & e2)
            {
                return e1.takenCount > e2.takenCount;   
            };

            std::sort(outEdges.begin(), outEdges.end(), compareEdges);
        }
    };

    /// Graph origin/start node (doesn't have a key or a position, just out edges)
    FieldEntry origin;

    /// Graph of all nodes (the fields)
    std::vector<FieldEntry> fields;

    /// Number of fixed input fields (those for which we have assigned output fields)
    /// They are first in the fields array
    int numFixedInputFields = 0;

    /// The names of the fixed output fields we produce
    std::vector<ColumnPath> fixedOutputFields;

    /// Index of key name to order of encounter
    LightweightHash<uint64_t, int> keysIndex;

    int32_t numCalls = 0;
    int32_t numSuccesses = 0;
    int32_t numFailures = 0;
    int32_t extraFields = 0;
    int32_t missingFields = 0;

    virtual ~StructureExpressionValueParser() override
    {
        if (true || std::uncaught_exceptions())
            return;
        if (numCalls >= 0) {
            cerr << "calls: " << numCalls << " successes: " << numSuccesses << " failures: " << numFailures
                 << " extraFields: " << extraFields << " missingFields: " << missingFields << endl;
        }
        dump();
    }

    void dump()
    {
        for (int i = -1;  i < (int)fields.size();  ++i) {
            auto & field = (i == -1 ? origin : fields[i]);
            cerr << "  " << i << ": field " << field.key << " has " << field.outEdges.size() << " edges" << endl;
            if (i != -1)
                cerr << "    from " << field.startPosition << " for " << field.numFixedOutputFields << endl;
            for (auto & [name, to, count]: field.outEdges) {
                cerr << "    " << name << " to " << to << " : " << count << endl;
            }
        }
    }

    virtual PredictiveExpressionValueParser * clone() const override
    {
        auto result = std::make_unique<StructureExpressionValueParser>(*this);
        for (auto & field: result->fields) {
            field.parser.reset(field.parser->clone()); 
        }
        return result.release();
    }

    virtual bool canReturnValue() const override
    {
        return false;
    }

    virtual bool hasNestedColumns() const override
    {
        return true;
    }

    virtual bool apply(std::span<CellValue> fixed,
                       std::vector<std::pair<Path, CellValue>> & extra,
                       JsonParsingContext & context,
                       const Path & prefix, 
                       Date timestamp, JsonArrayHandling arrays) override
    {
        //cerr << "apply at " << context.printPath() << " with " << endl;
        ++numCalls;

        if (fixed.size() != fixedOutputFields.size()) {
            static std::mutex mutex;
            std::unique_lock guard{mutex};
            cerr << "fixed.size() = " << fixed.size()
                 << " fixedOutputFields.size() = " << fixedOutputFields.size()
                 << " prefix = " << prefix << endl;
            dump();
        }
        ExcAssertEqual(fixed.size(), fixedOutputFields.size());

        bool success = true;

        // Node number we're currently at
        int32_t current = -1;

        auto onMember = [&] ()
        {
            auto fieldName = context.fieldNameView();

            //cerr << "member " << fieldName << " current " << current << endl;

            auto & currentField = current == -1 ? origin : fields[current];
            int next = -1;

            // Look for an out edge to a known destination
            for (size_t i = 0;  i < currentField.outEdges.size();  ++i) {
                auto & [key, fieldNumber, takenCount] = currentField.outEdges[i];
                if (MLDB_LIKELY(fieldName == key)) {
                    next = fieldNumber;
                    ++takenCount;
                    if (MLDB_UNLIKELY(i > 0) && takenCount > currentField.outEdges[i-1].takenCount + 128) {
                        // Badly un-sorted taken counts are a failure
                        success = false;
                    }
                    break;
                }
            }

            //cerr << "next = " << next << endl;
            bool alreadyParsed = false;

            // Didn't find it.  We have a new (or rare) edge.  Look it up in the
            // index and add it to the list of out edges.
            if (MLDB_UNLIKELY(next == -1)) {
                success = false;
                auto fieldNameElement = PathElement(fieldName);
                auto fieldNameHash = fieldNameElement.hash();
                auto it = keysIndex.find(fieldNameHash);
                if (it == keysIndex.end()) {
                    //cerr << "not in keys" << endl;
                    // Unhappy path... we have a new field, so the destination node is unknown
                    // Add the new node
                    FieldEntry newField;
                    newField.key = fieldName;
                    std::vector<CellValue> newFixed;
                    std::vector<std::pair<Path, CellValue>> newExtra;
                    newField.parser = PredictiveExpressionValueParser::create(newFixed, newExtra, context, prefix + fieldNameElement, timestamp, arrays);
                    alreadyParsed = true;
                    newField.isPresentCount = 1;
                    newField.parserSuccessCount = 1;

                    //cerr << "newExtra = " << jsonEncode(newExtra) << endl;

                    // No position in output, it gets added to the end
                    newField.startPosition = -1;
                    next = fields.size();
                    if (!keysIndex.insert({fieldNameHash, next}).second)
                        MLDB_THROW_LOGIC_ERROR("Key hash");
                    fields.emplace_back(std::move(newField));

                    for (auto & [path, value]: newExtra) {
                        extra.emplace_back(std::move(path), std::move(value));
                    }
                }
                else {
                    // TODO: verify no hash collision
                    next = it->second;
                }

                // We may have reallocated fields above, so we need to get a new reference
                auto & currentField = current == -1 ? origin : fields[current];

                //cerr << "new edge from " << current << " to " << fieldName << ":" << next
                //     << " which had " << currentField.outEdges.size() << " edges" << endl;

                // Record the new edge
                currentField.outEdges.push_back({fieldName, next, 1 /* takenCount */});
            }

            auto & nextField = fields[next];
            ExcAssertEqual(nextField.key, fieldName);
            int startPosition = nextField.startPosition;
            if (startPosition == -1 && !alreadyParsed) {
                ExcAssert(!alreadyParsed);
                // We don't yet have an assigned place
                // Create one, and add to the extra fields
                nextField.numFixedOutputFields = nextField.parser->getFixedColumnCount();
                PossiblyDynamicBuffer<CellValue> fixed(nextField.numFixedOutputFields);
                //cerr << "doing " << nextField.numFixedOutputFields << " fields for " << nextField.key << endl;
                nextField.parser->apply(fixed, extra, context, prefix + nextField.key, timestamp, arrays);
                //cerr << jsonEncode(extra) << endl;
                if (fixed.size() != 0) {
                    //cerr << "fixed.size() = " << fixed.size() << endl;
                    //cerr << "extra.size() = " << extra.size() << endl;
                    auto fieldNames = nextField.parser->getFixedColumnNames();
                    ExcAssertEqual(fieldNames.size(), nextField.numFixedOutputFields);
                    for (size_t i = 0;  i < nextField.numFixedOutputFields;  ++i) {
                        //cerr << "adding to extra " << prefix << " . " << fieldNames[i] << " = " << fixed[i] << endl;
                        extra.emplace_back(prefix + nextField.key + std::move(fieldNames[i]), std::move(fixed[i]));
                    }
                }
                alreadyParsed = true;
            }

            if (!alreadyParsed) {
                try {
                    bool fieldSuccess = nextField.parser
                        ->apply(fixed.subspan(startPosition, nextField.numFixedOutputFields),
                                extra, context, prefix + nextField.key, timestamp, arrays);
                    nextField.isPresentCount += 1;
                    nextField.parserSuccessCount += fieldSuccess;
                    success = success && fieldSuccess;
                } catch (...) {
                    cerr << "startPosition = " << startPosition << endl;
                    cerr << "nextField.numFixedOutputFields = " << nextField.numFixedOutputFields << endl;
                    cerr << "nextField.parser->getFixedColumnCount = " << nextField.parser->getFixedColumnCount() << endl;
                    throw;
                }
            }

            //cerr << "set " << name << " to " << value << endl;

            current = next;
        };

        context.forEachMember(onMember);

        (success ? numSuccesses : numFailures) += 1;

        return success;
    }

    virtual size_t getFixedColumnCount() const override
    {
        return fixedOutputFields.size();
    }

    virtual std::vector<ColumnPath> getFixedColumnNames() const override
    {
        return fixedOutputFields;
    }

    virtual std::shared_ptr<PredictiveExpressionValueParser> learn() override
    {
        if (numFailures == 0)
            return nullptr;  // nothing to learn

        cerr << "learning over " << fields.size() << " fields with " << numCalls << " calls" << endl;

        // Learn each of them individually
        origin.learn();
        for (auto & field: fields) {
            field.learn();
        }

        // Sort the keys so we can insert things in the right order
        std::vector<std::pair<PathElement, int>> sortedKeys;
        int i = 0;
        for (auto & field: fields) {
            sortedKeys.emplace_back(field.key, i++);
        }

        std::sort(sortedKeys.begin(), sortedKeys.end());

        fixedOutputFields.clear();
        for (size_t i = 0;  i < sortedKeys.size();  ++i) {
            auto & [key, index] = sortedKeys[i];
            auto & field = fields[index];
            field.startPosition = fixedOutputFields.size();
            if (field.parser->canReturnValue()) {
                fixedOutputFields.emplace_back(key);
            }
            if (field.parser->hasNestedColumns()) {
                auto columnNames = field.parser->getFixedColumnNames();
                for (auto & c: columnNames) {
                    fixedOutputFields.emplace_back(key + std::move(c));
                }
            }
            field.numFixedOutputFields = fixedOutputFields.size() - field.startPosition;
        }

        numFixedInputFields = fields.size();

        numSuccesses += numFailures;
        numFailures = 0;

        return nullptr;  // we learned in place, so we return nullptr
    }

    static std::shared_ptr<PredictiveExpressionValueParser>
    create(std::span<CellValue> fixedValues,
           std::vector<std::pair<Path, CellValue>> & extraValues,
           JsonParsingContext & context,
           const Path & prefix,
           Date timestamp, JsonArrayHandling arrays)
    {
        auto result = std::make_shared<StructureExpressionValueParser>();
        ExcAssertEqual(extraValues.size(), 0);
        result->apply(fixedValues, extraValues, context, prefix, timestamp, arrays);
        ExcAssertGreater(result->numFailures, 0);
        ExcAssert(fixedValues.empty());
        auto learnt = result->learn();
        std::sort(extraValues.begin(), extraValues.end());
        auto resultOut = learnt ? std::move(learnt) : std::move(result);
        // Debug
        {
            //auto names = resultOut->getFixedColumnNames();
            //cerr << "prefix = " << prefix << endl;
            //cerr << "names = " << jsonEncodeStr(names) << endl;
            //cerr << "extraValues = " << jsonEncode(extraValues) << endl;
            //ExcAssertEqual(extraValues.size(), names.size());
            //for (size_t i = 0;  i < extraValues.size();  ++i) {
            //    ExcAssertEqual(extraValues[i].first.hasSuffix(names[i]));
            //}
        }

        return resultOut;
    }

};

struct ArrayExpressionValueParser: public PredictiveExpressionValueParser {

    virtual ~ArrayExpressionValueParser() = default;

    virtual PredictiveExpressionValueParser * clone() const override
    {
        return new ArrayExpressionValueParser(*this);
    }

    virtual bool apply(std::span<CellValue> fixed,
                       std::vector<std::pair<Path, CellValue>> & extra,
                       JsonParsingContext & context,
                       const Path & prefix, 
                       Date timestamp, JsonArrayHandling arrays) override
    {
        ExcAssertEqual(fixed.size(), 0);
        ExpressionValue expr = ExpressionValue::parseJson(context, timestamp, arrays);
        auto onAtom = [&] (auto && path, auto && value, Date timestamp) -> bool
        {
            //static std::mutex mutex;
            //std::unique_lock guard{mutex};
            //cerr << "array atom " << prefix << " " << path << " " << value << endl;
            extra.emplace_back(prefix + std::move(path), std::move(value));
            return true;
        };
        expr.forEachAtomDestructive(onAtom);
        return true;
    }

    virtual std::shared_ptr<PredictiveExpressionValueParser> learn() override
    {
        return nullptr;
    }

    virtual size_t getFixedColumnCount() const override
    {
        return 0;
    }

    virtual std::vector<ColumnPath> getFixedColumnNames() const override
    {
        return {};
    } 

    virtual bool canReturnValue() const override
    {
        return false;
    }

    virtual bool hasNestedColumns() const override
    {
        return true;
    }

    static std::shared_ptr<PredictiveExpressionValueParser>
    create(std::span<CellValue> fixedValues,
           std::vector<std::pair<Path, CellValue>> & extraValues,
           JsonParsingContext & context,
           const Path & prefix,
           Date timestamp, JsonArrayHandling arrays)
    {
        auto result = std::make_shared<ArrayExpressionValueParser>();
        ExcAssertEqual(extraValues.size(), 0);
        result->apply(fixedValues, extraValues, context, prefix, timestamp, arrays);
        ExcAssert(fixedValues.empty());
        auto learnt = result->learn();
        auto resultOut = learnt ? std::move(learnt) : std::move(result);
        return resultOut;
    }
};

namespace {
void learnPredictor(std::shared_ptr<PredictiveExpressionValueParser> & predictor)
{
    if (!predictor)
        return;
    auto newPredictor = predictor->learn();
    if (newPredictor)
        predictor = std::move(newPredictor);
}
}

struct ScalarExpressionValueParser: public PredictiveExpressionValueParser {

    virtual PredictiveExpressionValueParser * clone() const override
    {
        auto result = std::make_unique<ScalarExpressionValueParser>(*this);
        if (result->arrays)
            result->arrays.reset(result->arrays->clone());
        if (result->objects)
            result->objects.reset(result->objects->clone());
        return result.release();
    }

    virtual std::shared_ptr<PredictiveExpressionValueParser> learn() override
    {
        learnPredictor(arrays);
        learnPredictor(objects);
        return nullptr;
    }

    virtual bool apply(std::span<CellValue> values, std::vector<std::pair<Path, CellValue>> & extra,
                       JsonParsingContext & context, const Path & prefix,
                       Date timestamp, JsonArrayHandling arrays) override
    {
        if (context.isArray()) {
            if (!this->arrays) {
                this->arrays = ArrayExpressionValueParser::create(values.subspan(0, 0), extra, context, prefix, timestamp, arrays);
            }
            else {
                this->arrays->apply(values.subspan(0,0), extra, context, prefix, timestamp, arrays);
            }
        }
        else if (context.isObject()) {
            if (!objects) {
                objects = ArrayExpressionValueParser::create(values.subspan(0,0), extra, context, prefix, timestamp, arrays);
            }
            else {
                objects->apply(values.subspan(0,0), extra, context, prefix, timestamp, arrays);
            }
        }
        else {
            static const auto desc = getDefaultDescriptionSharedT<CellValue>();
            CellValue val;
            desc->parseJsonTyped(&val, context);
            if (values.size() == 1) {
                values[0] = std::move(val);
            }
            else if (values.size() == 0) {
                extra.emplace_back(prefix, std::move(val));
            }
            else {
                MLDB_THROW_LOGIC_ERROR("scalar with too much storage");
            }
        }

        return true;  // it matched
    }

    static std::shared_ptr<PredictiveExpressionValueParser>
    create(std::span<CellValue> fixed,
           std::vector<std::pair<Path, CellValue>> & extra,
           JsonParsingContext & context, const Path & prefix,
           Date timestamp, JsonArrayHandling arrays)
    {
        auto result = std::make_shared<ScalarExpressionValueParser>();
        result->apply(fixed, extra, context, prefix, timestamp, arrays);
        return result;
    }

    virtual bool canReturnValue() const override
    {
        return true;
    }

    virtual bool hasNestedColumns() const override
    {
        return false;  //!!arrays || !!objects;
    }

    virtual size_t getFixedColumnCount() const override
    {
        return 0;
    }

    virtual std::vector<ColumnPath> getFixedColumnNames() const override
    {
        return {};
    } 

    std::shared_ptr<PredictiveExpressionValueParser> objects;
    std::shared_ptr<PredictiveExpressionValueParser> arrays;
};

std::shared_ptr<PredictiveExpressionValueParser>
PredictiveExpressionValueParser::
create(std::span<CellValue> fixed, std::vector<std::pair<Path, CellValue>> & extra,
        JsonParsingContext & context, const Path & prefix,
        Date timestamp, JsonArrayHandling arrays)
{
    if (context.isObject()) {
        return StructureExpressionValueParser::create(fixed, extra, context, prefix, timestamp, arrays);
    }
    else if (context.isArray()) {
        return ArrayExpressionValueParser::create(fixed, extra, context, prefix, timestamp, arrays);
    }
    else {
        return ScalarExpressionValueParser::create(fixed, extra, context, prefix, timestamp, arrays);
    }
}

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

        DatasetBuilder builder;
        builder.initialize(*engine, logger, runProcConf, onProgress);

        Date zeroTs;

        std::atomic<int64_t> errors(0);
        std::atomic<int64_t> recordedLines(0);
        int64_t lineOffset = 1;
        std::string line;
        std::string filename = runProcConf.dataFileUrl.toDecodedString();

        filter_istream stream(filename, { { "mapped", "true" } });

        bool isArray = false;
        //bool separatedWithNewlines = false;
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
                jsonContext.hasEmbeddedNewlines = false;
                firstRecord = jsonContext.expectJson();
                bool hadEmbeddedNewlines = jsonContext.hasEmbeddedNewlines;
                jsonContext.skipJsonWhitespace();
                if (jsonContext.hasEmbeddedNewlines && !hadEmbeddedNewlines)
                    oneRecordPerLine = true;
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

        StructuredJsonParsingContext firstContext(firstRecord);
        std::vector<CellValue> fixed;
        std::vector<std::pair<ColumnPath, CellValue>> extra;
        auto predictor = PredictiveExpressionValueParser::
            create(fixed, extra, firstContext, {} /* prefix */, Date(), runProcConf.arrays);

        // Now we scan through the initial records to figure out the schema
        // This is only necessary because the tabular dataset infers its schema
        // from the first record only
        // Once that changes, we can stop doing this
        for (size_t i = 0;  i < 100 && !jsonContext.eof();  ++i) {
            fixed.resize(predictor->getFixedColumnCount());
            extra.clear();
            predictor->apply(fixed, extra, jsonContext, {}, Date(), runProcConf.arrays);
        }
        auto initialPredictor = predictor->learn();
        if (!initialPredictor)
            initialPredictor = predictor;

        auto fixedColumns = initialPredictor->getFixedColumnNames();
        cerr << "Importing for " << fixedColumns.size() << " fixed columns: " << jsonEncodeStr(fixedColumns) << endl;

        cerr << "firstRecord " << firstRecord << endl;
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

        struct ThreadAccum {
            /// JSON parser for this thread
            simdjson::ondemand::parser parser;

            /// Dataset builder
            DatasetBuilder * datasetBuilder = nullptr;

            /// Info used in the binding scope
            Date fileTimestamp;
            Utf8String dataFileUrl;

            /// Dataset builder bound to our current predictor
            BoundDatasetBuilder boundBuilder;

            /// Predictive structured JSON parser
            std::shared_ptr<PredictiveExpressionValueParser> predictor;

            /// Number of times called and successful for this predictor, as well
            /// as the number of failures until we attempt to re-learn
            uint32_t numCalls = 0, numSuccesses = 0, learningRate = 16;

            /// List of known (fixed) columns.  The SqlCSVScope has a reference to
            /// this, so if we change it we need to reinitialize the recordScope.
            std::vector<ColumnPath> knownColumns;

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

            void initialize(DatasetBuilder & builder,
                            std::span<const ColumnPath> knownColumns,
                            Date fileTimestamp,
                            Utf8String dataFileUrl,
                            const PredictiveExpressionValueParser & predictor)
            {
                this->datasetBuilder = &builder;
                this->fileTimestamp = fileTimestamp;
                this->dataFileUrl = std::move(dataFileUrl);
                this->predictor.reset(predictor.clone());
                this->bind();
                isInitialized = true;
            }

            void bind()
            {
                knownColumns = predictor->getFixedColumnNames();
                //cerr << "creating recordScope with " << knownColumns.size() << " known columns" << endl;
                recordScope = SqlCsvScope(datasetBuilder->engine, knownColumns, fileTimestamp, dataFileUrl, true /* canHaveExtra */);
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

            void learn()
            {
                //auto oldFixedColumns = predictor->getFixedColumnNames();
                auto newPredictor = predictor->learn();
                if (newPredictor)
                    predictor = std::move(newPredictor);
                numCalls = 0;
                numSuccesses = 0;
                learningRate *= 2;
                bind();
            }

            void recordLine(const char * line, size_t len, size_t capacity,
                            Date timestamp, JsonArrayHandling arrays,
                            uint64_t lineNumber, uint64_t lineOffset)
            {
                //cerr << endl << endl << "recording line " << lineNumber << endl;
                PossiblyDynamicBuffer<CellValue> fixedValues(predictor->getFixedColumnCount());
                std::vector<std::pair<Path, CellValue>> extraValues;

                simdjson::padded_string_view lineView(line, len, capacity);
                auto parsed = parser.iterate(lineView);
                //cerr << "parsing error: " << parsed.error() << endl;
                SimdJsonParsingContext context(parsed.value());
                bool success = predictor->apply(fixedValues, extraValues, context, {}, timestamp, arrays);
                numCalls += 1;
                numSuccesses += success && extraValues.empty();

                //expr = ExpressionValue::parseJson(context, timestamp, config.arrays);
                //cerr << "returned: " << expr << endl;

                //cerr << format("got %zd fixed and %zd extra values\n", fixedValues.size(), extraValues.size());

                //cerr << "fixedValues.size() = " << fixedValues.size() << endl;
                //cerr << "recordScope.columnNames.size() = " << recordScope.columnNames.size() << endl;

                auto rowScope = recordScope.bindRow(fixedValues.data(), fixedValues.size(),
                                                    extraValues.data(), extraValues.size(),
                                                    timestamp, lineNumber, lineOffset);

                recorder.recordRow(rowScope, fixedValues, extraValues, lineNumber);

                auto numFailures = numCalls - numSuccesses;
                if (numFailures >= learningRate) {
                    //cerr << "numFailures = " << numFailures
                    //        << " numCalls = " << numCalls << " learningRate = " << learningRate << endl;
                    // Re-learn the predictive parser
                    learn();
                }
            }
        };

        PerThreadAccumulator<ThreadAccum> accum;

        auto startChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                auto & threadAccum = accum.get();
                if (!threadAccum.isInitialized) {
                    threadAccum.initialize(builder, {} /* knownColumns */, timestamp,
                                           runProcConf.dataFileUrl.toDecodedUtf8String(),
                                           *initialPredictor);
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

            size_t len = e - line;
            try {
                threadAccum.recordLine(line, len, len + simdjson::SIMDJSON_PADDING, timestamp,
                                       config.arrays, actualLineNum, lineOffset);
            } catch (const std::exception & exc) {
                return handleError(exc.what(), actualLineNum, string(line, lineLength));
            }

#if 0
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
#endif

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

        JsonSplitter jsonSplitter(filename, 0, 0);
        const auto  & splitter = oneRecordPerLine
            ? (const BlockSplitter &)newLineSplitter
            : (const BlockSplitter &)jsonSplitter;

        forEachLineBlock(stream, onLine, runProcConf.limit, numCpus(),
                         startChunk, doneChunk, splitter);
        if (!keepGoing) {
            throw MLDB::CancellationException("Procedure import.json cancelled");
        }

        if (isArray && lastLine == -1 && runProcConf.limit == -1) {
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

    JSONImporterConfig procConfig;
};

static RegisterProcedureType<JSONImporter, JSONImporterConfig>
regJSON(builtinPackage(),
        "Import a text file with one JSON per line into MLDB",
        "procedures/JSONImporter.md.html");


} // namespace MLDB


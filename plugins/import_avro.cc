/** import_avro.cc
    Jeremy Barnes, 30 May 2017
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#include "import_avro.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/optional_description.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/jml/utils/hex_dump.h"
#include "mldb/base/thread_pool.h"
#include "mldb/vfs/compressor.h"
#include "mldb/arch/endian.h"
#include "mldb/server/dataset_context.h"

using namespace std;


namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(ImportAvroConfig);

ImportAvroConfigDescription::ImportAvroConfigDescription()
{
    addField("dataFileUrl", &ImportAvroConfig::dataFileUrl,
             "URL of the avro data to import");
    addAuto("outputDataset", &ImportAvroConfig::outputDataset,
            "Dataset to record the data into.");
    addAuto("limit", &ImportAvroConfig::limit,
            "Maximum number of records to process.");
    addAuto("offset", &ImportAvroConfig::offset,
            "Skip the first n records.");
    addAuto("select", &ImportAvroConfig::select,
            "Which columns to import.  Default is to import all.");
    addAuto("where", &ImportAvroConfig::where,
            "Which lines to use to create rows.");
    addAuto("named", &ImportAvroConfig::named,
            "Row name expression for output dataset. Note that each row "
            "must have a unique name.");
    addAuto("timestamp", &ImportAvroConfig::timestamp,
            "Expression for row timestamp.");

    addParent<ProcedureConfig>();
}

#if 0
/*****************************************************************************/
/* IMPORT AVRO PROCEDURE WORK INSTANCE                                       */
/* Manages all the temporary data and work to load a avro file               */
/*****************************************************************************/

struct ImportAvroProcedureWorkInstance
{
    ImportAvroProcedureWorkInstance(std::shared_ptr<spdlog::logger> logger)
        : logger(logger),
          lineOffset(1), // we start at line 1
          isAvroLine(false),
          areOutputColumnNamesKnown(true),
          separator(0),
          quote(0),
          replaceInvalidCharactersWith(-1),
          hasQuoteChar(false),
          isIdentitySelect(false),
          rowCount(0),
          numLineErrors(0)
    {
        
    }

    std::shared_ptr<spdlog::logger> logger;
    vector<ColumnPath> knownColumnNames;
    Lightweight_Hash<ColumnHash, int> columnIndex; //To check for duplicates column names
    int64_t lineOffset;
    // Column names in the CSV file.  This is distinct from the
    // output column names that will be created once parsing has
    // happened.
    vector<ColumnPath> inputColumnNames;
    bool isAvroLine;
    std::atomic<int> areOutputColumnNamesKnown;
    char separator;
    char quote;
    int replaceInvalidCharactersWith;
    Encoding encoding;
    bool hasQuoteChar = false;
    Date ts;
    bool isIdentitySelect;

    BoundSqlExpression whereBound;
    BoundSqlExpression selectBound;
    BoundSqlExpression namedBound;
    BoundSqlExpression timestampBound;

    size_t rowCount;
    uint64_t numLineErrors;

    /*    Load a avro file and filter according to the configuration  */
    void loadAvro(const ImportAvroConfig& config,
                  std::shared_ptr<Dataset> dataset,
                  MldbServer * server,
                  const std::function<bool (const Json::Value &)> & onProgress)
    {
        string filename = config.dataFileUrl.toDecodedString();

        // Ask for a memory mappable stream if possible
        filter_istream stream(config.dataFileUrl, { { "mapped", "true" } });

        // Get the file timestamp out
        ts = stream.info().lastModified;

        string header;

        if (config.delimiter.length() == 1) {
            separator = config.delimiter[0];
        }
        else if (config.delimiter.length() > 1) {
            throw HttpReturnException(400, "Separator string must have one character");
        }
        else if (config.quoter.length() > 0)
        {
            throw HttpReturnException(400, "Separator string must not be empty if we have a quoter string");
        }

        if (config.quoter.length() == 1) {
            quote = config.quoter[0];
            hasQuoteChar = true;
        }
        else if (config.quoter.length() > 1) {
            throw HttpReturnException(400, "Quoter string must have one character");
        }

        isAvroLine = config.quoter.empty() && config.delimiter.empty();

        if (!config.replaceInvalidCharactersWith.empty()) {
            if (config.replaceInvalidCharactersWith.length() != 1)
                throw HttpReturnException(400, "replaceInvalidCharactersWith string must have one character");
            replaceInvalidCharactersWith = *config.replaceInvalidCharactersWith.begin();
        }

        encoding = parseEncoding(config.encoding);

        RE2 skipLineRegex(config.skipLineRegex.initialized()
                          ? config.skipLineRegex.surface().rawData()
                          : "");

        if (isAvroLine) {
            //MLDB-1312 optimize if there is no delimiter: only 1 column
            if (config.headers.empty()) {
                inputColumnNames = { ColumnPath(config.autoGenerateHeaders ? 0 : "lineAvro") };
            }
            else if (config.headers.size() != 1) {
                throw HttpReturnException(
                    400,
                    "Custom CSV header must have exactly one element if there is "
                    "no delimiter");
            }
            else {
                inputColumnNames = { ColumnPath(config.headers[0]) };
            }
        }
        else {
            // Turn a string into a column name, depending upon how the plugin
            // is configured.
            auto parseColumnName = [&] (const Utf8String & str) -> ColumnPath
                {
                    if (str.empty())
                        return ColumnPath();

                    if (config.structuredColumnNames) {
                        return ColumnPath::parse(str);
                    }
                    else {
                        return ColumnPath(str);
                    }
                };

            if (config.headers.empty()) {

                vector<string> fields;

                // Read header line
                string prevHeader;
                while(true) {
                    std::getline(stream, header);

                    if (prevHeader.empty()
                        && config.skipLineRegex.initialized()
                        && RE2::FullMatch(header, skipLineRegex))
                        continue;

                    if(!prevHeader.empty()) {
                        prevHeader += ' ' + header;
                        header.assign(std::move(prevHeader));
                    }

                    try {
                        ParseConavro pconavro(filename,
                                               header.c_str(), header.length(), 1, 0);
                        fields = expect_csv_row(pconavro, -1, separator);
                        break;
                    }
                    catch (FileFinishInsideQuote & exp) {
                        if(config.allowMultiLines) {
                            prevHeader.assign(std::move(header));
                            continue;
                        }

                        throw exp;
                    }
                }

                if (config.autoGenerateHeaders) {
                    // Re-open stream
                    stream.open(config.dataFileUrl, { { "mapped", "true" } });
                    auto nfields = fields.size();
                    for (ssize_t i = 0; i < nfields; ++i) {
                        inputColumnNames.emplace_back(i);
                    }
                }
                else {
                    lineOffset += 1;
                    switch (encoding) {
                    case ASCII:
                        for (const auto & f: fields)
                            inputColumnNames.emplace_back(parseColumnName(f));
                        break;
                    case UTF8:
                        for (const auto & f: fields)
                            inputColumnNames.emplace_back(parseColumnName(Utf8String(f)));
                        break;
                    case LATIN1:
                        for (const auto & f: fields)
                            inputColumnNames.emplace_back(parseColumnName(Utf8String::fromLatin1(f)));
                        break;
                    };
                }
            }
            else {
                for (const auto & f: config.headers) {
                    inputColumnNames.emplace_back(parseColumnName(f));
                }
            }

            // MLDB-1649
            // A trailing comma on the header row should be accepted
            if (!inputColumnNames.empty() && inputColumnNames.back().empty())
                inputColumnNames.pop_back();
        }

        // Early check for duplicate column names in input
        Lightweight_Hash<ColumnHash, int> inputColumnIndex;
        for (unsigned i = 0;  i < inputColumnNames.size();  ++i) {
            const ColumnPath & c = inputColumnNames[i];
            ColumnHash ch(c);
            if (!inputColumnIndex.insert(make_pair(ch, i)).second)
                throw HttpReturnException(400, "Duplicate column name in CSV file",
                                          "columnName", c);
        }

        // Now we know the columns, we can bind our SQL expressions for the
        // select, where, named and timestamp parts of the expression.
        SqlCsvScope scope(server, inputColumnNames, ts,
                          Utf8String(config.dataFileUrl.toDecodedString()));

        selectBound = config.select.bind(scope);
        whereBound = config.where->bind(scope);
        namedBound = config.named->bind(scope);
        timestampBound = config.timestamp->bind(scope);

        // Do we have a "select *"?  In that case, we can perform various
        // optimizations to avoid calling into the SQL layer
        SqlExpressionDatasetScope noConavro(*dataset, ""); //needs a conavro because x.* is ambiguous
        isIdentitySelect = config.select.isIdentitySelect(noConavro);  

        // Figure out our output column names from the bound
        // select clause

        if (selectBound.info->getSchemaCompletenessRecursive() != SCHEMA_CLOSED) {
            areOutputColumnNamesKnown = false;
        }

        auto cols = selectBound.info->getKnownColumns();

        for (unsigned i = 0;  i < cols.size();  ++i) {
            const auto& col = cols[i];
            if (!col.valueInfo->isScalar())
                throw HttpReturnException
                    (400,
                     "Import select expression cannot have row-valued columns.",
                     "select", config.select,
                     "selectOutputInfo", selectBound.info,
                     "columnName", col.columnName);

            ColumnHash ch(col.columnName);
            if (!columnIndex.insert(make_pair(ch, i)).second)
                throw HttpReturnException(400, "Duplicate column name in select expression",
                                          "columnName", col.columnName);

            knownColumnNames.emplace_back(col.columnName);
        }

        if (isIdentitySelect)
            ExcAssertEqual(inputColumnNames, knownColumnNames);

        DEBUG_MSG(logger)
            << "reading " << inputColumnNames.size() << " columns "
            << jsonEncodeStr(inputColumnNames);

        DEBUG_MSG(logger)
            << "writing " << knownColumnNames.size() << " columns "
            << jsonEncodeStr(knownColumnNames);

        std::string line;

        // Skip those up to the offset
        for (size_t i = 0;  stream && i < config.offset;  ++i, ++lineOffset) {
            getline(stream, line);
        }

        loadAvroData(dataset, stream, config, scope, onProgress);
    }

    /*    Load, filter and format all lines and process them  */
    void
    loadAvroData(std::shared_ptr<Dataset> dataset,
                 std::istream& stream,
                 const ImportAvroConfig& config,
                 SqlCsvScope& scope,
                 const std::function<bool (const Json::Value &)> & onProgress)
    {
        Progress progress;
        std::shared_ptr<Step> iterationStep = progress.steps({
            make_pair("iterating", "lines"),
        });

        // Do we have a "where true'?  In that case, we don't need to
        // call the SQL parser
        bool isWhereTrue = config.where->isConstantTrue();

        // Do we have a "NAMED lineNumber()"?  In that case, we can short
        // circuit the evaluation of that expression.
        bool isNamedLineNumber = config.named->surface == "lineNumber()";

        std::atomic<uint64_t> numSkipped(0);
        std::atomic<uint64_t> totalLinesProcessed(0);

        Timer timer;

        auto handleError = [&](const std::string & message,
                               int64_t lineNumber,
                               int64_t columnNumber,
                               const std::string& line) {
            if (config.ignoreBadLines) {
                ++numSkipped;
                return true;
            }

            throw HttpReturnException(400, "Error parsing CSV row: "
                                      + message,
                                      "lineNumber", lineNumber,
                                      "columnNumber", columnNumber,
                                      "line", line);
        };


        atomic<ssize_t> lineCount(0);
        atomic<ssize_t> byteCount(0);
        auto onLine = [&] (const char * line,
                           size_t length,
                           int chunkNum,
                           int64_t lineNum)
        {
            byteCount += length + 1;
            if (++lineCount % PROGRESS_RATE == 0) {
                iterationStep->value = lineCount;
                onProgress(jsonEncode(iterationStep));
            }
            int64_t actualLineNum = lineNum + lineOffset;
#if 1
            uint64_t linesDone = totalLinesProcessed.fetch_add(1);

            if (linesDone && linesDone % 100000 == 0) {

                double wall = timer.elapsed_wall();
                INFO_MSG(this->logger)
                    << "done " << linesDone << " in " << wall
                    << "s at " << linesDone / wall * 0.000001
                    << "M lines/second on "
                    << timer.elapsed_cpu() / timer.elapsed_wall()
                    << " CPUs";
            }
#endif

            auto & threadAccum = accum.get();


            // Skip lines if we are asked to
            if (config.skipLineRegex.initialized()) {
                if (!threadAccum.skipLineRegex) {
                    threadAccum.skipLineRegex.reset(new RE2(config.skipLineRegex.surface().rawData()));
                }
                
                if (RE2::FullMatch(re2::StringPiece(line, length),
                                   *threadAccum.skipLineRegex))
                    return true;
            }

            // MLDB-1111 empty lines are treated as error
            if (length == 0)
                return handleError("empty line", actualLineNum, 0, "");


            // Values that come in from the CSV file
            // TODO: clang doesn't like a variable length array
            // here.  Find another way to allocate it on the
            // stack.
            PossiblyDynamicBuffer<CellValue> values(inputColumnNames.size());

            const char * lineStart = line;

            const size_t numInputColumn = inputColumnNames.size();

            const char * errorMsg
                    = parseFixedWidthCsvRow(line, length, values.data(),
                                            numInputColumn,
                                            separator, quote, encoding,
                                            replaceInvalidCharactersWith,
                                            isAvroLine,
                                            hasQuoteChar, logger,
                                            config.ignoreExtraColumns,
                                            config.processExcelFormulas);
            
                if (errorMsg) {
                    if(config.allowMultiLines) {
                        // check if we hit an error meaning we probably
                        // have a multiline error
                        if(errorMsg == unclosedQuoteError ||
                           errorMsg == notEnoughColsError) {
                            return false;
                        }
                    }

                    return handleError(errorMsg, actualLineNum,
                                           line - lineStart + 1,
                                           string(line, length));
                }

            auto row = scope.bindRow(values.data(), ts, actualLineNum,
                                     0 /* todo: chunk ofs */);

            ExpressionValue nameStorage;
            RowPath rowName;

            if (isNamedLineNumber) {
                rowName = Path(actualLineNum);
            }
            else {
                rowName = namedBound(row, nameStorage, GET_ALL).coerceToPath();
            }
            row.rowName = &rowName;
            
            // If it doesn't match the where, don't add it
            if (!isWhereTrue) {
                ExpressionValue storage;
                if (!whereBound(row, storage, GET_ALL).isTrue())
                    return true;
            }

            // Get the timestamp for the row
            Date rowTs = ts;
            ExpressionValue tsStorage;
            rowTs = timestampBound(row, tsStorage, GET_ALL)
                    .coerceToTimestamp().toTimestamp();

            //ExcAssert(!(isIdentitySelect && outputColumnNamesUnknown));

            if (isIdentitySelect) {
                // If it's a select *, we don't really need to run the
                // select clause.  We simply go for it.
                threadAccum.specializedRecorder(std::move(rowName),
                                                rowTs, values.data(),
                                                values.size(), {});
            }
            else {
                // TODO: optimization for
                // SELECT * excluding (...)

                ExpressionValue selectStorage;
                const ExpressionValue & selectOutput
                        = selectBound(row, selectStorage, GET_ALL);

                if (&selectOutput == &selectStorage) {
                    // We can destructively work with it
                    threadAccum.threadRecorder
                        ->recordRowExprDestructive(std::move(rowName),
                                                   std::move(selectStorage));
                    }
                    else {
                        // We don't own the output; we will need to copy
                        // it.
                        threadAccum.threadRecorder
                            ->recordRowExpr(std::move(rowName),
                                            selectOutput);
                }
            }

            return true;
        };


        if(!config.allowMultiLines) {
            forEachLineBlock(stream, onLine, config.limit,
                             numCpus() /* parallelism */,
                             startChunk, doneChunk);
        }
        else {
            // very simplistic and not efficient way of doing multi-line. we send
            // lines one by one to the 'onLine' function, and if
            // we get an error that probably is caused by a multi-
            // line string, we concat the current line with the next
            // one and try again. 
            startChunk(0, 0);

            string line;
            string t_line;
            string prevLine;
            int64_t lineNum = 0;
            while(getline(stream, line)) {
                // prepend previous line if we're tagging it along
                if(!prevLine.empty()) {
                    t_line.assign(std::move(line));
                    line.assign(std::move(prevLine));
                    line += ' ' + t_line;
                }

                if(!onLine(line.c_str(), line.size(),
                           0 /* chunkNum */, lineNum)) {
                    prevLine.assign(std::move(line));
                } else {
                    prevLine.erase();
                    lineNum++;
                }

                if(config.limit > 0 && lineNum >= config.limit)
                    break;
            }

            doneChunk(0, lineNum);
        }

        double wall = timer.elapsed_wall();
        INFO_MSG(logger)
            << "imported " << totalLinesProcessed << " in " << wall
            << "s at " << totalLinesProcessed / wall * 0.000001
            << "M lines/second on "
            << timer.elapsed_cpu() / timer.elapsed_wall() << " CPUs";
        INFO_MSG(logger)
            << "done " << byteCount * 0.000001 << " megabytes at "
            << byteCount / timer.elapsed_wall() * 0.000001 << " megabytes/sec";
        INFO_MSG(logger) << "processed " << totalLinesProcessed << " lines";
        
        recorder.commit();

        numLineErrors = numSkipped;
        rowCount = lineCount;
    }
};
#endif

/*****************************************************************************/
/* IMPORT AVRO PROCEDURE                                                     */
/*****************************************************************************/

ImportAvroProcedure::
ImportAvroProcedure(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->config = config.params.convert<ImportAvroConfig>();
}

Any
ImportAvroProcedure::
getStatus() const
{
    return Any();
}

struct AvroSchema;
struct AvroDataDictionary;
struct AvroField;
struct AvroType;

struct AvroParser {
    virtual ~AvroParser()
    {
    }

    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const = 0;

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const = 0;

    virtual bool isNull() const
    {
        return false;
    }
};

struct AvroNullParser: public AvroParser {
    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        return ExpressionValue();
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<EmptyValueInfo>();
    }

    virtual bool isNull() const
    {
        return true;
    }
};

static int64_t parseAvroLong(const char * & current,
                             const char * end)
{
    int64_t encoded = 0;

    for (int n = 0; /* break in loop */; ++n) {
        if (current >= end)
            throw HttpReturnException(400, "End of stream parsing Avro int");
        unsigned char c = *current++;
        uint64_t bits = ((unsigned char)c) & 127;
        encoded = encoded | (bits << 7*n);
        if (c & 128) continue;
        break;
    }
        
    return encoded << 63 /* sign */ | encoded >> 1 /* value */;
}

static int32_t parseAvroInt(const char * & current,
                            const char * end)
{
    int32_t encoded = 0;

    for (int n = 0; /* break in loop */; ++n) {
        if (current >= end)
            throw HttpReturnException(400, "End of stream parsing Avro int");
        unsigned char c = *current++;
        uint64_t bits = ((unsigned char)c) & 127;
        encoded = encoded | (bits << 7*n);
        if (c & 128) continue;
        break;
    }
        
    return encoded << 31 /* sign */ | encoded >> 1 /* value */;
}

struct AvroLongParser: public AvroParser {

    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        return ExpressionValue(CellValue(parseAvroLong(current, end)),
                               ts);
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<IntegerValueInfo>();
    }
};

struct AvroIntParser: public AvroParser {
    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        return ExpressionValue(CellValue(parseAvroInt(current, end)),
                               ts);
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<IntegerValueInfo>();
    }
};

struct AvroBooleanParser: public AvroParser {
    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        if (current >= end)
            throw HttpReturnException(400, "End of stream parsing Avro boolean");
        unsigned char c = *current++;
        if (c == 0)
            return ExpressionValue(CellValue(false), ts);
        else if (c == 1)
            return ExpressionValue(CellValue(true), ts);
        else {
            --current;
            throw HttpReturnException(400, "Invalid Avro boolean value");
        }
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<BooleanValueInfo>();
    }
};

struct AvroFloatParser: public AvroParser {
    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        if (current + 4 > end)
            throw HttpReturnException(400, "End of stream parsing Avro float");
        float_le val;
        std::memcpy(&val, current, 4);
        current += 4;
        return ExpressionValue(CellValue(val), ts);
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<Float32ValueInfo>();
    }
};

struct AvroDoubleParser: public AvroParser {
    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        if (current + 8 > end)
            throw HttpReturnException(400, "End of stream parsing Avro double");
        double_le val;
        std::memcpy(&val, current, 8);
        current += 8;
        return ExpressionValue(CellValue(val), ts);
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<Float64ValueInfo>();
    }
};

struct AvroStringParser: public AvroParser {
    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        int64_t length = parseAvroLong(current, end);
        //cerr << "parsing string with length " << length << endl;
        if (current + length > end)
            throw HttpReturnException(400, "End of stream parsing Avro string");

        //ML::hex_dump(current, length);

        Utf8String val(current, length, true /* check */);

        //cerr << "finished parsing string" << endl;

        ExpressionValue result(CellValue(val), ts);
        current += length;
        return result;
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<StringValueInfo>();
    }
};

struct AvroBytesParser: public AvroParser {
    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        int64_t length = parseAvroLong(current, end);
        if (current + length > end)
            throw HttpReturnException(400, "End of stream parsing Avro bytes");
        Utf8String val(current, current + length);
        auto result = ExpressionValue(CellValue::blob(current, length),
                                      ts);
        current += length;
        return result;
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<BlobValueInfo>();
    }
};

struct AvroUnionParser: public AvroParser {
    AvroUnionParser(std::vector<AvroType> fields,
                    AvroDataDictionary & dict);
    
    AvroUnionParser(std::vector<std::shared_ptr<const AvroParser> > parsers);

    std::vector<AvroType> options;  ///< For records
    std::vector<std::shared_ptr<const AvroParser> > optionParsers;

    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        long id = parseAvroLong(current, end);
        //cerr << "union option " << id << " of " << optionParsers.size()
        //     << endl;
        return optionParsers.at(id)->parseRow(current, end, ts);
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        std::vector<std::shared_ptr<ExpressionValueInfo> > subInfo;
        bool nullable = false;
        bool onlyAtoms = true;
        for (auto & p: optionParsers) {
            if (p->isNull()) {
                nullable = true;
                break;
            }
            subInfo.push_back(p->getInfo());
            if (!subInfo.back()->isScalar()) {
                onlyAtoms = false;
            }
        }

        if (nullable && onlyAtoms) {
            return std::make_shared<AtomValueInfo>();
        }

        return std::make_shared<AnyValueInfo>();
    }
};

struct AvroRecordParser: public AvroParser {
    AvroRecordParser(std::vector<AvroField> fields,
                     AvroDataDictionary & dict);

    std::vector<AvroField> fields;  ///< For records
    std::vector<std::pair<PathElement, std::shared_ptr<const AvroParser> > >
    fieldParsers;

    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override;

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override;
};

struct AvroArrayParser: public AvroParser {
    AvroArrayParser(AvroSchema items,
                    AvroDataDictionary & dict);

    std::shared_ptr<const AvroParser> itemParser;

    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        throw HttpReturnException(600, "not done array");
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        // If the underlying info is atoms, we say it's an
        // embedding.  Otherwise we say it's a row
        auto innerInfo = itemParser->getInfo();
        if (innerInfo->isScalar()) {
            return std::make_shared<EmbeddingValueInfo>
                (-1, innerInfo->getEmbeddingType());
        }
        
        return std::make_shared<UnknownRowValueInfo>();
    }
};

struct AvroMapParser: public AvroParser {
    AvroMapParser(AvroSchema values,
                  AvroDataDictionary & dict);

    std::shared_ptr<const AvroParser> valueParser;

    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        throw HttpReturnException(600, "not done map");
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<UnknownRowValueInfo>();
    }
};

struct AvroFixedParser: public AvroParser {
    AvroFixedParser(size_t width)
        : width(width)
    {
    }

    size_t width;

    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        if (current + width > end)
            throw HttpReturnException(400, "End of stream parsing Avro fixed");

        std::string bytes(current, current + width);
        
        return ExpressionValue(CellValue::blob(std::move(bytes)), ts);
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<BlobValueInfo>();
    }
};

struct AvroEnumParser: public AvroParser {
    AvroEnumParser(const std::vector<Utf8String> & symbols)
        : symbols(std::move(symbols))
    {
    }

    std::vector<Utf8String> symbols;

    virtual ExpressionValue
    parseRow(const char * & current,
             const char * end,
             Date ts) const override
    {
        int64_t index = parseAvroLong(current, end);
        return ExpressionValue(symbols.at(index), ts);
    }

    virtual std::shared_ptr<ExpressionValueInfo>
    getInfo() const override
    {
        return std::make_shared<StringValueInfo>();
    }
};

struct AvroDataDictionary {
    AvroDataDictionary()
    {
        types.emplace("null", std::make_shared<AvroNullParser>());
        types.emplace("int", std::make_shared<AvroIntParser>());
        types.emplace("long", std::make_shared<AvroLongParser>());
        types.emplace("boolean", std::make_shared<AvroBooleanParser>());
        types.emplace("float", std::make_shared<AvroFloatParser>());
        types.emplace("double", std::make_shared<AvroDoubleParser>());
        types.emplace("string", std::make_shared<AvroStringParser>());
        types.emplace("bytes", std::make_shared<AvroBytesParser>());
    }
    
    std::shared_ptr<const AvroParser>
    getParserForType(const std::string & typeName)
    {
        auto it = types.find(typeName);
        if (it == types.end())
            throw HttpReturnException(400, "Unknown Avro type " + typeName);
        return it->second;
    }

    std::map<std::string, std::shared_ptr<const AvroParser> > types;
};


enum AvroTypeEnum {
    AVRO_NULL,
    AVRO_BOOLEAN,
    AVRO_INT,
    AVRO_LONG,
    AVRO_FLOAT,
    AVRO_DOUBLE,
    AVRO_BYTES,
    AVRO_STRING,
    AVRO_RECORD,
    AVRO_ENUM,
    AVRO_ARRAY,
    AVRO_MAP,
    AVRO_UNION,
    AVRO_FIXED,
    AVRO_TYPE_NAME
};

DECLARE_ENUM_DESCRIPTION(AvroTypeEnum);

DEFINE_ENUM_DESCRIPTION(AvroTypeEnum);

AvroTypeEnumDescription::
AvroTypeEnumDescription()
{
    addValue("null", AVRO_NULL);
    addValue("boolean", AVRO_BOOLEAN);
    addValue("int", AVRO_INT);
    addValue("long", AVRO_LONG);
    addValue("float", AVRO_FLOAT);
    addValue("double", AVRO_DOUBLE);
    addValue("bytes", AVRO_BYTES);
    addValue("string", AVRO_STRING);
    addValue("record", AVRO_RECORD);
    addValue("enum", AVRO_ENUM);
    addValue("array", AVRO_ARRAY);
    addValue("map", AVRO_MAP);
    addValue("union", AVRO_UNION);
    addValue("fixed", AVRO_FIXED);
    addValue("type_name", AVRO_TYPE_NAME);
}

struct AvroSchema;

enum AvroOrder {
    AVRO_DESCENDING,
    AVRO_ASCENDING,
    AVRO_IGNORE
};

DECLARE_ENUM_DESCRIPTION(AvroOrder);

DEFINE_ENUM_DESCRIPTION(AvroOrder);

AvroOrderDescription::
AvroOrderDescription()
{
    addValue("ascending", AVRO_ASCENDING);
    addValue("descending", AVRO_DESCENDING);
    addValue("ignore", AVRO_IGNORE);
}

struct AvroField;


struct AvroType {    
    AvroTypeEnum type;
    std::string name;
    std::string nameSpace;
    Utf8String doc;
    std::vector<Utf8String> aliases;
    std::vector<AvroField> fields;  ///< For records
    std::vector<Utf8String> symbols; ///< For enums
    std::vector<AvroType> options;  ///< For unions
    Optional<AvroSchema> items; ///< For arrays
    Optional<AvroSchema> values; ///< For maps
    ssize_t size = -1;  ///< for fixed

    std::shared_ptr<const AvroParser>
    compile(AvroDataDictionary & dict) const
    {
        std::shared_ptr<const AvroParser> result;

        switch (type) {
        case AVRO_NULL:
            return std::make_shared<AvroNullParser>();
        case AVRO_BOOLEAN:
            return std::make_shared<AvroBooleanParser>();
        case AVRO_INT:
            return std::make_shared<AvroIntParser>();
        case AVRO_LONG:
            return std::make_shared<AvroLongParser>();
        case AVRO_FLOAT:
            return std::make_shared<AvroFloatParser>();
        case AVRO_DOUBLE:
            return std::make_shared<AvroDoubleParser>();
        case AVRO_BYTES:
            return std::make_shared<AvroBytesParser>();
        case AVRO_STRING:
            return std::make_shared<AvroStringParser>();
        case AVRO_TYPE_NAME:
            return dict.getParserForType(name);

        // These we parse, then add to the dictionary

        case AVRO_RECORD:
            result = std::make_shared<AvroRecordParser>(fields, dict);
            break;
        case AVRO_ENUM:
            result = std::make_shared<AvroEnumParser>(symbols);
            break;
        case AVRO_ARRAY:
            result = std::make_shared<AvroArrayParser>(*items, dict);
            break;
        case AVRO_MAP:
            result = std::make_shared<AvroMapParser>(*values, dict);
            break;
        case AVRO_UNION:
            throw HttpReturnException(400, "hello");
            result = std::make_shared<AvroUnionParser>(options, dict);
            break;
        case AVRO_FIXED: 
            result = std::make_shared<AvroFixedParser>(size);
            break;
        }

        if (!result) {
            throw HttpReturnException(500, "Internal logic error in Avro parser");
        }

        return result;
    }
};

DECLARE_STRUCTURE_DESCRIPTION(AvroType);

struct AvroSchema {
    std::string name;
    std::vector<AvroType> types;

    std::shared_ptr<const AvroParser>
    compile(AvroDataDictionary & dict) const
    {
        cerr << "compiling " << name << " with " << types.size()
             << " types" << endl;
        if (!name.empty())
            return dict.getParserForType(name);

        std::vector<std::shared_ptr<const AvroParser> > parsers;
        for (auto & t: types) {
            parsers.emplace_back(t.compile(dict));
        }

        if (parsers.size() == 1) {
            return parsers[0];
        }
        else {
            ExcAssert(!parsers.empty());
            return std::make_shared<AvroUnionParser>(parsers);
        }
    }
};

PREDECLARE_VALUE_DESCRIPTION(AvroSchema);

struct AvroField {
    PathElement name;
    Utf8String doc;
    AvroSchema type;
    Json::Value defaultValue;
    AvroOrder order = AVRO_ASCENDING;
    std::vector<Utf8String> aliases;
};

DECLARE_STRUCTURE_DESCRIPTION(AvroField);

DEFINE_STRUCTURE_DESCRIPTION(AvroField);

AvroFieldDescription::
AvroFieldDescription()
{
    addField("name", &AvroField::name, "");
    addField("doc", &AvroField::doc, "");
    addField("type", &AvroField::type, "");
    addField("default", &AvroField::defaultValue, "");
    addAuto("order", &AvroField::order, "");
    addField("aliases", &AvroField::aliases, "");
}

DEFINE_STRUCTURE_DESCRIPTION(AvroType);

AvroTypeDescription::
AvroTypeDescription()
{
    addField("type", &AvroType::type, "");
    addField("name", &AvroType::name, "");
    addField("namespace", &AvroType::nameSpace, "");
    addField("doc", &AvroType::doc, "");
    addField("aliases", &AvroType::aliases, "");
    addField("fields", &AvroType::fields, "");
    addField("symbols", &AvroType::symbols, "");
    addField("items", &AvroType::items, "");
    addField("values", &AvroType::values, "");
    addAuto("size", &AvroType::size, "");

    // Intercept parsing to deal with a string as a type name
    this->onEntryHandler
        = [&] (AvroType * type, JsonParsingContext & context)
        {
            if (context.isString()) {
                // We got a type name
                type->type = AVRO_TYPE_NAME;
                type->name = context.expectStringAscii();

                // Finished parsing, we return false as we don't want the
                // default structure parser to start
                return false;
            }

            return true;
        };
         
    // Unknown fields are considered OK as metadata, we ignore them
    this->onUnknownField = [&] (AvroType * type, JsonParsingContext & context)
        {
            // Just consume it and ignore it
            context.expectJson();
        };
}

struct AvroSchemaDescription
    : public ValueDescriptionT<AvroSchema> {
    virtual void parseJsonTyped(AvroSchema * val,
                                JsonParsingContext & context) const
    {
        if (context.isArray()) {
            val->types
                = jsonDecode<std::vector<AvroType> >(context.expectJson());
        }
        else if (context.isString()) {
            val->name = context.expectStringAscii();
        }
        else if (context.isObject()) {
            val->types.resize(1);
            val->types[0]
                = jsonDecode<AvroType>(context.expectJson());
        }
        else throw HttpReturnException(400, "Invalid avro type in schema");
    }

    virtual void printJsonTyped(const AvroSchema * val,
                                JsonPrintingContext & context) const
    {
        static AvroTypeDescription desc;

        if (!val->name.empty()) {
            context.writeString(val->name);
        }
        else if (val->types.size() == 1) {
            desc.printJsonTyped(&val->types[0], context);
        }
        else {
            context.startArray(val->types.size());
            for (auto & t: val->types) {
                context.newArrayElement();
                desc.printJsonTyped(&t, context);
            }
            context.endArray();
        }
    }

    virtual bool isDefaultTyped(const AvroSchema * val) const
    {
        return false;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(AvroSchema, AvroSchemaDescription);

AvroRecordParser::
AvroRecordParser(std::vector<AvroField> fields,
                 AvroDataDictionary & dict)
    : fields(std::move(fields))
{
    for (auto & f: this->fields) {
        fieldParsers.emplace_back(PathElement(f.name), f.type.compile(dict));
    }
}

ExpressionValue
AvroRecordParser::
parseRow(const char * & current,
         const char * end,
         Date ts) const
{
    StructValue result;
    for (auto & f: fieldParsers) {
        result.emplace_back(f.first, f.second->parseRow(current, end, ts));
    }
    return result;
}

std::shared_ptr<ExpressionValueInfo>
AvroRecordParser::
getInfo() const
{
    std::vector<KnownColumn> columns;
    for (size_t i = 0;  i < fields.size();  ++i) {
        columns.emplace_back(fieldParsers[i].first,
                             fieldParsers[i].second->getInfo(),
                             COLUMN_IS_DENSE);
    }
    return std::make_shared<RowValueInfo>(std::move(columns),
                                          SCHEMA_CLOSED);
}

AvroUnionParser::
AvroUnionParser(std::vector<AvroType> fields,
                AvroDataDictionary & dict)
    : options(std::move(options))
{
    for (auto & o: this->options) {
        optionParsers.emplace_back(o.compile(dict));
    }
}
    
AvroUnionParser::
AvroUnionParser(std::vector<std::shared_ptr<const AvroParser> > parsers)
    : optionParsers(std::move(parsers))
{
}

AvroMapParser::
AvroMapParser(AvroSchema values,
              AvroDataDictionary & dict)
{
    valueParser = values.compile(dict);
}

AvroArrayParser::
AvroArrayParser(AvroSchema items,
                AvroDataDictionary & dict)
{
    itemParser = items.compile(dict);
}

static bool matchAvroLong(int64_t & val, std::istream & stream)
{
    uint64_t encoded = 0;
    for (int n = 0; /* break in loop */; ++n) {
        int c = stream.get();
        if (c == EOF) {
            if (n == 0)
                return false;
            throw HttpReturnException(400, "Truncated AVRO integer");
        }
        uint64_t bits = ((unsigned char)c) & 127;
        //cerr << "n = " << n << " c = " << c << " bits = " << bits << endl;
        encoded = encoded | (bits << 7*n);
        if (c & 128) continue;
        break;
    }
    if (!stream)
        throw HttpReturnException(400, "Error reading AVRO integer");

    //cerr << "encoded = " << encoded << endl;

    val = encoded << 63 /* sign */ | encoded >> 1 /* value */;
    return true;
}

static int64_t expectAvroLong(std::istream & stream)
{
    int64_t result;
    if (!matchAvroLong(result, stream)) {
        throw HttpReturnException(400, "Expected Avro integer");
    }
    return result;
}

static std::string expectAvroBytes(std::istream & stream)
{
    int64_t length = expectAvroLong(stream);

    std::string bytes((size_t)length, '\0');
    stream.read(&bytes[0], length);

    if (!stream)
        throw HttpReturnException(400, "Invalid AVRO bytes");

    return bytes;
}

static Utf8String expectAvroString(std::istream & stream)
{
    return expectAvroBytes(stream);
}

struct AvroDataBlock {
    AvroDataBlock(int64_t blockBytes = 0,
                  int64_t firstObject = 0,
                  int64_t numObjects = 0)
        : contents(new char[blockBytes]),
          length(blockBytes),
          firstObject(firstObject),
          numObjects(numObjects)
    {
    }

    std::unique_ptr<char[]> contents;
    size_t length;
    int64_t firstObject;
    int64_t numObjects;
};

struct AvroImportCoordinator {
    AvroImportCoordinator(AvroSchema schema,
                          std::string compression,
                          Dataset * dataset,
                          const SelectExpression & select,
                          ssize_t offset,
                          ssize_t limit)
        : schema(std::move(schema)),
          compression(std::move(compression)),
          dataset(dataset),
          offset(offset),
          limit(limit),
          outerScope(dataset->server),
          selectScope(outerScope),
          hasException(false)
    {
        parser = this->schema.compile(dict);
        recorder = dataset->getChunkRecorder();

        auto info = parser->getInfo();
        //cerr << jsonEncode(info) << endl;

        selectScope.init(info);
        selectBound = select.bind(selectScope);

        info = selectBound.info;
        cerr << jsonEncode(info) << endl;

        cerr << "wildcardsInInput = " << selectScope.wildcardsInInput << endl;
        cerr << "inferredInputs = ";
        for (auto & i: selectScope.inferredInputs)
            cerr << i << " ";
        cerr << endl;
    }
    
    AvroSchema schema;
    std::string compression;
    Dataset * dataset;
    AvroDataDictionary dict;
    std::shared_ptr<const AvroParser> parser;
    Dataset::MultiChunkRecorder recorder;
    ssize_t offset;
    ssize_t limit;
    SqlExpressionMldbScope outerScope;
    SqlExpressionExtractScope selectScope;
    BoundSqlExpression selectBound;

    std::atomic_flag hasException = ATOMIC_FLAG_INIT;
    std::exception_ptr exception;

    void process(std::vector<AvroDataBlock> & blocks,
                 size_t chunkNumber)
    {
        auto chunkRecorder = recorder.newChunk(chunkNumber);
        size_t chunkNumObjects = 0;

        try {
            for (auto & block: blocks) {
                if (block.firstObject + block.numObjects < offset)
                    continue;
                if (limit != -1 && block.firstObject >= offset + limit)
                    break;

                std::shared_ptr<Decompressor> decompressor
                    (Decompressor::create(compression));
                
                std::string decompressed;
                decompressed.reserve(block.length);
            
                auto onData = [&] (const char * data, size_t len)
                    {
                        decompressed.append(data, len);
                        return len;
                    };

                size_t compressedLength = block.length;
                if (compression == "snappy") {
                    compressedLength -= 4; // for CRC32 calculation
                }
            
                decompressor->decompress(block.contents.get(), compressedLength,
                                         onData);

                //cerr << "decompressed is " << decompressed.size() << " bytes" << endl;

                const char * current = decompressed.data();
                const char * end = current + decompressed.length();

                Date ts = Date::now();

                size_t numObjects = 0;
                while (current < end) {
                    size_t objectNumber = numObjects + block.firstObject;
                    ++numObjects;
                    if (objectNumber < offset)
                        continue;
                    if (limit != -1 && objectNumber > offset + limit)
                        break;
                    Path rowName(PathElement(0 + objectNumber));
                    ExpressionValue row
                        = parser->parseRow(current, end, ts);

                    ExpressionValue storage;
                    const ExpressionValue & selected
                        = selectBound.exec(selectScope.getRowScope(row),
                                           storage,
                                           GET_ALL);
                    
                    if (&selected == &row) {
                        chunkRecorder
                            ->recordRowExprDestructive(std::move(rowName),
                                                       std::move(row));
                    }
                    else if (&selected == &storage) {
                        chunkRecorder
                            ->recordRowExprDestructive(std::move(rowName),
                                                       std::move(storage));
                    }
                    else {
                        throw HttpReturnException(500, "Internal logic error");
                    }

                }

                //cerr << "got " << numObjects << " objects of "
                //     << block.numObjects << endl;
                if (numObjects != block.numObjects) {
                    throw HttpReturnException(400, "Avro block had wrong number of objects");
                }

                chunkNumObjects += numObjects;
            }

        } catch (const std::exception & exc) {
            cerr << "got exception " << exc.what() << endl;
            if (hasException.test_and_set() == false) {
                this->exception = std::current_exception();
            }
        } MLDB_CATCH_ALL {
            if (hasException.test_and_set() == false) {
                this->exception = std::current_exception();
            }
        }

        chunkRecorder->finishedChunk();

        cerr << "done chunk " << chunkNumber << " with "
             << chunkNumObjects << " objects" << endl;
    }

    void commit()
    {
        recorder.commit();
    }
};

RunOutput
ImportAvroProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(config, run);

    std::shared_ptr<Dataset> dataset
        = createDataset(server, runProcConf.outputDataset, onProgress,
                        true /*overwrite*/);

    filter_istream stream(config.dataFileUrl);


    char h[5] = {0,0,0,0};

    stream.read(h, 4);

    if (h[0] != 'O' || h[1] != 'b' || h[2] != 'j' || h[3] != 1) {
        cerr << "header " << h << endl;
        throw HttpReturnException(400, "Avro file header is wrong");
    }

    // These are the header fields we care about
    std::string compression = "null";
    std::string schemaJson;

    while (true) {
        int64_t numHeaders = expectAvroLong(stream);

        cerr << "there are " << numHeaders << " headers" << endl;

        if (numHeaders == 0)
            break;
        if (numHeaders < 0) {
            numHeaders = -numHeaders;
            expectAvroLong(stream);  // byte count of block; unused
        }
        
        for (size_t i = 0;  i < numHeaders;  ++i) {
            Utf8String headerName = expectAvroString(stream);
            std::string headerValue = expectAvroBytes(stream);

            if (headerName == "avro.schema") {
                schemaJson = headerValue;
            }
            else if (headerName == "avro.codec") {
                compression = headerValue;
            }

            cerr << "header " << headerName << " has value " << headerValue
                 << endl;
        }
    }

    if (schemaJson.empty()) {
        throw HttpReturnException(400, "Avro file had no schema");
    }
    
    AvroSchema schema = jsonDecodeStr<AvroSchema>(schemaJson);

    cerr << "schema = " << jsonEncode(schema) << endl;

    AvroImportCoordinator coordinator(schema, compression, dataset.get(),
                                      config.select,
                                      config.offset, config.limit);

    static constexpr size_t SYNC_MARKER_LENGTH = 16;
    char syncMarker[SYNC_MARKER_LENGTH];
    stream.read(syncMarker, SYNC_MARKER_LENGTH);

    if (!stream) {
        throw HttpReturnException(400, "Invalid Avro stream marker");
    }

    Date before = Date::now();
    
    // Read blocks for parallel processing
    int64_t objectNumber = 0;
    size_t numBlocks = 0;
    int64_t totalBytes = 0;
    int numWorkerThreads = numCpus();
    size_t chunkNumber = 0;

    ThreadPool pool(ThreadPool::instance(), numWorkerThreads);

    // Do the given block, starting with the given set of records
    std::function<void (AvroDataBlock )> doChunk
        = [&] (AvroDataBlock startingBlock)
        {
            std::vector<AvroDataBlock> chunkBlocks;
            size_t chunkNumObjects = 0;
            chunkBlocks.reserve(1024);
            if (startingBlock.numObjects > 0) {
                chunkNumObjects = startingBlock.numObjects;
                chunkBlocks.emplace_back(std::move(startingBlock));
            }

            size_t thisChunkNumber = chunkNumber++;
            
            static constexpr size_t CHUNK_SIZE = 65536;

            while (chunkNumObjects < CHUNK_SIZE) {
                ++numBlocks;
                int64_t numObjects;
                if (!matchAvroLong(numObjects, stream))
                    break;
                int64_t blockBytes = expectAvroLong(stream);
                totalBytes += blockBytes;
                
                AvroDataBlock block(blockBytes, objectNumber, numObjects);
        
                objectNumber += numObjects;
        
                stream.read(block.contents.get(), blockBytes);

                if (!stream) {
                    throw HttpReturnException(400, "Truncated Avro stream");
                }

                char blockSyncMarker[SYNC_MARKER_LENGTH];

                stream.read(blockSyncMarker, SYNC_MARKER_LENGTH);
                
                if (!stream) {
                    throw HttpReturnException(400, "Truncated Avro stream");
                }
                
                //cerr << "block " << numBlocks
                //     << " has " << blockBytes << " bytes and " << numObjects
                //     << " objects" << endl;
                
                for (int i = 0;  i < SYNC_MARKER_LENGTH;  ++i) {
                    if (blockSyncMarker[i] != syncMarker[i]) {
                        ML::hex_dump(syncMarker, 16);
                        ML::hex_dump(blockSyncMarker, 16);
                        throw HttpReturnException(400, "Invalid Avro sync marker");
                    }
                }

                if (chunkNumObjects > 0
                    && chunkNumObjects + block.numObjects > CHUNK_SIZE) {
                    // We have finished with this chunk
                    // Continue processing the stream on another thread
                    auto sharedBlock = std::make_shared<AvroDataBlock>(std::move(block));
                    pool.add([=] () { doChunk(std::move(*sharedBlock)); });
                    break;
                }
                else {
                    // Add to the list
                    chunkNumObjects += block.numObjects;
                    chunkBlocks.emplace_back(std::move(block));
                }
            }

            // Now import this block from this thread
            coordinator.process(chunkBlocks, thisChunkNumber);
        };

    pool.add([&] () { doChunk(AvroDataBlock()); });

    pool.waitForAll();

    coordinator.commit();

    Date after = Date::now();
    double elapsed = after.secondsSince(before);

    cerr << endl << endl;
    cerr << "read " << numBlocks << " blocks with " << objectNumber
         << " objects in " << elapsed << "seconds with "
         << 1.0 * totalBytes / objectNumber
         << " bytes/object at " << objectNumber / elapsed
         << " objects/second and "
         << totalBytes / elapsed / 1000000.0 << " MB/second"
         << endl;



#if 0
    ImportAvroProcedureWorkInstance instance(logger);

    instance.loadAvro(config, dataset, server, onProgress);
#endif

    Json::Value status;
#if 0
    status["numLineErrors"] = instance.numLineErrors;
    status["rowCount"] = instance.rowCount;
#endif

    dataset->commit();

    return Any(status);
}

namespace {

RegisterProcedureType<ImportAvroProcedure, ImportAvroConfig>
regImportAvro(builtinPackage(),
      "Import from a avro file, line by line.",
      "procedures/import_avro_procedure.md.html");

} // file scope

} //MLDB


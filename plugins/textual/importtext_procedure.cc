/** importext_procedure.cc
    Mathieu Marquis Bolduc, February 12, 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Procedure that reads text files into an indexed dataset.
*/

#include "importtext_procedure.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/demangle.h"
#include "mldb/utils/csv.h"
#include "mldb/utils/lightweight_hash.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/utils/for_each_line.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/base/per_thread_accumulator.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/core/dataset_scope.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/utils/progress.h"
#include "mldb/utils/vector_utils.h"
#include "mldb/utils/log.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "sql_csv_scope.h"
#include "csv_splitter.h"
#include "mldb/base/parse_context.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/base/optimized_path.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/base/hex_dump.h"
#include "mldb/sql/decompose.h"


using namespace std;


namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION_INLINE(ImportTextConfig)
{
    addField("dataFileUrl", &ImportTextConfig::dataFileUrl,
             "URL of the text data to import");
    addField("headers", &ImportTextConfig::headers,
             "List of headers for when first row doesn't contain headers",
             vector<Utf8String>());
    addField("quoteChar", &ImportTextConfig::quoter,
             "Character to enclose strings", string("\""));
    addField("delimiter", &ImportTextConfig::delimiter,
             "Delimiter for column separation", string(","));
    addField("limit", &ImportTextConfig::limit,
             "Maximum number of lines to process.  Bad lines including empty lines "
             "contribute to the limit.  As a result, it is possible for the dataset "
             "to contain less rows that the requested limit.");
    addAuto("preHeaderOffset", &ImportTextConfig::preHeaderOffset,
             "Skip the first n lines before reading the header (if present).");
    addField("offset", &ImportTextConfig::offset,
             "Skip the first n lines (excluding the header if present).", int64_t(0));
    addField("encoding", &ImportTextConfig::encoding,
             "Character encoding of file: 'us-ascii', 'ascii', 'latin1', 'iso8859-1', 'utf8' or 'utf-8'",
             string("utf-8"));
    addField("ignoreBadLines", &ImportTextConfig::ignoreBadLines,
             "If true, any line causing a parsing error will be skipped. "
             "Empty lines are considered bad lines.", false);
    addField("structuredColumnNames", &ImportTextConfig::structuredColumnNames,
             "If true, column names that look like 'x.y' will import like "
             "`{ x: { y: ... } }` instead of `{ \"x.y\": ... }`, in other "
             "words structure will be preserved.  The flip side is that quotes "
             "will need to be doubled and any name that includes a period will "
             "need to be quoted.  The default is to not preserve structure", false);
    addField("replaceInvalidCharactersWith",
             &ImportTextConfig::replaceInvalidCharactersWith,
             "If this is set, it should be a single Unicode character will be used "
             "to replace badly-encoded characters in the input. "
             "The default is nothing, which will cause lines with badly-"
             "encoded characters to throw an error.");
    addField("allowMultiLines", &ImportTextConfig::allowMultiLines,
             "Allows columns with multi-line quoted strings. "
             "This option disables many optimizations and makes the procedure "
             "run much slower. Only use if necessary. The `offset` parameter "
             "will not be reliable when this is activated.", false);
    addField("autoGenerateHeaders", &ImportTextConfig::autoGenerateHeaders,
             "If true, the indexes of the columns will be used to name them."
             "This cannot be set to true if headers is defined.",
             false);
    addAuto("ignoreExtraColumns", &ImportTextConfig::ignoreExtraColumns,
            "Ignore extra columns that weren't in header.  This allows for files that "
            "have optional trailing columns that aren't listed in the header or for "
            "files with a partially fixed, partially variable column set to be imported.");
    addAuto("processExcelFormulas", &ImportTextConfig::processExcelFormulas,
            "Process formulas in `=\"...\"` format from Excel export of "
            "CSV");
    addAuto("skipLineRegex", &ImportTextConfig::skipLineRegex,
            "Regex used to skip lines.  This regex must match the entire line, in "
            "other words it is automatically anchored at the beginning and the "
            "end of the line, so `#.*` will skip lines that have a `#` in the "
            "first character position.  Default skips no lines.");

    addParent<ProcedureConfig>();
    addParent<DatasetBuilderConfig>();
    onUnknownField = [] (ImportTextConfig * config,
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
            else if (context.fieldName() == "quotechar") {
                config->quoter = context.expectStringAscii();
                auto logger = MLDB::getMldbLog<ImportTextProcedure>();
                WARNING_MSG(logger) << "The 'quotechar' argument has been renamed to 'quoteChar'";
            }
            else {
                context.exception("Unknown field '" + context.fieldName()
                                  + " parsing import.text configuration");
            }
        };

    onPostValidate = [] (ImportTextConfig * config,
                         JsonParsingContext & context) {
        if (!config->headers.empty() && config->autoGenerateHeaders) {
            throw MLDB::Exception("autoGenerateHeaders cannot be true if "
                                "headers is defined.");
        }
    };
}


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

namespace {

const char * findInvalidAscii(const char * start, size_t length, char*buf, char replaceInvalidCharactersWith) {

    memcpy(buf, start, length);

    char* p = buf;
    char* end = buf+length;
    while (p != end) {
        if (!isJsonValidAscii(*p))
            *p = replaceInvalidCharactersWith;
        ++p;
    }

    return buf;
}

} // file scope

namespace {
    const string unclosedQuoteError = "Unclosed quoted CSV value";
    const string notEnoughColsError = "not enough columns in row";
}

// Inline version of isascii
MLDB_ALWAYS_INLINE bool isascii(int c)
{
    return (c & (~127)) == 0;
}

/** Parse a single row of CSV into an array of CellValues.

    Carefully designed to not perform any memory allocations in the
    common case.

    Returns an error message (which may be null) describing what kind
    of error was caused.  Note that it is also possible that the
    system throw, eg if a memory allocation failed when constructing a
    CellValue.

    Parameters:
    - line: pointer to the start of the line being parsed
    - length: number of characters in the line.  No null terminator is
    required or expected.
    - values: output array of CellValues to recieve output.  These should all
    be initialized to null on entry.
    - numColumns: length of the values array, which is the number of columns
    expected to be found.
    - separator: CSV separator character; usually a comma
    - quote: CSV quote character, usually a double quote
    - encoding: encoding of lines
    - replaceInvalidCharactersWith: if -1, badly encoded lines will cause an error
    Otherwise, it's the ASCII code point to put in place of them.
    - isTextLine: optimization to ignore separator and quote chars and get a single column per line
    - hasQuoteChar: should we use the quote char
*/

const char *
parseFixedWidthCsvRow(const char * & line,
                      size_t length,
                      CellValue * values,
                      size_t numColumns,
                      char separator,
                      char quote,
                      CsvLineEncoding encoding,
                      int replaceInvalidCharactersWith,
                      bool isTextLine,
                      bool hasQuoteChar,
                      const shared_ptr<spdlog::logger> & logger,
                      bool ignoreExtraColumns,
                      bool processExcelFormulas,
                      const std::vector<int> & columnIsUsed,
                      const std::vector<ColumnPath> & inputColumnNames)
{
    ExcAssert(!(hasQuoteChar && isTextLine));

    // Skip trailing whitespace in the row.  If we want spaces, we should use
    // quotes.
    while (length > 0 && isspace(line[length - 1]))
        --length;
    
    const char * lineEnd = line + length;

    const char * errorMsg = nullptr;

    size_t colNum = 0;

    auto finishString = [encoding,replaceInvalidCharactersWith,&colNum,&columnIsUsed]
        (const char * start, size_t len, bool eightBit) -> CellValue
        {
            // Short circuit for when we don't use the column
            if (colNum < columnIsUsed.size() && !columnIsUsed[colNum]) {
                return CellValue();
            }

            if (!eightBit) {
                PossiblyDynamicBuffer<char> buf(len);
                if (replaceInvalidCharactersWith >= 0) {
                    ExcAssert(replaceInvalidCharactersWith < 256);
                    start = findInvalidAscii(start, len, buf.data(), (char)replaceInvalidCharactersWith);
                }
                return CellValue::parse(start, len, STRING_IS_VALID_ASCII);
            }

            // Parse differently based upon encoding
            switch (encoding) {
            case ASCII:
                throw MLDB::Exception("non-ASCII character in ASCII text file");
            case LATIN1:
                return CellValue(Utf8String::fromLatin1(string(start, len)));
            case UTF8:
                if (replaceInvalidCharactersWith != -1) {
                    const char * end = utf8::find_invalid(start, start + len);
                    if (end == start + len)
                        return CellValue(start, len, STRING_UNKNOWN);
                    else {
                        static constexpr int BUF_PADDING = 64; // defensive; only 5 chars should be needed

                        PossiblyDynamicBuffer<char> buf(len + BUF_PADDING);
                        char * end
                            = utf8::replace_invalid(start, start + len, buf.data(),
                                                    replaceInvalidCharactersWith);

                        if (end < buf.begin() || end > buf.end()) {
                            // Abort immediately without unwinding as stack has
                            // been smashed
                            ::fprintf(stderr, "Replace invalid smashed stack");
                            abort();
                        }
                        return CellValue(buf, end - buf, STRING_UNKNOWN);
                    }
                }
                return CellValue(start, len, STRING_UNKNOWN);
            default:
                ExcAssert(false);
            }
        };

    while (colNum < numColumns) {

        ExcAssert(line <= lineEnd);

        if (line == lineEnd) {
            // Empty column at the end
            ++colNum;
            break;
        }

        if (colNum >= numColumns) {
            if (ignoreExtraColumns)
                break;
            return "too many columns in row";
        }

        const char * start = line;

        const char c = *line++;

        if (c == separator && !isTextLine) {
            // null field
            ++colNum;
            continue;
        }
        else if (hasQuoteChar
                 && (c == quote
                     || (processExcelFormulas
                         && c == '='
                         && length > 1
                         && line[0] == quote && (++line || true/* side effect */)))) {
            // quoted string, or ="..." (excel formula style)
            static constexpr size_t FIXED_BUF_LEN = 4096;
            char sbuf[FIXED_BUF_LEN];  // holds the extracted string
            char * s = sbuf;
            size_t buflen = FIXED_BUF_LEN;
            std::unique_ptr<char[]> sdynamic;
            int len = 0;   // and its length

            bool eightBit = false;
            bool ok = false;

            bool parseColumn = colNum >= columnIsUsed.size()
                || columnIsUsed[colNum];
            
            auto pushChar = [&] (char c)
                {
                    if (!parseColumn)
                        return;
                    
                    if (len == buflen) {
                        std::unique_ptr<char[]> newBuf(new char[buflen * 2]);
                        std::copy(s, s + len, newBuf.get());
                        sdynamic.swap(newBuf);
                        s = sdynamic.get();
                        buflen *= 2;
                    }


                    ExcAssertLess(len, buflen);
                    eightBit = eightBit || !isascii(c);
                    s[len++] = c;
                };

            for (; line < lineEnd;  ++line) {
                const char c = *line;
                if (c == quote) {
                    ++line;
                    if (line >= lineEnd) {
                        ok = true;
                        break;
                    }
                    else if (*line == separator) {
                        ok = true;
                        ++line;
                        break;
                    }
                    else if (*line == quote) {
                        // doubled quote; take a literal value
                        pushChar(quote);
                    }
                    else {
                        // Error
                        errorMsg = "Garbage after closing quote";
                        break;
                    }
                }
                else {
                    pushChar(c);
                }
            }

            if (!ok)
                errorMsg = unclosedQuoteError.c_str();

            if (errorMsg)
                break;

            values[colNum++] = finishString(s, len, eightBit);
        }
        else if ((isdigit(c) || (c == '-' && line != lineEnd && *line != separator)) && !isTextLine) {
            // Special case for something that looks like a number, in order to
            // save on parsing it.  We short circuit out when we get to a length
            // where we could start to lose digits, and fall back on parsing the
            // string version.
            int64_t sign = -(c == '-');
            uint64_t num = isdigit(c) ? c - '0' : 0;
            int len = 1;
            bool isInt = true;

            bool eightBit = false;

            for (; line < lineEnd;  ++line, ++len) {
                ExcAssert(line < lineEnd);
                const char c = *line;
                if (c == separator) {
                    ++line;
                    break;
                }
                if (line - start >= 18)
                    isInt = false;  // too long; could lose precision
                if (isInt && isdigit(c)) {
                    num = 10 * num + (c - '0');
                }
                else if (!isascii(c)) {
                    eightBit = true;
                    isInt = false;
                }
                else {
                    isInt = false;
                }
            }

            if (isInt && sign == -1)
                values[colNum++] = (int64_t)-num;
            else if (isInt)  // positive integer
                values[colNum++] = num;
            else // get it from the string
                values[colNum++]
                    = finishString(start, len, eightBit);
        }
        else {
            // likely a non-quoted string

            bool eightBit = !isascii(c);
            size_t len = 1;

            for (; line < lineEnd;  ++line, ++len) {
                const char c = *line;
                if (c == separator && !isTextLine) {
                    ++line;
                    break;
                }
                if (!isascii(c))
                    eightBit = true;
            }

            values[colNum++] = finishString(start, len, eightBit);
        }
    }

    if (errorMsg)
        return errorMsg;

    if (line < lineEnd && !ignoreExtraColumns) {
        return "too many columns in row";
    }

    if (colNum != numColumns)
        return notEnoughColsError.c_str();

    return errorMsg;
}

/*****************************************************************************/
/* IMPORT TEXT PROCEDURE WORK INSTANCE                                       */
/* Manages all the temporary data and work to load a text file               */
/*****************************************************************************/

struct ImportTextProcedureWorkInstance {
    BoundDatasetBuilder builder;
    //vector<ColumnPath> knownColumnNames;
    LightweightHash<ColumnHash, int> inputColumnIndex;
    //LightweightHash<ColumnHash, int> columnIndex; //To check for duplicates column names
    int64_t lineOffset = 1;  // we start at line 1
    // Column names in the CSV file.  This is distinct from the
    // output column names that will be created once parsing has
    // happened.
    vector<ColumnPath> inputColumnNames;
    bool isTextLine = false;
    char separator = 0;
    char quote = 0;
    int replaceInvalidCharactersWith = -1;
    CsvLineEncoding encoding;
    bool hasQuoteChar = false;
    Date ts;

    size_t rowCount = 0;
    uint64_t numLineErrors = 0;

    /*    Load a text file and filter according to the configuration  */
    void loadText(DatasetBuilder & datasetBuilder,
                  const ImportTextConfig& config)
    {
        auto filename = config.dataFileUrl.toDecodedString();

        // Get a handle to this content, which ensures we don't have any
        // kind of version skew from asking for different parts of the file
        std::shared_ptr<ContentHandler> content
            = getDecompressedContent(filename);

        // For some operations, we need a stream.  Get this from the
        // content handler, so that the underlying data is all shared.
        filter_istream stream = content->getStream( /*{ { "mapped", true } }*/);
        
        // Get the file timestamp out, to be used internally
        ts = content->getLastModified();

        std::string line;
        
        // Skip those up to the offset
        for (size_t i = 0;  stream && i < config.preHeaderOffset;  ++i, ++lineOffset) {
            getline(stream, line);
        }

        string header;

        if (config.delimiter.length() == 1) {
            separator = config.delimiter[0];
        }
        else if (config.delimiter.length() > 1) {
            throw AnnotatedException(400, "Separator string must have one character");
        }
        else if (config.quoter.length() > 0)
        {
            throw AnnotatedException(400, "Separator string must not be empty if we have a quoter string");
        }

        if (config.quoter.length() == 1) {
            quote = config.quoter[0];
            hasQuoteChar = true;
        }
        else if (config.quoter.length() > 1) {
            throw AnnotatedException(400, "Quoter string must have one character");
        }

        isTextLine = config.quoter.empty() && config.delimiter.empty();

        if (!config.replaceInvalidCharactersWith.empty()) {
            if (config.replaceInvalidCharactersWith.length() != 1)
                throw AnnotatedException(400, "replaceInvalidCharactersWith string must have one character");
            replaceInvalidCharactersWith = *config.replaceInvalidCharactersWith.begin();
        }

        encoding = parseCsvLineEncoding(config.encoding);

        if (isTextLine) {
            //MLDB-1312 optimize if there is no delimiter: only 1 column
            if (config.headers.empty()) {
                inputColumnNames = { ColumnPath(config.autoGenerateHeaders ? 0 : "lineText") };
            }
            else if (config.headers.size() != 1) {
                throw AnnotatedException(
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
                        && regex_match(header, config.skipLineRegex))
                        continue;
                    
                    if(!prevHeader.empty()) {
                        prevHeader += ' ' + header;
                        header.assign(std::move(prevHeader));
                    }

                    try {
                        ParseContext pcontext(config.dataFileUrl.getUrlStringUtf8(),
                                              header.c_str(), header.length(), 1, 0);
                        fields = expect_csv_row(pcontext, -1, separator);
                        break;
                    }
                    catch (const FileFinishInsideQuote & exp) {
                        if(config.allowMultiLines) {
                            prevHeader.assign(std::move(header));
                            continue;
                        }

                        throw exp;
                    }
                }

                if (config.autoGenerateHeaders) {
                    // Re-open stream
                    content = getContent(filename);
                    stream = content->getStream({ { "mapped", true } });
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
        for (unsigned i = 0;  i < inputColumnNames.size();  ++i) {
            const ColumnPath & c = inputColumnNames[i];
            ColumnHash ch(c);
            if (!inputColumnIndex.insert(make_pair(ch, i)).second)
                throw AnnotatedException(400, "Duplicate column name in CSV file",
                                          "columnName", c);
        }

        // Now we know the columns, we can bind our SQL expressions for the
        // select, where, named and timestamp parts of the expression.
        SqlCsvScope scope(datasetBuilder.engine, inputColumnNames, ts,
                          Utf8String(config.dataFileUrl.getUrlString()),
                          false /* can have extra columns */);

        this->builder = datasetBuilder.bind(scope, inputColumnNames);

        // Skip those up to the offset now we've done the header
        // TODO: do this skipping later on
        for (size_t i = 0;  stream && i < config.offset;  ++i, ++lineOffset) {
            getline(stream, line);
        }

        loadTextData(content, stream.tellg(), config, scope, datasetBuilder.onProgress);

        datasetBuilder.commit();
    }

    /*    Load, filter and format all lines and process them  */
    void
    loadTextData(std::shared_ptr<ContentHandler> content,
                 uint64_t offset,
                 const ImportTextConfig& config,
                 SqlCsvScope& scope,
                 const std::function<bool (const Json::Value &)> & onProgress)
    {
        auto & logger = builder.logger;

        Progress progress;
        std::shared_ptr<Step> iterationStep = progress.steps({
            make_pair("iterating", "lines"),
        });
        
        std::atomic<uint64_t> numSkipped(0);

        Timer timer;

        auto handleError = [&](const std::string & message,
                               int64_t lineNumber,
                               int64_t columnNumber,
                               const std::string& line) {
            if (config.ignoreBadLines) {
                ++numSkipped;
                return true;
            }

            cerr << "lineNumber " << lineNumber << endl;
            cerr << "columnNumber " << columnNumber << endl;
            //cerr << "line " << line << endl;
            //hex_dump(line.data(), line.length());
            
            throw AnnotatedException(400, "Error parsing CSV row: "
                                      + message,
                                      "lineNumber", lineNumber,
                                      "columnNumber", columnNumber /*,
                                      "line", line*/);
        };

        struct ThreadAccum {
            /// Recorder object for this thread that the DatasetBuilder gives us
            /// to record into the dataset.
            DatasetBuilderChunkRecorder recorder;

            /// Lines done in this thread
            uint64_t linesDone = 0;
            
            /// Bytes done in this thread
            uint64_t bytesDone = 0;

            /// Number of lines in chunk
            ssize_t numLinesInChunk = -1;
        };

        PerThreadAccumulator<ThreadAccum> accum;

        auto startChunk = [&] (int64_t chunkNumber, size_t lineNumber, int64_t numLinesInChunk)
            {
                auto & threadAccum = accum.get();
                threadAccum.recorder = builder.newChunk(chunkNumber);
                return true;
            };

        auto doneChunk = [&] (int64_t chunkNumber, size_t lineNumber, int64_t numLinesInChunk)
            {
                auto & threadAccum = accum.get();
                threadAccum.recorder.finish();
                threadAccum.recorder = {};
                return true;
            };

        //cerr << "outputColumnNames = " << jsonEncode(knownColumnNames) << endl;
        
        atomic<ssize_t> lineCount(0);
        atomic<ssize_t> byteCount(0);

        auto onLine = [&] (LineInfo lineInfo)
        {
            auto lineNum = lineInfo.lineNumber;
            const char * line = lineInfo.line.data();
            size_t length = lineInfo.line.size();

            try {
                auto & threadAccum = accum.get();

                threadAccum.linesDone += 1;
                threadAccum.bytesDone += length + 1;

                if (threadAccum.linesDone > 100 || threadAccum.bytesDone > 65536) {
                    byteCount += threadAccum.bytesDone;
                    uint64_t linesDone
                        = lineCount.fetch_add(threadAccum.linesDone)
                        + threadAccum.linesDone;

                    if (onProgress && linesDone % PROGRESS_RATE_LOW < PROGRESS_RATE_LOW) {
                        iterationStep->value = linesDone;
                        onProgress(jsonEncode(iterationStep));
                    }
                    
                    // Look for the wraparound of the modulus
                    if (linesDone % 100000 < threadAccum.linesDone) {
                        double wall = timer.elapsed_wall();
                        INFO_MSG(logger)
                            << "done " << linesDone << " in " << wall
                            << "s at " << linesDone / wall * 0.000001
                            << "M lines/second on "
                            << timer.elapsed_cpu() / timer.elapsed_wall()
                            << " CPUs";
                    }
                    threadAccum.bytesDone = 0;
                    threadAccum.linesDone = 0;
                }
                
                int64_t actualLineNum = lineNum + lineOffset;

                // Skip lines if we are asked to
                if (config.skipLineRegex.initialized()) {
                    if (regex_match(line, length,
                                    config.skipLineRegex))
                        return true;
                }
                
                // MLDB-1111 empty lines are treated as error, unless they are
                // at the end of the file
                if (length == 0) {
                    return handleError("empty line", actualLineNum, 0, "");
                }
            
                const size_t numInputColumns = inputColumnNames.size();


                // Values that come in from the CSV file
                PossiblyDynamicBuffer<CellValue> values(numInputColumns);


                const char * errorMsg
                        = parseFixedWidthCsvRow(line, length, values.data(),
                                                numInputColumns,
                                                separator, quote, encoding,
                                                replaceInvalidCharactersWith,
                                                isTextLine,
                                                hasQuoteChar, logger,
                                                config.ignoreExtraColumns,
                                                config.processExcelFormulas,
                                                scope.columnsUsed,
                                                inputColumnNames);

                if (errorMsg) {
                    return handleError(errorMsg, actualLineNum,
                                        line - lineStart + 1,
                                        string(line, length));
                }

                auto row = scope.bindRow(values.data(), numInputColumns, ts, actualLineNum,
                                        0 /* todo: chunk ofs */);

                threadAccum.recorder.recordRow(row, values, {} /* extraValues */, actualLineNum);

                return true;
            } MLDB_CATCH_ALL {
                rethrowException(400, "Error parsing CSV line",
                                 "lineNumber", lineNum + lineOffset,
                                 //"line", std::string(line, length),
                                 "dataFileUrl", config.dataFileUrl);
            }
            return false;
        };

        CSVSplitter csvSplitter {
            config.quoter[0], config.allowMultiLines,
            parseCsvLineEncoding(config.encoding), !config.replaceInvalidCharactersWith.empty() };

        NewlineSplitter newlineSplitter;
        const BlockSplitter & splitter
            = config.allowMultiLines
            ? (const BlockSplitter &)csvSplitter
            : (const BlockSplitter &)newlineSplitter;
        forEachLineBlock(content, offset, onLine, config.limit,
                         numCpus() /* parallelism */,
                         startChunk, doneChunk,
                         20'000'000, /* blockSize */
                         splitter);

        if (deferredEmptyLineNumber != -1
            && deferredEmptyLineNumber < lineCount - 1) {
            handleError("empty line", deferredEmptyLineNumber, 0, "");
        }
        
        // Accumulate any from the end
        accum.forEach([&] (ThreadAccum * accum)
                      {
                          byteCount += accum->bytesDone;
                          lineCount += accum->linesDone;
                      });
        
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
        
        numLineErrors = numSkipped;
        rowCount = lineCount;
    }
};


/*****************************************************************************/
/* IMPORT TEXT PROCEDURE                                                     */
/*****************************************************************************/

ImportTextProcedure::
ImportTextProcedure(MldbEngine * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->config = config.params.convert<ImportTextConfig>();
}

Any
ImportTextProcedure::
getStatus() const
{
    return Any();

}

RunOutput
ImportTextProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(config, run);

    DatasetBuilder builder;
    builder.initialize(*engine, logger, runProcConf, onProgress);

    ImportTextProcedureWorkInstance instance;
    instance.loadText(builder, config);

    Json::Value status;
    status["numLineErrors"] = instance.numLineErrors;
    status["rowCount"] = instance.rowCount;

    return Any(status);
}

namespace {

RegisterProcedureType<ImportTextProcedure, ImportTextConfig>
regImportText(builtinPackage(),
      "Import from a text file, line by line.",
      "procedures/importtextprocedure.md.html");

} // file scope

} //MLDB


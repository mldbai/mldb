/** importext_procedure.cc
    Mathieu Marquis Bolduc, February 12, 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Procedure that reads text files into an indexed dataset.
*/

#include "importtext_procedure.h"
#include "mldb/arch/timers.h"
#include "mldb/jml/utils/csv.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/plugins/for_each_line.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/dataset_context.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/utils/progress.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/utils/log.h"


using namespace std;


namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(ImportTextConfig);

ImportTextConfigDescription::ImportTextConfigDescription()
{
    addField("dataFileUrl", &ImportTextConfig::dataFileUrl,
             "URL of the text data to import");
    addField("outputDataset", &ImportTextConfig::outputDataset,
             "Dataset to record the data into.",
             PolyConfigT<Dataset>().withType("tabular"));
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
    addField("select", &ImportTextConfig::select,
             "Which columns to use.",
             SelectExpression::STAR);
    addField("where", &ImportTextConfig::where,
             "Which lines to use to create rows.",
             SqlExpression::TRUE);
    addField("named", &ImportTextConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name.",
             SqlExpression::parse("lineNumber()"));
    addField("timestamp", &ImportTextConfig::timestamp,
             "Expression for row timestamp.",
             SqlExpression::parse("fileTimestamp()"));
    addField("allowMultiLines", &ImportTextConfig::allowMultiLines,
             "Allows columns with multi-line quoted strings. "
             "This option disables many optimizations and makes the procedure "
             "run much slower. Only use if necessary. The `offset` parameter "
             "will not be reliable when this is activated.", false);
    addField("autoGenerateHeaders", &ImportTextConfig::autoGenerateHeaders,
             "If true, the indexes of the columns will be used to name them."
             "This cannot be set to true if headers is defined.",
             false);

    addParent<ProcedureConfig>();
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
/* SQL CSV SCOPE                                                             */
/*****************************************************************************/

/** This allows an SQL expression to be bound to a parsed CSV row, which
    allowing it to find the variables, etc.
*/

struct SqlCsvScope: public SqlExpressionMldbScope {

    struct RowScope: public SqlRowScope {
        RowScope(const CellValue * row, Date ts, int64_t lineNumber,
                 int64_t lineOffset)
            : row(row), ts(ts), lineNumber(lineNumber), lineOffset(lineOffset)
        {
        }

        const CellValue * row;
        Date ts;
        int64_t lineNumber;
        int64_t lineOffset;
        const RowPath * rowName;
    };

    SqlCsvScope(MldbServer * server,
                const std::vector<ColumnPath> & columnNames,
                Date fileTimestamp, Utf8String dataFileUrl)
        : SqlExpressionMldbScope(server), columnNames(columnNames),
          fileTimestamp(fileTimestamp),
          dataFileUrl(std::move(dataFileUrl))
    {
        columnsUsed.resize(columnNames.size(), false);
        lineNumberUsed = false;
    }

    /// Column names passed in to the scope
    const std::vector<ColumnPath> & columnNames;

    /// Which columns are accessed by the bound expressions?
    std::vector<int> columnsUsed;

    /// Is the line number required by the bound expression?  Some optimizations
    /// can be turned off if not.
    bool lineNumberUsed;

    /// What is the timestamp for the actual file itself?  This is used as a
    /// default timestamp on values returned.
    Date fileTimestamp;

    /// What is the URI for this file?
    Utf8String dataFileUrl;

    virtual ColumnGetter doGetColumn(const Utf8String & tableName,
                                     const ColumnPath & columnName)
    {
        if (!tableName.empty()) {
            throw HttpReturnException(400, "Unknown table name in import.text procedure",
                                      "tableName", tableName);
        }

        int index = std::find(columnNames.begin(), columnNames.end(), columnName)
            - columnNames.begin();
        if (index == columnNames.size())
            throw HttpReturnException(400, "Unknown column name in import.text procedure",
                                      "columnName", columnName,
                                      "knownColumnNames", columnNames);

        columnsUsed[index] = true;

        return {[=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = scope.as<RowScope>();
                    return storage = ExpressionValue(row.row[index], row.ts);
                },
                std::make_shared<AtomValueInfo>()};
    }

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter& keep)
    {
        vector<ColumnPath> toKeep;
        std::vector<KnownColumn> columnsWithInfo;

        for (unsigned i = 0;  i < columnNames.size();  ++i) {
            const ColumnPath & columnName = columnNames[i];
            ColumnPath outputName(keep(columnName));

            bool keep = !outputName.empty();
            toKeep.emplace_back(outputName);
            if (keep) {
                columnsUsed[i] = true;
                columnsWithInfo.emplace_back(outputName,
                                             std::make_shared<AtomValueInfo>(),
                                             COLUMN_IS_DENSE);
            }
        }

        // Fill out the offset so we know where it is in the input
        for (size_t i = 0;  i < columnsWithInfo.size();  ++i) {
            columnsWithInfo[i].offset = i;
        }
        
        auto exec = [=] (const SqlRowScope & scope, const VariableFilter & filter)
            {
                /* 
                   The filter parameter here is not used since this context is
                   only used when importing tabular data and there is no way to 
                   specify a timestamp for this data.
                */

                auto & row = scope.as<RowScope>();

                RowValue result;

                for (unsigned i = 0;  i < columnNames.size();  ++i) {
                    if (!toKeep[i].empty())
                        result.emplace_back(columnNames[i], row.row[i], row.ts);
                }

                return result;
            };

        GetAllColumnsOutput result;
        result.exec = exec;
        result.info = std::make_shared<RowValueInfo>(std::move(columnsWithInfo),
                                                     SCHEMA_CLOSED);
        return result;
    }

    virtual BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope)
    {
        if (functionName == "lineNumber") {
            lineNumberUsed = true;
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                    {
                        auto & row = scope.as<RowScope>();
                        return ExpressionValue(row.lineNumber, fileTimestamp);
                    },
                    std::make_shared<IntegerValueInfo>()
                };
        }
        else if (functionName == "rowHash") {
            lineNumberUsed = true;
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                    {
                        auto & row = scope.as<RowScope>();
                        if(!row.rowName) {
                            throw MLDB::Exception("rowHash() not available in this scope");
                        }
                        return ExpressionValue(row.rowName->hash(), fileTimestamp);
                    },
                    std::make_shared<IntegerValueInfo>()
                };
        }
        else if (functionName == "fileTimestamp") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                    {
                        return ExpressionValue(fileTimestamp, fileTimestamp);
                    },
                    std::make_shared<TimestampValueInfo>()
                };
        }
        else if (functionName == "dataFileUrl") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                    {
                        return ExpressionValue(dataFileUrl, fileTimestamp);
                    },
                    std::make_shared<Utf8StringValueInfo>()
                };
        }
        else if (functionName == "lineOffset") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                    {
                        auto & row = scope.as<RowScope>();
                        return ExpressionValue(row.lineOffset, fileTimestamp);
                    },
                    std::make_shared<IntegerValueInfo>()
                };
        }
        return SqlBindingScope::doGetFunction(tableName, functionName, args,
                                              argScope);
    }

    static RowScope bindRow(const CellValue * row, Date ts,
                            int64_t lineNumber, int64_t lineOffset)
    {
        return RowScope(row, ts, lineNumber, lineOffset);
    }
};


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

namespace {

enum Encoding {
    ASCII,
    UTF8,
    LATIN1
};

Encoding parseEncoding(const std::string & encodingStr_)
{
    std::string encodingStr;
    for (auto & c: encodingStr_)
        encodingStr += tolower(c);

    Encoding encoding;
    if (encodingStr == "us-ascii" || encodingStr == "ascii") {
        encoding = ASCII;
    }
    else if (encodingStr == "utf-8" || encodingStr == "utf8") {
        encoding = UTF8;
    }
    else if (encodingStr == "latin1" || encodingStr == "iso8859-1")
        encoding = LATIN1;
    else throw HttpReturnException(400, "Unknown encoding '" + encodingStr_
                                   + "'for import.text parser: accepted are "
                                   "'us-ascii', 'ascii', 'utf-8', 'utf8', "
                                   "'latin1', 'iso8859-1'",
                                   "encoding", encodingStr);
    return encoding;
}

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
                      Encoding encoding,
                      int replaceInvalidCharactersWith,
                      bool isTextLine,
                      bool hasQuoteChar,
                      shared_ptr<spdlog::logger> & logger)
{
    ExcAssert(!(hasQuoteChar && isTextLine));

    // Skip trailing whitespace in the row.  If we want spaces, we should use
    // quotes.
    while (length > 0 && isspace(line[length - 1]))
        --length;
    
    const char * lineEnd = line + length;

    const char * errorMsg = nullptr;

    size_t colNum = 0;

    auto finishString = [encoding,replaceInvalidCharactersWith,&logger]
        (const char * start, size_t len, bool eightBit)
        {
            TRACE_MSG(logger)
                 << "finishing string " << string(start, len)
                 << " with eightBit " << eightBit
                 << " encoding " << encoding
                 << " replaceInvalidCharactersWith " << replaceInvalidCharactersWith;

            if (!eightBit) {
                char buf[len];
                if (replaceInvalidCharactersWith >= 0) {
                    ExcAssert(replaceInvalidCharactersWith < 256);
                    start = findInvalidAscii(start, len, buf, (char)replaceInvalidCharactersWith);
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
                    char buf[len + BUF_PADDING];
                    char * end
                        = utf8::replace_invalid(start, start + len, buf,
                                                replaceInvalidCharactersWith);

                    if (end < buf || end > buf + len + BUF_PADDING) {
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

        if (colNum >= numColumns)
            return "too many columns in row";

        const char * start = line;

        char c = *line++;

        if (c == separator && !isTextLine) {
            // null field
            ++colNum;
            continue;
        }
        else if (c == quote && hasQuoteChar) {
            // quoted string
            static constexpr size_t FIXED_BUF_LEN = 4096;
            char sbuf[FIXED_BUF_LEN];  // holds the extracted string
            char * s = sbuf;
            size_t buflen = FIXED_BUF_LEN;
            std::unique_ptr<char[]> sdynamic;
            int len = 0;   // and its length

            bool eightBit = false;
            bool ok = false;

            auto pushChar = [&] (char c)
                {
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
                c = *line;
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
        else if ((isdigit(c) || c == '-') && !isTextLine) {
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
                c = *line;
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
                c = *line;
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

    if (line < lineEnd) {
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

struct ImportTextProcedureWorkInstance
{
    ImportTextProcedureWorkInstance(std::shared_ptr<spdlog::logger> logger)
        : logger(logger),
          lineOffset(1), // we start at line 1
          isTextLine(false),
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
    bool isTextLine;
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

    /*    Load a text file and filter according to the configuration  */
    void loadText(const ImportTextConfig& config,
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

        isTextLine = config.quoter.empty() && config.delimiter.empty();

        if (!config.replaceInvalidCharactersWith.empty()) {
            if (config.replaceInvalidCharactersWith.length() != 1)
                throw HttpReturnException(400, "replaceInvalidCharactersWith string must have one character");
            replaceInvalidCharactersWith = *config.replaceInvalidCharactersWith.begin();
        }

        encoding = parseEncoding(config.encoding);


        if (isTextLine) {
            //MLDB-1312 optimize if there is no delimiter: only 1 column
            if (config.headers.empty()) {
                inputColumnNames = { ColumnPath(config.autoGenerateHeaders ? 0 : "lineText") };
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

                    if(!prevHeader.empty()) {
                        prevHeader += ' ' + header;
                        header.assign(std::move(prevHeader));
                    }

                    try {
                        ParseContext pcontext(filename,
                                               header.c_str(), header.length(), 1, 0);
                        fields = expect_csv_row(pcontext, -1, separator);
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
        SqlExpressionDatasetScope noContext(*dataset, ""); //needs a context because x.* is ambiguous
        isIdentitySelect = config.select.isIdentitySelect(noContext);  

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

        loadTextData(dataset, stream, config, scope, onProgress);
    }

    /*    Load, filter and format all lines and process them  */
    void
    loadTextData(std::shared_ptr<Dataset> dataset,
                 std::istream& stream,
                 const ImportTextConfig& config,
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

        Dataset::MultiChunkRecorder recorder
            = dataset->getChunkRecorder();

        struct ThreadAccum {
            /// Recorder object for this thread that the dataset gives us
            /// to record into the dataset.
            std::unique_ptr<Recorder> threadRecorder;

            /// Special function to allow rapid insertion of fixed set of
            /// atom valued columns.  Only for isIdentitySelect.
            std::function<void (RowPath rowName,
                                Date timestamp,
                                CellValue * vals,
                                size_t numVals,
                                std::vector<std::pair<ColumnPath, CellValue> > extra)>
            specializedRecorder;

        };

        PerThreadAccumulator<ThreadAccum> accum;

        auto startChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                DEBUG_MSG(logger)
                    << "started chunk " << chunkNumber << " at line " << lineNumber;
                auto & threadAccum = accum.get();
                threadAccum.threadRecorder = recorder.newChunk(chunkNumber);
                if (isIdentitySelect)
                    threadAccum.specializedRecorder
                        = threadAccum.threadRecorder
                        ->specializeRecordTabular(inputColumnNames);
                return true;
            };

        auto doneChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                DEBUG_MSG(logger) << "finished chunk " << chunkNumber;
                auto & threadAccum = accum.get();
                ExcAssert(threadAccum.threadRecorder.get());
                threadAccum.threadRecorder->finishedChunk();
                threadAccum.threadRecorder.reset(nullptr);
                threadAccum.specializedRecorder = nullptr;
                return true;
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

            // MLDB-1111 empty lines are treated as error
            if (length == 0)
                return handleError("empty line", actualLineNum, 0, "");


            // Values that come in from the CSV file
            // TODO: clang doesn't like a variable length array
            // here.  Find another way to allocate it on the
            // stack.
            vector<CellValue> values(inputColumnNames.size());

            const char * lineStart = line;

            const size_t numInputColumn = inputColumnNames.size();

            const char * errorMsg
                    = parseFixedWidthCsvRow(line, length, &values[0],
                                            numInputColumn,
                                            separator, quote, encoding,
                                            replaceInvalidCharactersWith,
                                            isTextLine,
                                            hasQuoteChar, logger);

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

            auto row = scope.bindRow(&values[0], ts, actualLineNum,
                                         0 /* todo: chunk ofs */);

            ExpressionValue nameStorage;
            RowPath rowName(namedBound(row, nameStorage, GET_ALL)
                                .toUtf8String());
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

            auto & threadAccum = accum.get();

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


/*****************************************************************************/
/* IMPORT TEXT PROCEDURE                                                     */
/*****************************************************************************/

ImportTextProcedure::
ImportTextProcedure(MldbServer * owner,
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

    std::shared_ptr<Dataset> dataset
        = createDataset(server, runProcConf.outputDataset, onProgress,
                        true /*overwrite*/);

    ImportTextProcedureWorkInstance instance(logger);

    instance.loadText(config, dataset, server, onProgress);

    Json::Value status;
    status["numLineErrors"] = instance.numLineErrors;
    status["rowCount"] = instance.rowCount;

    dataset->commit();

    return Any(status);
}

namespace {

RegisterProcedureType<ImportTextProcedure, ImportTextConfig>
regImportText(builtinPackage(),
      "Import from a text file, line by line.",
      "procedures/importtextprocedure.md.html");

} // file scope

} //MLDB


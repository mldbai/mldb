/** importext_procedure.cc
    Mathieu Marquis Bolduc, February 12, 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Procedure that reads text files into an indexed dataset.
*/

#include "importtext_procedure.h"
#include "mldb/arch/timers.h"
#include "mldb/utils/csv.h"
#include "mldb/utils/lightweight_hash.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/builtin/for_each_line.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/base/per_thread_accumulator.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/engine/dataset_scope.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/utils/progress.h"
#include "mldb/utils/vector_utils.h"
#include "mldb/utils/log.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "sql_csv_scope.h"
#include "mldb/base/parse_context.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/base/optimized_path.h"


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
            "first character position.  Default skips no lines.  This option applies "
            "to the text, not to the CSV rows, and so may interact strangely with the "
            "`allowMultiLines` option.");

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
    else throw AnnotatedException(400, "Unknown encoding '" + encodingStr_
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
                      const shared_ptr<spdlog::logger> & logger,
                      bool ignoreExtraColumns,
                      bool processExcelFormulas)
{
    ExcAssert(!(hasQuoteChar && isTextLine));

    // Skip trailing whitespace in the row.  If we want spaces, we should use
    // quotes.
    while (length > 0 && isspace(line[length - 1]))
        --length;
    
    const char * lineEnd = line + length;

    const char * errorMsg = nullptr;

    size_t colNum = 0;

    auto finishString = [encoding,replaceInvalidCharactersWith]
        (const char * start, size_t len, bool eightBit)
        {
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

// Allow the move into place optimization to be turned on or off to aid
// in unit esting.

static OptimizedPath moveIntoOutputs("mldb.textual.importText.moveIntoOutputs");
    
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
    LightweightHash<ColumnHash, int> inputColumnIndex;
    LightweightHash<ColumnHash, int> columnIndex; //To check for duplicates column names
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
                  MldbEngine * engine,
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

        encoding = parseEncoding(config.encoding);

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
        for (unsigned i = 0;  i < inputColumnNames.size();  ++i) {
            const ColumnPath & c = inputColumnNames[i];
            ColumnHash ch(c);
            if (!inputColumnIndex.insert(make_pair(ch, i)).second)
                throw AnnotatedException(400, "Duplicate column name in CSV file",
                                          "columnName", c);
        }

        // Now we know the columns, we can bind our SQL expressions for the
        // select, where, named and timestamp parts of the expression.
        SqlCsvScope scope(engine, inputColumnNames, ts,
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

        // Figure out the effect of each of the clauses in the select
        // statement

        cerr << "select is " << config.select.print() << endl;
        auto children = config.select.getChildren();
        cerr << "has " << children.size() << " children" << endl;
        if (selectBound.decomposition) {
            cerr << "decomposition has " << selectBound.decomposition->size()
                 << " entries" << endl;
            cerr << jsonEncode(selectBound.decomposition) << endl;
        }
        else cerr << "no decomposition available" << endl;

        for (unsigned i = 0;  i < cols.size();  ++i) {
            const auto& col = cols[i];
            if (!col.valueInfo->isScalar())
                throw AnnotatedException
                    (400,
                     "Import select expression cannot have row-valued columns.",
                     "select", config.select,
                     "selectOutputInfo", selectBound.info,
                     "columnName", col.columnName);

            ColumnHash ch(col.columnName);
            if (!columnIndex.insert(make_pair(ch, i)).second)
                throw AnnotatedException(400, "Duplicate column name in select expression",
                                          "columnName", col.columnName);

            knownColumnNames.emplace_back(col.columnName);
        }

        if (isIdentitySelect)
            ExcAssertEqual(inputColumnNames, knownColumnNames);

        INFO_MSG(logger)
            << "reading " << inputColumnNames.size() << " columns "
            << jsonEncodeStr(inputColumnNames);

        INFO_MSG(logger)
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

        // Describes any operations that transform a single column into
        // another single column; these can be run in-place.
        // This is in order of the input columns; only single input to
        // single output operations are run.
        struct ColumnOperation {
            int clauseNum = -1;      ///< Which index in decomposed we belong to?
            int outCol = -1;         ///< Output column number
            std::vector<int> inputCols;
            bool moveInputs = false;   ///< Can we move inputs into place?
            BoundSqlExpression bound;  ///< Expression to compute column; null mesans
        };

        // We split into three different types of operations, run in
        // order:
        // 1.  Clauses that can't be optimized generically
        // 2.  Clauses that run a function over the inputs returning a single col
        // 3.a Clauses that copy a single input to a single output
        // 3.b Clauses that move a single intput to a single output

        
        std::vector<BoundSqlExpression> otherClauses;
        std::vector<ColumnOperation> ops;

        bool canUseDecomposed = false;

        if (selectBound.decomposition) {
            canUseDecomposed = true;

            // Which is the last clause using each input?
            std::map<Path, int> lastClauseUsingInput;
            
            for (size_t i = 0;  i < selectBound.decomposition->size();  ++i) {
                auto & d = (*selectBound.decomposition)[i];

                // What can be optimized?
                // select input_column [as output_name] --> copy or move in place
                // select expr(input_column) [ as output_name ] --> run destructive
                
                cerr << "scanning clause " << i << ": " << jsonEncode(d) << endl;
                
                // Only single input to single output clauses can be run
                // in place like this
                // TODO: later, we can expand to multiple-input clauses...
                if (d.inputs.size() != 1 || d.inputWildcards.size() != 0
                    || d.outputs.size() != 1 || d.outputWildcards) {
                    cerr << "    clause fails" << endl;
                    canUseDecomposed = false;
                    otherClauses.emplace_back(d.expr->bind(scope));
                    continue;
                }

                // Operation to execute to run this column
                ColumnOperation exec;
                exec.clauseNum = i;
                
                cerr << "clause passes" << endl;

                // Record whether we're the last input or not
                for (const Path & input: d.inputs) {
                    lastClauseUsingInput[input] = i;
                }
                
                ColumnPath inputName = *d.inputs.begin();
                ColumnPath outputName = d.outputs[0];

                auto it = inputColumnIndex.find(inputName);
            
                if (it == inputColumnIndex.end()) {
                    otherClauses.emplace_back(d.expr->bind(scope));
                    continue;
                }

                int inputIndex = it->second;

                it = columnIndex.find(outputName);
                if (it == columnIndex.end())
                    throw AnnotatedException(500, "Output column name not found");
            
                int outputIndex = it->second;

                exec.inputCols.emplace_back(inputIndex);
                exec.outCol = outputIndex;

                if (d.expr->getType() == "selectExpr") {
                    // simple copy
                    auto op
                        = static_cast<const NamedColumnExpression *>(d.expr.get());

                    // Simply reading a variable can just make a copy.  Later
                    // we see if we can move instead of copy.
                    if (op->expression->getType() == "variable") {
                        const ColumnPath & varName
                            = static_cast<const ReadColumnExpression *>
                            (op->expression.get())->columnName;

                        ExcAssertEqual(varName, inputName);
                        
                        cerr << "op " << ops.size() << ": copy "
                             << inputName << " at " << inputIndex
                             << " to " << outputName << " at " << outputIndex
                             << endl;
                        ops.emplace_back(std::move(exec));
                        continue;
                    }
                    
                    // Bind a much simpler value
                    exec.bound = op->expression->bind(scope);
                
                    cerr << "op " << ops.size() << ": compute "
                         << outputName << " at " << outputIndex
                         << " from " << inputName << " with "
                         << op->expression->print() << endl;

                    ops.emplace_back(std::move(exec));
                    continue;
                }

                cerr << "*** not a select ***" << endl;
            
                cerr << "warning: operation " << d.expr->print() << " unhandled"
                     << endl;

                otherClauses.emplace_back(d.expr->bind(scope));
                canUseDecomposed = false;
                ExcAssert(false);
            }

            if (moveIntoOutputs.take()) {
                // Analyze if we're the last operation using a given input.
                // If so, we can move our inputs to our outputs instead of
                // copying them.   This is a pure optimization.
                for (auto & op: ops) {
                    int clauseNum = op.clauseNum;
                    const auto & d = (*selectBound.decomposition)[clauseNum];
                    op.moveInputs = true;
                    for (const Path & input: d.inputs) {
                        int lastUsed = lastClauseUsingInput[input];
                        if (lastUsed > clauseNum) {
                            op.moveInputs = false;
                            break;
                        }
                    }

                    if (op.moveInputs) {
                        cerr << "turning clause " << clauseNum
                             << " from copy into move" << endl;
                    }
                }
            }
        }
        
        cerr << "with " << otherClauses.size() << " other clauses" << endl;
        cerr << "canUseDecomposed = " << canUseDecomposed << endl;
        
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

            throw AnnotatedException(400, "Error parsing CSV row: "
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
                auto & threadAccum = accum.get();
                threadAccum.threadRecorder = recorder.newChunk(chunkNumber);
                if (isIdentitySelect || canUseDecomposed)
                    threadAccum.specializedRecorder
                        = threadAccum.threadRecorder
                        ->specializeRecordTabular(knownColumnNames);
                return true;
            };

        auto doneChunk = [&] (int64_t chunkNumber, size_t lineNumber)
            {
                auto & threadAccum = accum.get();
                ExcAssert(threadAccum.threadRecorder.get());
                threadAccum.threadRecorder->finishedChunk();
                threadAccum.threadRecorder.reset(nullptr);
                threadAccum.specializedRecorder = nullptr;
                return true;
            };

        cerr << "outputColumnNames = " << jsonEncode(knownColumnNames) << endl;
        
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
                if (regex_match(line, length,
                                config.skipLineRegex))
                    return true;
            }
            
            // MLDB-1111 empty lines are treated as error
            if (length == 0)
                return handleError("empty line", actualLineNum, 0, "");


            // Values that come in from the CSV file
            PossiblyDynamicBuffer<CellValue> values(inputColumnNames.size());

            const char * lineStart = line;

            const size_t numInputColumn = inputColumnNames.size();

            const char * errorMsg
                    = parseFixedWidthCsvRow(line, length, values.data(),
                                            numInputColumn,
                                            separator, quote, encoding,
                                            replaceInvalidCharactersWith,
                                            isTextLine,
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
                // select clause.  We simply record the values directly.
                threadAccum.specializedRecorder(std::move(rowName),
                                                rowTs, values.data(),
                                                values.size(), {});
            }
            else if (canUseDecomposed) {
                std::vector<CellValue> outputValues(knownColumnNames.size());
                std::vector<bool> outputValueSet(knownColumnNames.size(), false);

                // Apply the operations one by one
                for (size_t i = 0;  i < ops.size();  ++i) {
                    const ColumnOperation & op = ops[i];

                    // Process this input
                    if (op.bound) {
                        ExpressionValue opStorage;
                        const ExpressionValue & newVal
                            = op.bound(row, opStorage, GET_ALL);

                        // We record the output if it is used.  This would only
                        // not be true for operations that have no output but
                        // do cause side effects.
                        if (op.outCol != -1) {
                            if (&newVal == &opStorage)
                                outputValues[op.outCol] = opStorage.stealAtom();
                            else outputValues[op.outCol] = newVal.getAtom();
                            outputValueSet[op.outCol] = true;
                        }
                    }
                    else {
                        ExcAssertEqual(op.inputCols.size(), 1);
                        int inCol = op.inputCols[0];

                        // Copy or move value directly from input to output
                        if (op.moveInputs) {
                            outputValues[op.outCol] = std::move(values[inCol]);
                        }
                        else {
                            outputValues[op.outCol] = values[inCol];
                        }
                        outputValueSet[op.outCol] = true;
                    }
                }

                // Extra values we couldn't analyze statically, which will
                // be recorded when one of the columns has extra values
                std::vector<std::pair<ColumnPath, CellValue> > extra;
                
                for (auto & clause: otherClauses) {
                    ExpressionValue clauseStorage;
                    const ExpressionValue & clauseOutput
                        = clause(row, clauseStorage, GET_ALL);

                    // This must be a row output, since it's going to be
                    // merged together
                    if (&clauseOutput == &clauseStorage) {
                        auto recordAtom = [&] (Path & columnName,
                                               CellValue & value,
                                               Date ts) -> bool
                            {
                                // Is it the first time we've set this column?
                                auto it = columnIndex.find(columnName);
                                if (it != columnIndex.end()
                                    && !outputValueSet.at(it->second)) {
                                    // If so, put it in output values
                                    outputValues[it->second] = std::move(value);
                                    outputValueSet[it->second] = true;
                                }
                                else {
                                    // If not, put it in the extra values
                                    extra.emplace_back(std::move(columnName),
                                                       std::move(value));
                                }
                                return true;
                            };
                        
                        clauseStorage.forEachAtomDestructive(recordAtom);
                    }
                    else {
                        // We don't own the output, so we need to copy
                        // things before we record them.
                        
                        auto recordAtom = [&] (const Path & columnName_,
                                               const Path & prefix,
                                               const CellValue & value,
                                               Date ts) -> bool
                            {
                                Path columnName2;
                                if (!prefix.empty()) {
                                    columnName2 = prefix + columnName_;
                                }
                                const Path & columnName
                                    = prefix.empty()
                                    ? columnName_
                                    : columnName2;
                                
                                // Is it the first time we've set this column?
                                auto it = columnIndex.find(columnName);
                                if (it != columnIndex.end()
                                    && !outputValueSet.at(it->second)) {
                                    // If so, put it in output values
                                    outputValues[it->second] = value;
                                    outputValueSet[it->second] = true;
                                }
                                else {
                                    // If not, put it in the extra values
                                    if (prefix.empty()) {
                                        extra.emplace_back(std::move(columnName2),
                                                           value);
                                    }
                                    else {
                                        extra.emplace_back(std::move(columnName),
                                                           value);
                                    }
                                }

                                return true;
                            };
                        
                        clauseStorage.forEachAtom(recordAtom);
                        
                    }
                }
                
                threadAccum.specializedRecorder(std::move(rowName),
                                                rowTs, outputValues.data(),
                                                outputValues.size(),
                                                std::move(extra));
            }
            else {
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

    std::shared_ptr<Dataset> dataset
        = createDataset(engine, runProcConf.outputDataset, onProgress,
                        true /*overwrite*/);

    ImportTextProcedureWorkInstance instance(logger);

    instance.loadText(config, dataset, engine, onProgress);

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


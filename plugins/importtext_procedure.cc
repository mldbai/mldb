/** csv_dataset.cc
    Mathieu Marquis Bolduc, February 12, 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
    
    Procedure that reads text files into an indexed dataset.
*/

#include "importtext_procedure.h"
#include "mldb/arch/timers.h" 
#include "mldb/jml/utils/csv.h"    
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/plugins/for_each_line.h"
#include "mldb/plugins/tabular_dataset.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/function_collection.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/server/procedure_collection.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/server/dataset_context.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"


using namespace std;

namespace Datacratic {
namespace MLDB {

static constexpr size_t ROWS_PER_CHUNK=65536;

DEFINE_STRUCTURE_DESCRIPTION(ImportTextConfig);

ImportTextConfigDescription::ImportTextConfigDescription()
{
    addParent<ProcedureConfig>();    
    addField("dataFileUrl", &ImportTextConfig::dataFileUrl,
             "URL of the text data to import");
    addField("ouputDataset", &ImportTextConfig::ouputDataset,
             "Dataset to record the data into",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("headers", &ImportTextConfig::headers,
             "List of headers for when first row doesn't contain headers",
             vector<Utf8String>());
    addField("quotechar", &ImportTextConfig::quoter,
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
             "If true, any line causing an error will be skipped. "
             "Empty lines are considered bad lines.", false);
    addField("replaceInvalidCharactersWith",
             &ImportTextConfig::replaceInvalidCharactersWith,
             "If this is set, it should be a single Unicode character that badly "
             "encoded characters within the CSV file will be replaced with. "
             "The default is nothing, which will cause lines with badly "
             "encoded characters to throw an error.");
    addField("select", &ImportTextConfig::select,
             "What to select from the dataset",
             SelectExpression::STAR);
    addField("where", &ImportTextConfig::where,
             "Row filter for CSV dataset",
             SqlExpression::TRUE);
    addField("named", &ImportTextConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name.",
             SqlExpression::parse("lineNumber()"));
    addField("timestamp", &ImportTextConfig::timestamp,
             "Expression for row timestamp.",
             SqlExpression::parse("fileTimestamp()"));

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
            else {
                context.exception("Unknown field '" + context.fieldName()
                                  + " parsing CSV dataset configuration");
            }
        };
}


/*****************************************************************************/
/* SQL CSV SCOPE                                                             */
/*****************************************************************************/

/** This allows an SQL expression to be bound to a parsed CSV row, which
    allowing it to find the variables, etc.
*/

struct SqlCsvScope: public SqlExpressionMldbContext {

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
    };

    SqlCsvScope(MldbServer * server, const std::vector<ColumnName> & columnNames,
                Date fileTimestamp, Utf8String dataFileUrl)
        : SqlExpressionMldbContext(server), columnNames(columnNames),
          fileTimestamp(fileTimestamp),
          dataFileUrl(std::move(dataFileUrl))
    {
        columnsUsed.resize(columnNames.size(), false);
        lineNumberUsed = false;
    }

    /// Column names passed in to the scope
    const std::vector<ColumnName> & columnNames;

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

    virtual VariableGetter doGetVariable(const Utf8String & tableName,
                                         const Utf8String & variableName)
    {
        if (!tableName.empty()) {
            throw HttpReturnException(400, "Unknown table name in CSV dataset",
                                      "tableName", tableName);
        }

        int index = std::find(columnNames.begin(), columnNames.end(), variableName)
            - columnNames.begin();
        if (index == columnNames.size())
            throw HttpReturnException(400, "Unknown column name in CSV dataset",
                                      "columnName", variableName,
                                      "knownColumnNames", columnNames);

        columnsUsed[index] = true;

        return {[=] (const SqlRowScope & scope,
                     ExpressionValue & storage,
                     const VariableFilter & filter) -> const ExpressionValue &
                {
                    auto & row = static_cast<const RowScope &>(scope);
                    return storage = std::move(ExpressionValue(row.row[index], row.ts));
                },
                std::make_shared<AtomValueInfo>()};
    }

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    std::function<Utf8String (const Utf8String &)> keep)
    {
        vector<ColumnName> toKeep;
        std::vector<KnownColumn> columnsWithInfo;

        for (unsigned i = 0;  i < columnNames.size();  ++i) {
            const ColumnName & columnName = columnNames[i];
            ColumnName outputName(keep(columnName.toUtf8String()));

            bool keep = outputName != ColumnName();
            toKeep.emplace_back(outputName);
            if (keep) {
                columnsUsed[i] = true;
                columnsWithInfo.emplace_back(outputName,
                                             std::make_shared<AtomValueInfo>(),
                                             COLUMN_IS_DENSE);
            }
        }
        
        auto exec = [=] (const SqlRowScope & scope)
            {
                auto & row = static_cast<const RowScope &>(scope);

                RowValue result;

                for (unsigned i = 0;  i < columnNames.size();  ++i) {
                    if (toKeep[i] != ColumnName())
                        result.emplace_back(columnNames[i], row.row[i], row.ts);
                }

                return std::move(result);
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
                  const std::vector<std::shared_ptr<SqlExpression> > & args)
    {
        if (functionName == "lineNumber") {
            lineNumberUsed = true;
            return {[=] (const std::vector<BoundSqlExpression> & args,
                         const SqlRowScope & scope)
                    {
                        auto & row = static_cast<const RowScope &>(scope);
                        return ExpressionValue(row.lineNumber, fileTimestamp);
                    },
                    std::make_shared<IntegerValueInfo>()
                    };
        }
        else if (functionName == "fileTimestamp") {
            return {[=] (const std::vector<BoundSqlExpression> & args,
                         const SqlRowScope & scope)
                    {
                        return ExpressionValue(fileTimestamp, fileTimestamp);
                    },
                    std::make_shared<TimestampValueInfo>()
                    };
        }
        else if (functionName == "dataFileUrl") {
            return {[=] (const std::vector<BoundSqlExpression> & args,
                         const SqlRowScope & scope)
                    {
                        return ExpressionValue(dataFileUrl, fileTimestamp);
                    },
                    std::make_shared<Utf8StringValueInfo>()
                    };
        }
        else if (functionName == "lineOffset") {
            return {[=] (const std::vector<BoundSqlExpression> & args,
                         const SqlRowScope & scope)
                    {
                        auto & row = static_cast<const RowScope &>(scope);
                        return ExpressionValue(row.lineOffset, fileTimestamp);
                    },
                    std::make_shared<IntegerValueInfo>()
                    };
        }
        return SqlBindingScope::doGetFunction(tableName, functionName, args);
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

Encoding parseEncoding(const std::string & encodingStr)
{
    Encoding encoding;
    if (encodingStr == "us-ascii" || encodingStr == "ascii") {
        encoding = ASCII;
    }
    else if (encodingStr == "utf-8" || encodingStr == "utf8") {
        encoding = UTF8;
    }
    else if (encodingStr == "latin1" || encodingStr == "iso8859-1")
        encoding = LATIN1;
    else throw HttpReturnException(400, "Unknown encoding for CSV parser",
                                   "encoding", encodingStr);
    return encoding;
}

} // file scope

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
                      bool hasQuoteChar)
{
    ExcAssert(!(hasQuoteChar && isTextLine));

    const char * lineEnd = line + length;

    const char * errorMsg = nullptr;

    size_t colNum = 0;

    //cerr << "parsing line " << string(line, length) << endl;

    auto finishString = [encoding,replaceInvalidCharactersWith]
        (const char * start, size_t len, bool eightBit)
        {
            //cerr << "finishing string " << string(start, len) << " with eightBit " << eightBit << " and encoding " << encoding << endl;

            if (!eightBit) {
                return CellValue::parse(start, len, STRING_IS_VALID_ASCII);
            }

            // Parse differently based upon encoding
            switch (encoding) {
            case ASCII:
                throw ML::Exception("non-ASCII character in ASCII CSV file");
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
                //cerr << "c = " << c << endl;
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
                errorMsg = "Unclosed quoted CSV value";

            if (errorMsg)
                break;

            //cerr << "eightBit = " << eightBit << endl;
            //cerr << "parsing " << string(s, len) << endl;
            values[colNum++] = finishString(s, len, eightBit);

            //cerr << "after quoted, *line = " << *line << endl;
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

        //cerr << "added col " << (colNum - 1) << " val " << values[colNum - 1] << endl;
    }

    if (errorMsg)
        return errorMsg;

    if (line < lineEnd) {
        return "too many columns in row";
    }

    if (colNum != numColumns)
        return "not enough columns in row";

    return errorMsg;
}

/*****************************************************************************/
/* IMPORT TEXT PROCEDURE WORK INSTANCE                                       */
/*****************************************************************************/

struct ImportTextProcedureWorkInstance
{
	ImportTextProcedureWorkInstance() : lineOffset(1), // we start at line 1
										isTextLine(false),
										separator(0),
										quote(0),
										replaceInvalidCharactersWith(-1),
										hasQuoteChar(false),
										isIdentitySelect(false),
										rowCount(0),
										numLineErrors(0)
	{

	}

	vector<ColumnName> columnNames;
	ML::Lightweight_Hash<ColumnHash, int> columnIndex; //To check for duplicates column names
	int64_t lineOffset;  
	// Column names in the CSV file.  This is distinct from the
    // output column names that will be created once parsing has
    // happened.
    vector<ColumnName> inputColumnNames;
    bool isTextLine;
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

	void loadText(const ImportTextConfig& config, std::shared_ptr<Dataset> dataset, MldbServer * server)
	{
		cerr << "LOAD TEXT" << endl;

		string filename = config.dataFileUrl.toString();
        
    	// Ask for a memory mappable stream if possible
    	ML::filter_istream stream(filename, { { "mapped", "true" } });

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

	    if (isTextLine)
	    {
	        //MLDB-1312 optimize if there is no delimiter: only 1 column
	        if (config.headers.empty()) {
	            inputColumnNames = { ColumnName("lineText") };
	        }
	        else
	        {
	            if (inputColumnNames.size() != 1)
	                throw HttpReturnException(400, "Custom CSV header must have only one element if there is no delimiter");
	        }
	    }
	    else
	    {   
	        if (config.headers.empty()) {
	            // Read header line
	            std::getline(stream, header);
	            lineOffset += 1;
	            ML::Parse_Context pcontext(filename, 
	                                       header.c_str(), header.length(), 1, 0);
	            
	            vector<string> fields = ML::expect_csv_row(pcontext, -1, separator);

	            switch (encoding) {
	            case ASCII:
	                for (const auto & f: fields)
	                    inputColumnNames.emplace_back(ColumnName(f));
	                break;
	            case UTF8:
	                for (const auto & f: fields)
	                    inputColumnNames.emplace_back(ColumnName(Utf8String(f)));
	                break;
	            case LATIN1:
	                for (const auto & f: fields)
	                    inputColumnNames.emplace_back(ColumnName(Utf8String::fromLatin1(f)));
	                break;
	            };
	        }
	        else {
	            for (const auto & f: config.headers)
	                inputColumnNames.emplace_back(ColumnName(f));
	        }             
	    }
	    
	  //  std::vector<ColumnHash> columnHashes;

	    for (unsigned i = 0;  i < inputColumnNames.size();  ++i) {
	            const ColumnName & c = inputColumnNames[i];
	            ColumnHash ch(c);
	            if (!columnIndex.insert(make_pair(ch, i)).second)
	                throw HttpReturnException(400, "Duplicate column name in CSV file",
	                                          "columnName", c.toString());
	           // columnHashes.push_back(ch);
	        }

	    // Now we know the columns, we can bind our SQL expressions for the
	    // select, where, named and timestamp parts of the expression.
	    SqlCsvScope scope(server, inputColumnNames, ts,
	                      Utf8String(config.dataFileUrl.toString()));
	    
	    selectBound = config.select.bind(scope);
	    whereBound = config.where->bind(scope);
	    namedBound = config.named->bind(scope);
	    timestampBound = config.timestamp->bind(scope);

	    // Do we have a "select *"?  In that case, we can perform various
	    // optimizations to avoid calling into the SQL layer
	    SqlExpressionDatasetContext noContext(*dataset, ""); //needs a context because x.* is ambiguous
	    isIdentitySelect = config.select.isIdentitySelect(noContext);  

	    // Is the name the lineNumber()?  If so, we can save on
	    // calculating it
	    //cerr << "name = " << config.named->print() << endl;

	    // Figure out our output column names from the bound
	    // select clause

	    if (selectBound.info->getSchemaCompleteness()
	        != SCHEMA_CLOSED) {
	        throw HttpReturnException
	            (400,
	             "CSV dataset select expression cannot create extra columns in its expressions or have row-valued columns.",
	             "select", config.select,
	             "selectOutputInfo", selectBound.info);
	    }

	    auto cols = selectBound.info->getKnownColumns();
	    
	    for (auto & col: cols) {
	        if (!col.valueInfo->isScalar())
	            throw HttpReturnException
	                (400,
	                 "CSV dataset select expression cannot have row-valued columns.",
	                 "select", config.select,
	                 "selectOutputInfo", selectBound.info,
	                 "columnName", col.columnName);
	        
	        columnNames.emplace_back(col.columnName);
	    }

	    if (isIdentitySelect)
	        ExcAssertEqual(inputColumnNames, columnNames);

	    //cerr << "selectBound.info = " << jsonEncode(selectBound.info)
	    //     << endl;

	    //int rowNameColumnIndex = getRowNameHeaderIndex();

	    cerr << "reading " << inputColumnNames.size() << " columns "
	         << jsonEncodeStr(inputColumnNames) << endl;

	    cerr << "writing " << columnNames.size() << " columns "
	         << jsonEncodeStr(columnNames) << endl;

	    std::string line;

	    // Skip those up to the offset
	    for (size_t i = 0;  stream && i < config.offset;  ++i, ++lineOffset) {
	        getline(stream, line);
	    }

	    Date start = Date::now();

	    std::shared_ptr<TabularDataset> tabular = dynamic_pointer_cast<TabularDataset>(dataset);

	    if (tabular)
	    {
	    	loadToTabularDataset(tabular, stream, config, scope);
	    }
	    else
	    {
	    	loadToGeneric(dataset, stream, config, scope);
	    }    

	    Date end = Date::now();

	    double elapsed = start.secondsUntil(end);
	    cerr << "read " << rowCount << " lines in "
	         << elapsed << " at " << rowCount / elapsed
	         << " lines/second" << endl;
	    
	}

	void 
	loadToGeneric(std::shared_ptr<Dataset> dataset,
		          ML::filter_istream& stream,
		          const ImportTextConfig& config,
		          SqlCsvScope& scope)
	{
		cerr << "LOAD TO GENERIC" << endl;

		const size_t numberOutputColumns = columnNames.size();

		mutex lineMutex;

		//   virtual void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows);
        PerThreadAccumulator< std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > > accum;

        std::atomic<size_t> totalRows;

		auto onLine = [&] (int chunkNum, int64_t actualLineNum, RowName rowName, Date rowTs, CellValue * vals)
	    {
	    	std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows = accum.get();

	    	std::vector<std::tuple<ColumnName, CellValue, Date> > rowvalues;
	    	for (int i = 0; i < numberOutputColumns; ++i)
	    	{
	    		rowvalues.push_back( make_tuple(columnNames[i], std::move(vals[i]), rowTs) );
	    	}

	    	rows.push_back( { rowName, std::move(rowvalues) } );

	    	if (rows.size() == 1000)
	    	{
	    		{
	    			std::unique_lock<std::mutex> guard(lineMutex);
	    			dataset->recordRows(rows);
	    		}
	    		
	    		rows.clear();
	    	}
	    	++totalRows;	    	
	    };

	    loadTextData(dataset, stream, config, scope, numberOutputColumns, onLine);

	    this->rowCount = totalRows;
	}

	void 
	loadToTabularDataset(std::shared_ptr<TabularDataset> dataset, 
						 ML::filter_istream& stream, 
						 const ImportTextConfig& config,
						 SqlCsvScope& scope)
	{
		cerr << "LOAD TO TABULAR" << endl;

		const size_t numberOutputColumns = columnNames.size();

	    //This will steal columnNames
		dataset->initialize(columnNames, columnIndex);

		// When we create a new payload, we do so with the right number of cols
	    auto createPayload = [=] ()
	        {
	            //return new TabularDatasetChunk(columnNames.size(), ROWS_PER_CHUNK);
	            return dataset->createNewChunk(ROWS_PER_CHUNK);
	        };
	    
	    PerThreadAccumulator<TabularDatasetChunk> accum(createPayload);

	    /// Finished chunks, ordered by chunk number
	    std::vector<TabularDatasetChunk> doneChunks;

	    mutex lineMutex;

		auto onLine = [&] (int chunkNum, int64_t actualLineNum, RowName rowName, Date rowTs, CellValue * vals)
	        {
	        	TabularDatasetChunk & threadAccum = accum.get();

	            if (threadAccum.chunkNumber == -1) {
	                threadAccum.chunkNumber = chunkNum;
	                //threadAccum.chunkLineNumber = chunkLineNum;
	            }

	            threadAccum.add(actualLineNum, std::move(rowName), rowTs, vals);

	            if (threadAccum.rowCount() == ROWS_PER_CHUNK) {
	                //size_t before JML_UNUSED = threadAccum.memusage();
	                threadAccum.freeze();
	                //size_t after JML_UNUSED = threadAccum.memusage();
	                TabularDatasetChunk newChunk(numberOutputColumns, ROWS_PER_CHUNK);
	                std::unique_lock<std::mutex> guard(lineMutex);
	                doneChunks.emplace_back(std::move(newChunk));
	                doneChunks.back().swap(threadAccum);
	                ExcAssertEqual(threadAccum.rowCount(), 0);

	#if 0
	                cerr << "compressed from " << before << " to " << after << " bytes ("
	                     << 100.0 * after / before << "%)" << endl;

	                int rowBits = 0;
	                for (auto & c: doneChunks.back().columns) {
	                    rowBits += c.frozen->getIndexBits();
	                    //cerr << "column had " << c.indexedVals.size()
	                    //     << " distinct values on " << c.indexes.size()
	                    //     << " total entries" << endl;
	                }
	                cerr << "rowBits = " << rowBits << endl;
	#endif                    
	            }
	        };

		loadTextData(dataset, stream, config, scope, numberOutputColumns, onLine);

		 // Accumulate the partial chunks, too, at the end
	    std::mutex doneChunksLock;

	    auto doLeftoverChunk = [&] (int threadNum)
	        {
	            TabularDatasetChunk * ent = accum.threads.at(threadNum).get();
	            ent->freeze();
	            std::unique_lock<std::mutex> guard(doneChunksLock);
	            doneChunks.emplace_back(std::move(*ent));
	        };

	    ML::run_in_parallel_blocked(0, accum.threads.size(), doLeftoverChunk);

	    cerr << "got a total of " << doneChunks.size() << " chunks" << endl;

	    size_t totalMemUsage = 0;
	    size_t totalRows = 0;
	    for (auto & c: doneChunks) {
	        totalMemUsage += c.memusage();
	        totalRows += c.rowCount();
	    }
	    cerr << "total memory usage of " << totalMemUsage / 1000000.0 << "MB "
	         << " over " << totalRows << " rows at "
	         << 1.0 * totalMemUsage / totalRows << " bytes/row and "
	         << 1.0 * totalMemUsage / totalRows / numberOutputColumns
	         << " bytes/value" << endl;

	    this->rowCount = totalRows;

	    dataset->finalize(doneChunks, totalRows);	  
		
	}

	void 
	loadTextData(std::shared_ptr<Dataset> dataset, 
						 ML::filter_istream& stream, 
						 const ImportTextConfig& config,
						 SqlCsvScope& scope,
						 const size_t numberOutputColumns,
						 const std::function<void (int, int64_t , RowName , Date , CellValue * )> & processLine)
	{	
		std::mutex lineMutex;

		// Do we have a "where true'?  In that case, we don't need to
	    // call the SQL parser
	    bool isWhereTrue = config.where->isConstantTrue();
	  
	    
	    std::atomic<uint64_t> numSkipped(0);
	    std::atomic<uint64_t> totalLinesProcessed(0);

	    ML::Timer timer;

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

	    auto onLine = [&] (const char * line,
	                       size_t length,
	                       int chunkNum,
	                       int64_t lineNum)
	        {
	            //cerr << "doing line with lineNum " << lineNum << endl;
	            //cerr << "online " << string(line, length) << endl;
	            
	            int64_t actualLineNum = lineNum + lineOffset;
	            uint64_t linesDone = totalLinesProcessed.fetch_add(1);

	            if (linesDone && linesDone % 1000000 == 0) {
	                double wall = timer.elapsed_wall();
	                cerr << "done " << linesDone << " in " << wall
	                     << "s at " << linesDone / wall * 0.000001 << "M lines/second on "
	                     << timer.elapsed_cpu() / timer.elapsed_wall() << " CPUs" << endl;
	            }

	            if (length == 0) 
	                return handleError("empty line", actualLineNum, 0, ""); // MLDB-1111 empty lines are treated as error            
	           

	            // Values that come in from the CSV file
	            // TODO: clang doesn't like a variable length array
	            // here.  Find another way to allocate it on the
	            // stack.
	            vector<CellValue> values(inputColumnNames.size());
	            //CellValue values[inputColumnNames.size()];

	            const char * lineStart = line;

	            const char * errorMsg = parseFixedWidthCsvRow(line, length, &values[0],
	                                        inputColumnNames.size(),
	                                        separator, quote, encoding,
	                                        replaceInvalidCharactersWith,
	                                        isTextLine,
	                                        hasQuoteChar);

	            if (errorMsg)
	                return handleError(errorMsg, actualLineNum, line - lineStart + 1, string(line, length));

	            //cerr << "got values " << jsonEncode(vector<CellValue>(values, values + inputColumnNames.size())) << endl;
	                
	            auto row = scope.bindRow(&values[0], ts, actualLineNum, 0 /* todo: chunk ofs */);

	            // If it doesn't match the where, don't add it 
	            if (!isWhereTrue) {
	                ExpressionValue storage;
	                if (!whereBound(row, storage).isTrue())
	                    return true;
	            }
	            
	            // Get the timestamp for the row
	            Date rowTs = ts;
	            ExpressionValue tsStorage;
	            rowTs = timestampBound(row, tsStorage).coerceToTimestamp().toTimestamp();
	            
	            ExpressionValue nameStorage;
	            RowName rowName(namedBound(row, nameStorage).toUtf8String());

	            //cerr << "adding row with rowName " << rowName << endl;
	            
	            //cerr << jsonEncodeStr(vector<CellValue>(values, values + numberOutputColumns)) << endl;
	            

	            if (isIdentitySelect) {
	                // If it's a select *, we don't really need to run the
	                // select clause.  We simply go for it.
	                //threadAccum.add(actualLineNum, std::move(rowName), rowTs, &values[0]);
	                processLine(chunkNum, actualLineNum, std::move(rowName), rowTs, &values[0]);
	            }
	            else {
	                // TODO: optimization for
	                // SELECT * excluding (...)

	                // TODO: clang doesn't like a variable length array
	                // here.  Find another way to allocate it on the
	                // stack.
	                // CellValue valuesOut[numberOutputColumns];
	                vector<CellValue> valuesOut(numberOutputColumns);

	                ExpressionValue selectStorage;
	                const ExpressionValue & selectOutput
	                    = selectBound(row, selectStorage);

	                if (&selectOutput == &selectStorage) {
	                    // We can destructively work with it

	                    auto selectRow = selectStorage.stealRow();
	                    ExcAssertEqual(selectRow.size(), numberOutputColumns);
	                    for (unsigned i = 0;  i < selectRow.size();  ++i) {	                        
	                        valuesOut[i] = std::move(std::get<1>(selectRow[i]).stealAtom());
	                    }
	                    
	                }
	                else {
	                    // Need to copy things
	                    const auto & selectRow = selectOutput.getRow();
	                    ExcAssertEqual(selectRow.size(), numberOutputColumns);
	                    for (unsigned i = 0;  i < selectRow.size();  ++i)
	                        valuesOut[i] = std::get<1>(selectRow[i]).getAtom();
	                }
	                
	                //threadAccum.add(actualLineNum, std::move(rowName), rowTs, &valuesOut[0]);
	                processLine(chunkNum, actualLineNum, std::move(rowName), rowTs, &valuesOut[0]);
	            }
	            //cerr << "row = " << jsonEncodeStr(selectRow) << endl;

	            //cerr << "row has " << selectRow.size() << " values" << endl;

	            //selectOutput.forEachColumnDestructive();

	            // Finished with this chunk.  Clear to keep blocks reasonably small	            

	            return true;

	            //threadAccum.emplace_back(std::move(lineEntry));
	        };

	    forEachLineBlock(stream, onLine, config.limit);

	    cerr << timer.elapsed() << endl;
	    timer.restart();	   

	    numLineErrors = numSkipped;
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

	auto onProgress2 = [&] (const Json::Value & progress)
	{
	    Json::Value value;
	    value["dataset"] = progress;
	    return onProgress(value);
	};

	std::shared_ptr<Dataset> dataset = createDataset(server, runProcConf.ouputDataset, onProgress2, true /*overwrite*/);

	ImportTextProcedureWorkInstance instance;

    instance.loadText(config, dataset, server);

    Json::Value status;
    status["numLineErrors"] = instance.numLineErrors;

    dataset->commit();

    return Any(status);    
    
}

namespace {

RegisterProcedureType<ImportTextProcedure, ImportTextConfig>
regEM(builtinPackage(), "import.text",
          "NJK FILL ME",
          "procedures/importtextprocedure.md.html");

} // file scope

} //MLDB
} //Datacratic
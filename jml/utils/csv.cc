// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* csv.cc
   Jeremy Barnes, 5 April 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Code to parse a CSV file.
*/

#include "csv.h"
#include "mldb/base/parse_context.h"
#include "mldb/arch/format.h"
#include "mldb/jml/utils/vector_utils.h"

using namespace std;


namespace MLDB {

namespace {
static const string literalDoubleQuote("\"\"");
} // file scope

std::string expect_csv_field(ParseContext & context, bool & another,
                             char separator)
{
    bool quoted = false;
    std::string result;
    result.reserve(128);
    another = false;
    
    while (context) {
        //cerr << "at character '" << *context << "' quoted = " << quoted
        //     << endl;

        if (false && context.get_line() == 9723)
            cerr << "*context = " << *context << " quoted = " << quoted
                 << " result = " << result << endl;
        
        if (quoted) {

            if (context.match_literal(literalDoubleQuote)) {
                result += '\"';
                continue;
            }
            if (context.match_literal('\"')) {
                if (context && *context == separator)
                    another = true;
                if (!context || context.match_literal(separator)
                    || *context == '\n' || *context == '\r')
                    return result;
                //cerr << "(bool)context = " << (bool)context << endl;
                //cerr << "*context = " << *context << endl;
                //cerr << "result = " << result << endl;

                for (unsigned i = 0; i < 20;  ++i)
                    cerr << *context++;

                context.exception_fmt("invalid end of line: %d %c",
                                      (int)*context, *context);
            }
        }
        else {
            if (context.match_literal('\"')) {
                if (result == "") {
                    quoted = true;
                    continue;
                }
                // quotes only count at the beginning
                result += '\"';
                continue;
            }
            else if (context.match_literal(separator)) {
                another = true;
                return result;
            }
            else if (*context == '\n' || *context == '\r')
                return result;

        }
        result += *context++;
    }

    if (quoted)
        throw FileFinishInsideQuote("file finished inside quote");

    return result;
}

std::vector<std::string>
expect_csv_row(ParseContext & context, int length, char separator)
{
    //    cerr << "*** parsing" << endl;

    context.skip_whitespace();

    vector<string> result;
    if (length != -1)
        result.reserve(length);
    else result.reserve(16);

    bool another = false;
    while (another || (context && !context.match_eol() && *context != '\r')) {
        result.emplace_back(expect_csv_field(context, another, separator));
        //cerr << "read " << result.back() << " another = " << another << endl;
    }

    if (length != -1 && result.size() != length) {
        cerr << "result = " << result << endl;
        context.exception(format("Wrong CSV length: expected %d, got %zd",
                                 length, result.size()));
    }
    
    //cerr << "returning result" << endl;

    return result;
}

std::string csv_escape(const std::string & s)
{
    int quote_pos = s.find('"');
    int nl_pos = s.find('\n');
    int comma_pos = s.find(',');

    if (quote_pos == string::npos && nl_pos == string::npos
        && comma_pos == string::npos)
        return s;

    string result = "\"";
    result.reserve(s.size() + 4);

    for (unsigned i = 0;  i < s.size();  ++i) {
        if (s[i] == '\"') result += "\"\"";
        else result += s[i];
    }

    result += "\"";

    return result;
}

} // namespace MLDB

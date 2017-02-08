/* csv.h                                                           -*- C++ -*-
   Jeremy Barnes, 5 April 2010
   This file is part of MLDB. Copyright 2010 mldb.ai inc. All rights reserved.

   Comma Separated Value parsing code.
*/

#pragma once

#include <string>
#include <vector>

namespace MLDB {

struct ParseContext;


/**
 * Exception used to signal that a line ended inside a quote.
 * It can be used to append the next available line in a file, if
 * one is available, since there is possibly an end of line
 * character in the actual string we're trying to parse
 **/
struct FileFinishInsideQuote : public std::exception {
public:
    FileFinishInsideQuote(const char *msg) : msg(msg) {};
    const char *what() const throw() { return msg; };
private:
    const char* msg;
};



/** Expect a CSV field from the given parse context.  Another will be set
    to true if there is still another field in the CSV row. */
std::string expect_csv_field(ParseContext & context, bool & another,
                             char separator = ',');


/** Expect a row of CSV from the given parse context.  If length is not -1,
    then the extact number of fields required is given in that parameter. */
std::vector<std::string>
expect_csv_row(ParseContext & context, int length = -1, char separator = ',');

/** Convert the string to a CSV representation, escaping everything that
    needs to be escaped. */
std::string csv_escape(const std::string & s);

} // namespace MLDB


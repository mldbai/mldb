// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/**
 * csv_writer.cc
 * Mich, 2015-11-11
 * Copyright (c) 2015 mldb.ai inc. All rights reserved.
 **/

#include "csv_writer.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

using namespace std;

CsvWriter::
CsvWriter(ostream & out)
    : out(out), delimiterChar(","), quoteChar("\""), lineStart(true)
{
}

CsvWriter::
CsvWriter(std::ostream & out, char delimiterChar, char quoteChar)
    : out(out), delimiterChar(""), quoteChar(""), lineStart(true)
{
    this->delimiterChar += delimiterChar;
    this->quoteChar += quoteChar;
    ExcAssert(this->delimiterChar.size() == 1);
    ExcAssert(this->quoteChar.size() == 1);
}

CsvWriter&
CsvWriter::
operator<< (int value)
{
    return *this << std::to_string(value);
}

CsvWriter&
CsvWriter::
operator<< (const char* str)
{
    return *this << string(str);
}

CsvWriter&
CsvWriter::
operator<< (const std::string & val)
{
    {
        // Delimiter
        if (lineStart) {
            lineStart = false;
        }
        else {
            out << delimiterChar;
        }
    }

    {
        // escaping
        auto newVal = boost::replace_all_copy(val, quoteChar,
                                                quoteChar + quoteChar);
        if (val.find(delimiterChar) != std::string::npos
            || newVal.size() != val.size())
        {
            out << quoteChar << newVal << quoteChar;
        }
        else {
            out << val;
        }
    }

    return *this;
}

CsvWriter&
CsvWriter::
operator<< (const Utf8String & val)
{
    return operator << (val.rawString());
}

void
CsvWriter::
endl()
{
    out << '\n';
    lineStart = true;
}

} // namespace MLDB

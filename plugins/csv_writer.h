// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/**
 * csv_writer.h
 * Mich, 2015-11-11
 * Copyright (c) 2015 Datacratic Inc. All rights reserved.
 **/

#pragma once

#include <iostream>
#include <string>
#include <ostream>
#include <boost/algorithm/string/replace.hpp>

#include "mldb/vfs/filter_streams.h"
#include "mldb/types/id.h"
#include "mldb/types/string.h"

namespace Datacratic {

struct CsvWriter {
    std::ostream & out;
    std::string delimiterChar;
    std::string quoteChar;
    bool lineStart;

    CsvWriter() = delete;
    CsvWriter(std::ostream & out);
    CsvWriter(std::ostream & out, char delimiterChar, char quoteChar);

    CsvWriter& operator<< (int value);
    CsvWriter& operator<< (const Id & value);
    CsvWriter& operator<< (const char* str);
    CsvWriter& operator<< (const std::string & value);
    CsvWriter& operator<< (const Utf8String & value);

    void endl();
};

} // Datacratic

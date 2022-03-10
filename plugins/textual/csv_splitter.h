/** csv_splitter.h                                                -*- C++ -*-
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to process each line in a file in parallel.
*/

#pragma once

#include "mldb/utils/for_each_line.h"

namespace MLDB {

enum CsvLineEncoding {
    ASCII,
    UTF8,
    LATIN1
};

CsvLineEncoding parseCsvLineEncoding(const std::string & encodingStr_);

struct CSVSplitterState {
};


struct CSVSplitter: public BlockSplitterT<CSVSplitterState> {
    CSVSplitter(char quoteChar,
                bool allowMultiLine,
                CsvLineEncoding encoding)
        : quoteChar(quoteChar), allowMultiLine(allowMultiLine), encoding(encoding)
    {
    }

    virtual std::pair<const char *, CSVSplitterState>
    nextBlockT(const char * block1, size_t n1, const char * block2, size_t n2,
               bool noMoreData,
               const CSVSplitterState & state) const override;
    virtual std::span<const char> fixupBlock(std::span<const char> block) const override;

    char quoteChar;
    bool allowMultiLine;
    CsvLineEncoding encoding;
};


} // namespace MLDB

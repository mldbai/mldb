/* tokenize.h                                        -*- C++ -*-
   Mathieu Marquis Bolduc, October 5th 2015
   Copyright (c) 2015 Datacratic.  All rights reserved.

   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Generic delimiter-token parsing.
*/

#pragma once

#include <string>
#include <unordered_map>
#include <functional>
#include "types/string.h"
#include "jml/stats/distribution.h"

namespace ML {
    struct Parse_Context;
}

namespace Datacratic {

    void
    tokenize_exec(std::function<bool (Utf8String&)> exec,
              ML::Parse_Context& context,
              const Utf8String& splitchars,
              const Utf8String& quotechar,
              int min_token_length);

    char32_t expectUtf8Char(ML::Parse_Context & context);

    bool tokenize(std::unordered_map<Utf8String, int>& bagOfWords,
                  ML::Parse_Context& pcontext,
                  const Utf8String& splitchars,
                  const Utf8String& quotechar,
                  int offset, int limit,
                  int min_token_length,
                  ML::distribution<float, std::vector<float> > & ngram_range);

    Utf8String token_extract(ML::Parse_Context& context,
                             const Utf8String& splitchars,
                             const Utf8String& quotechar,
                             int offset, int limit, int nth,
                             int min_token_length);

}


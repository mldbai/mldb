/* tokenize.cc
   Mathieu Marquis Bolduc, October 5th 2015
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Generic delimiter-token parsing.
*/

#include "tokenize.h"
#include "base/parse_context.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/pair_description.h"
#include <queue>

using namespace std;

namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(TokenizeOptions);

TokenizeOptionsDescription::
TokenizeOptionsDescription()
{
    addAuto("splitChars", &TokenizeOptions::splitchar,
            "Characters to split on in the tokenization.");
    addAuto("quoteChar", &TokenizeOptions::quotechar,
            "a single character to delimit tokens which may contain the "
            "`splitchars`, so by default "
            "`tokenize('a,\"b,c\"', {quoteChar:'\"'})` will return the row "
            "`{'a':1,'b,c':1}`.  By default no quoting character is used.");
    addAuto("offset", &TokenizeOptions::offset,
            "Skip the first `offset` tokens of the output (default 0).");
    addAuto("limit", &TokenizeOptions::limit,
            "Only generate `limit` tokens in the output (default is -1, "
            "which means generate all.");
    addAuto("value", &TokenizeOptions::value,
            "`value` (if not set to `null`) will be used instead of "
            "token counts for the values of the columns in the output row.");
    addAuto("minTokenLength", &TokenizeOptions::minTokenLength,
            "Minimum number of characters in a token for it to be output or "
            "included as part of an ngram");
    addAuto("ngramRange", &TokenizeOptions::ngramRange,
            "Specifies the complexity of n-grams to return, with the "
            "first element corresponding to minimum length and the "
            "second to maximum length.  "
            "`[1, 1]` will return only unigrams, while `[2, 3]` will "
            "return bigrams and trigrams, where tokens are joined by "
            "underscores. For example, "
            "`tokenize('Good day world', {splitChars:' ', ngramRange:[2,3]})`"
            "will return the row `{'Good_day': 1, 'Good_day_world': 1, 'day_world': 1}`");

    onUnknownField = [] (TokenizeOptions * options,
                         JsonParsingContext & context)
    {
        if(context.fieldName() == "min_token_length") {
            options->minTokenLength = context.expectInt();
            cerr << "The 'min_token_length' argument has been renamed to 'minTokenLength'" << endl;
        }
        else if(context.fieldName() == "ngram_range") {
            int i=0;
            context.forEachElement([&] () 
                {
                    if(i++==0)
                        options->ngramRange.first = context.expectInt();
                    else
                        options->ngramRange.second = context.expectInt();
                });
            cerr << "The 'ngram_range' argument has been renamed to 'ngramRange'" << endl;
        }
        else if(context.fieldName() == "splitchars") {
            options->splitchar = context.expectStringUtf8();
            cerr << "The 'splitchars' argument has been renamed to 'splitChars'" << endl;
        }
        else if(context.fieldName() == "quotechar") {
            options->quotechar = context.expectStringUtf8();
            cerr << "The 'quotechar' argument has been renamed to 'quoteChar'" << endl;
        }
        else {
            context.exception("Unknown field '" + context.fieldName()
                    + " parsing tokenize configuration");
          }
        return false;
    };
}

struct NGramer {
    NGramer(int min_range, int max_range)
        : min_range(min_range), max_range(max_range),
          count(0), buffer_pos(0)
    {
        if(max_range<min_range || min_range<1 || max_range<1)
            throw MLDB::Exception("ngramRange values must be bigger than 0 "
                    "and the second value needs to be equal or bigger than the first");
    
        buffer.resize(max_range);
    }

    void
    add_token(const Utf8String & token, std::function<bool (Utf8String&)> onGram) {
        count ++;
        buffer[buffer_pos] = token;
        buffer_pos = (buffer_pos + 1) % max_range;

        // if we got at least the min_range, we can start
        // returning that
        if(count >= min_range) {
            // for each type of gram we want, this means do the following for 
            // 1-grams, 2-grams, etc,
            int curr_max = count < max_range ? count : max_range;

            for(int curr_gram = min_range; curr_gram <= curr_max; curr_gram++) {
                Utf8String accumulator;
                for(int sub_gram=curr_gram; sub_gram > 0; sub_gram--) {

                    int idx = buffer_pos - sub_gram;
                    if(idx < 0)
                        idx = max_range + idx;

                    if(sub_gram!=curr_gram) accumulator += "_";
                    accumulator += buffer[idx];
                }

                if(!onGram(accumulator))
                    return;
            }
        }
    }

    int min_range;
    int max_range;

    size_t count;
    size_t buffer_pos;
    vector<Utf8String> buffer;
};



char32_t expectUtf8Char(ParseContext & context)
{
    if (!context)
        context.exception("Expected UTF-8 character but got EOF instead");

    unsigned char c = *context;

    if (c < 0x80)
        return *context++;

    int n = 1; // number of bytes
    if (c >= 0xc0) ++n;
    if (c >= 0xe0) ++n;
    if (c >= 0xf0) ++n;

    // Get a maximum of 64 chars
    unsigned char buf[n];
    for (unsigned i = 0;  i < n;  ++i)
        buf[i] = *context++;

    //cerr << "utf-8 read " << n << " chars" << endl;

    unsigned char * p = buf;
    unsigned char * e = buf + n;

    char32_t res = utf8::next(p, e);

    if (p != e) {
        context.exception("consumed too many UTF-8 chars");
    }

    return res;
}

static bool matchUtf8Literal(ParseContext & context, const Utf8String& literal)
{
    ParseContext::Revert_Token token(context);
    auto it = literal.begin();
    while (!context.eof() && it != literal.end())
    {
        char32_t utfChar = expectUtf8Char(context);

        if (utfChar != *it)
                break;

        ++it;
        if (it == literal.end()) {
            token.ignore();
            return true;
        }
    }
    return false;
}

static bool matchOneOfUtf8(ParseContext & context, const Utf8String& literals)
{
    ParseContext::Revert_Token token(context);

    if (context.eof())
       return false;

    char32_t utfChar = expectUtf8Char(context);

    for (auto x : literals) {
        if (x == utfChar) {
           token.ignore();
           return true;
        }
    }

    return false;
}


void
tokenize_exec(std::function<bool (Utf8String&)> exec,
              ParseContext& context,
              const Utf8String& splitchars,
              const Utf8String& quotechar,
              int minTokenLength)
{
    auto expect_token = [=] (ParseContext& context, bool& another) -> Utf8String
        {
            Utf8String result;

            bool quoted = false;
            another = false;

            while (!context.eof()) {

                if (quoted) {

                    if (matchUtf8Literal(context, quotechar)) {
                        if (context && matchOneOfUtf8(context, splitchars)) {
                            another = true;
                            return result;
                        }

                        if (!context)
                            return result;

                        quoted = false;
                    }
                }
                else {
                    if (matchUtf8Literal(context, quotechar)) {
                        if (result == "") {
                            quoted = true;
                            continue;
                        }
                        // quotes only count at the beginning
                        result += quotechar;
                        continue;
                    }
                    else if (matchOneOfUtf8(context, splitchars)) {
                        another = true;
                        return result;
                    }
                }

                result += expectUtf8Char(context);
            }

            if (quoted)
                throw MLDB::Exception("string finished inside quote");

            return result;
        };

    bool another = false;
    while (another || (context && !context.eof())) {

        Utf8String word = expect_token(context, another);

        if(word.length() < minTokenLength)
            continue;

        if (!exec(word))
            break;
    }

    return;
}

bool tokenize(std::unordered_map<Utf8String, int>& bagOfWords,
              ParseContext& pcontext,
              const TokenizeOptions & options)
{
    int count = 0;
 
    NGramer nGramer(options.ngramRange.first, options.ngramRange.second);

    auto onGram = [&](Utf8String& word) -> bool
    {
        auto it = bagOfWords.find(word);

        if (it == bagOfWords.end()) {
            bagOfWords[word] = 1;
        }
        else {
            it->second += 1;
        }
        return true;
    };

    auto aggregate = [&](Utf8String& word) -> bool
    {
        ++count;
        if (count <= options.offset)
            return true; //continue

        if (word != "") {
            nGramer.add_token(word, onGram);
        }

        if (count == options.limit+options.offset)
            return false; //stop here

        return true;
    };

    tokenize_exec(aggregate, pcontext,
                  options.splitchar,
                  options.quotechar,
                  options.minTokenLength);

    return !bagOfWords.empty();
}


Utf8String token_extract(ParseContext& context,
                         int nth,
                         const TokenizeOptions & options)
{
    Utf8String result;

    int count = 0;
    if (nth >= 0) {
        auto aggregate_positive = [&] (Utf8String& word) -> bool
        {
            ++count;
            if (count <= options.offset + nth)
                return true; //skip & continue

            if (word != "") {
                result = word;
                return false; //found it, stop
            }

            if (count == options.limit+options.offset+nth)
                return false; //stop here

            return true;
        };

        tokenize_exec(aggregate_positive, context,
                      options.splitchar, options.quotechar,
                      options.minTokenLength);
    }
    else {
        std::queue<Utf8String> tokens;

        auto aggregate_negative = [&](Utf8String& word) -> bool
        {
            ++count;
            if (count <= options.offset)
                return true; //continue

            tokens.push(word);

            if (tokens.size() > -nth)
                tokens.pop(); //For memory efficiency

            if (count == options.limit+options.offset)
                return false; //stop here

            return true;
        };

        tokenize_exec(aggregate_negative, context,
                      options.splitchar, options.quotechar,
                      options.minTokenLength);
        if (tokens.size() == -nth)
            result = std::move(tokens.front());
    }

    return result;

}

std::vector<Utf8String> token_split(ParseContext& context,
                                    const Utf8String& splitchars)
{
    std::vector<Utf8String> result;

    auto aggregate = [&] (Utf8String& word) -> bool
    {

        if (!word.empty())
            result.push_back(word);

        return true;
    };

    tokenize_exec(aggregate, context,
                  splitchars, "",
                  0);


    return result;
}


} // namespace MLDB

// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* tokenize.cc
   Mathieu Marquis Bolduc, October 5th 2015
   Copyright (c) 2015 Datacratic.  All rights reserved.

   Generic delimiter-token parsing.
*/

#include "tokenize.h"
#include "base/parse_context.h"
#include <queue>

using namespace std;


namespace Datacratic {


struct NGramer {

    NGramer(int min_range, int max_range) 
        : min_range(min_range), max_range(max_range),
          count(0), buffer_pos(0)
    {
        if(max_range<min_range || min_range<1 || max_range<1)
            throw ML::Exception("ngram_range values must be bigger than 0 "
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



char32_t expectUtf8Char(ML::Parse_Context & context)
{
    if (!context)
        context.exception("Expected UTF-8 character");

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

static bool matchUtf8Literal(ML::Parse_Context & context, const Utf8String& literal)
{
    ML::Parse_Context::Revert_Token token(context);
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

static bool matchOneOfUtf8(ML::Parse_Context & context, const Utf8String& literals)
{
    ML::Parse_Context::Revert_Token token(context);

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
              ML::Parse_Context& context,
              const Utf8String& splitchars,
              const Utf8String& quotechar,
              int min_token_length)
{
    auto expect_token = [=] (ML::Parse_Context& context, bool& another) -> Utf8String
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
                throw ML::Exception("string finished inside quote");

            return result;
        };

    bool another = false;
    while (another || (context && !context.eof())) {

        Utf8String word = std::move(expect_token(context, another));

        if(word.length() < min_token_length)
            continue;

        if (!exec(word))
            break;
    }

    return;
}

bool
tokenize(std::unordered_map<Utf8String, int>& bagOfWords,
         ML::Parse_Context& context,
         const Utf8String& splitchars,
         const Utf8String& quotechar,
         int offset, int limit,
         int min_token_length,
         ML::distribution<float, std::vector<float> > & ngram_range)
{
    int count = 0;
 
    NGramer nGramer(ngram_range[0], ngram_range[1]);

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
        if (count <= offset)
            return true; //continue

        if (word != "") {
            nGramer.add_token(word, onGram);
        }

        if (count == limit+offset)
            return false; //stop here

        return true;
    };

    tokenize_exec(aggregate, context, splitchars, quotechar, min_token_length);

    return !bagOfWords.empty();
}

Utf8String token_extract(ML::Parse_Context& context,
                         const Utf8String& splitchars,
                         const Utf8String& quotechar,
                         int offset, int limit, int nth,
                         int min_token_length)
{
    Utf8String result;

    int count = 0;
    if (nth >= 0) {
        auto aggregate_positive = [&] (Utf8String& word) -> bool
        {
            ++count;
            if (count <= offset + nth)
                return true; //skip & continue

            if (word != "") {
                result = word;
                return false; //found it, stop
            }

            if (count == limit+offset+nth)
                return false; //stop here

            return true;
        };

        tokenize_exec(aggregate_positive, context, splitchars, quotechar, min_token_length);
    }
    else {
        std::queue<Utf8String> tokens;

        auto aggregate_negative = [&](Utf8String& word) -> bool
        {
            ++count;
            if (count <= offset)
                return true; //continue

            tokens.push(word);

            if (tokens.size() > -nth)
                   tokens.pop(); //For memory efficiency

            if (count == limit+offset)
                return false; //stop here

            return true;
        };

        tokenize_exec(aggregate_negative, context, splitchars, quotechar, min_token_length);
        if (tokens.size() == -nth)
            result = std::move(tokens.front());
    }

    return result;

}


} //Datacratic

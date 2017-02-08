// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* parse_context.cc                                                 -*- C++ -*-
   Jeremy Barnes, 13 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.

   Routines for parsing of text files.
*/

#include "parse_context.h"
#include <cstdarg>
#include "mldb/arch/exception.h"
#include "mldb/arch/format.h"
#include "fast_int_parsing.h"
#include "fast_float_parsing.h"
#include <boost/utility.hpp>
#include <cassert>
#include <fstream>


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* PARSE_CONTEXT                                                             */
/*****************************************************************************/

const std::string ParseContext::CONSOLE("-");

ParseContext::
ParseContext()
    : stream_(0), chunk_size_(0), first_token_(0), last_token_(0),
      cur_(0), ebuf_(0),
      line_(0), col_(0), ofs_(0)
{
}

ParseContext::
ParseContext(const std::string & filename, const char * start,
              const char * end, unsigned line, unsigned col)
    : stream_(0), chunk_size_(0), first_token_(0), last_token_(0),
      filename_(filename), cur_(start), ebuf_(end),
      line_(line), col_(col), ofs_(0)
{
    current_ = buffers_.insert(buffers_.end(),
                               Buffer(0, start, end - start, false));
}

ParseContext::
ParseContext(const std::string & filename, const char * start,
              size_t length, unsigned line, unsigned col)
    : stream_(0), chunk_size_(0), first_token_(0), last_token_(0),
      filename_(filename), cur_(start), ebuf_(start + length),
      line_(line), col_(col), ofs_(0)
{
    current_ = buffers_.insert(buffers_.end(),
                               Buffer(0, start, length, false));

    //cerr << "current buffer has " << current_->size << " chars" << endl;
}

ParseContext::
ParseContext(const std::string & filename)
    : stream_(0), chunk_size_(0), first_token_(0), last_token_(0),
      cur_(0), ebuf_(0),
      line_(0), col_(0), ofs_(0)
{
    init(filename);
}

ParseContext::
ParseContext(const std::string & filename, std::istream & stream,
              unsigned line, unsigned col, size_t chunk_size)
    : stream_(&stream), chunk_size_(chunk_size), first_token_(0), last_token_(0),
      filename_(filename), cur_(0), ebuf_(0),
      line_(1), col_(1), ofs_(0)
{
    current_ = read_new_buffer();

    if (current_ != buffers_.end()) {
        cur_ = current_->pos;
        ebuf_ = cur_ + current_->size;
    }
}

ParseContext::
~ParseContext()
{
}

void
ParseContext::
init(const std::string & filename)
{
    stream_ = 0;
    chunk_size_ = DEFAULT_CHUNK_SIZE;
    first_token_ = 0;
    last_token_ = 0;
    filename_ = filename;
    line_ = 1;
    col_ = 1;
    ofs_ = 0;

    // TODO: plug how we open a file
    ownedStream_.reset(new std::ifstream(filename.c_str()));
    stream_ = ownedStream_.get();

    current_ = read_new_buffer();

    if (current_ != buffers_.end()) {
        cur_ = current_->pos;
        ebuf_ = cur_ + current_->size;
    }
}

namespace {

struct MatchAnyChar {
    MatchAnyChar(const char * delimiters, int nd)
    {
        assert(nd > 0 && nd <= 4);
        for (unsigned i = 0;  i < nd;  ++i)
            chars[i] = delimiters[i];
        for (unsigned i = nd;  i < 4;  ++i)
            chars[i] = delimiters[0];
    }
    
    char chars[4];

    bool operator () (char c) const
    {
        return (c == chars[0] || c == chars[1]
                || c == chars[2] || c == chars[3]);
    }
};

struct MatchAnyCharLots {
    MatchAnyCharLots(const char * delimiters, int nd)
    {
        const unsigned char * del2
            = reinterpret_cast<const unsigned char *>(delimiters);

        for (unsigned i = 0;  i < 8;  ++i)
            bits[i] = 0;
        for (unsigned i = 0;  i < nd;  ++i) {
            int x = del2[i];
            bits[x >> 5] |= (1 << (x & 31));
        }
    }
    
    uint32_t bits[8];

    bool operator () (unsigned char c) const
    {
        return bits[c >> 5] & (1 << (c & 31));
    }
};

} // file scope

bool
ParseContext::
match_text(std::string & text, const char * delimiters)
{
    int nd = strlen(delimiters);

    if (nd == 0)
        throw MLDB::Exception("ParseContext::match_text(): no characters");

    if (nd <= 4) return match_text(text, MatchAnyChar(delimiters, nd));
    else return match_text(text, MatchAnyCharLots(delimiters, nd));
}

bool 
ParseContext::
match_test_icase(const char* word)
{
    Revert_Token token(*this);

    const char * p = word;

    while (!eof() && *p) {       

        if (tolower(*cur_) != tolower(*p))
            return false;

        operator ++ ();
        ++p;
    }

    if (*p == 0) {
        token.ignore();
        return true;
    }

    return false;
}

std::string
ParseContext::
expect_text(char delimiter, bool allow_empty, const char * error)
{
    string result;
    if (!match_text(result, delimiter)
        || (result.empty() && !allow_empty)) exception(error);
    return result;
}
    
std::string
ParseContext::
expect_text(const char * delimiters, bool allow_empty, const char * error)
{
    string result;
    if (!match_text(result, delimiters)
        || (result.empty() && !allow_empty)) exception(error);
    return result;
}

bool
ParseContext::
match_int(int & val_, int min, int max)
{
    Revert_Token tok(*this);
    long val = 0;
    if (!MLDB::match_int(val, *this)) return false;
    if (val < min || val > max) return false;
    val_ = val;
    tok.ignore();
    return true;
}
    
int
ParseContext::
expect_int(int min, int max, const char * error)
{
    int result;
    if (!match_int(result, min, max)) exception(error);
    return result;
}

bool
ParseContext::
match_hex4(int & val_, int min, int max)
{
    Revert_Token tok(*this);
    long val = 0;
    if (!MLDB::match_hex4(val, *this)) return false;
    if (val < min || val > max) return false;
    val_ = val;
    tok.ignore();
    return true;
}

int
ParseContext::
expect_hex4(int min, int max, const char * error)
{
    int result;
    if (!match_hex4(result, min, max)) exception(error);
    return result;
}

bool
ParseContext::
match_unsigned(unsigned & val_, unsigned min, unsigned max)
{
    Revert_Token tok(*this);
    unsigned long val;
    if (!MLDB::match_unsigned(val, *this)) return false;
    if (val < min || val > max) return false;
    val_ = val;
    tok.ignore();
    return true;
}

unsigned
ParseContext::
expect_unsigned(unsigned min, unsigned max, const char * error)
{
    unsigned result = 1;
    if (!match_unsigned(result, min, max)) exception(error);
    return result;
}

bool
ParseContext::
match_long(long & val_, long min, long max)
{
    Revert_Token tok(*this);
    long val = 0;
    if (!MLDB::match_long(val, *this)) return false;
    if (val < min || val > max) return false;
    val_ = val;
    tok.ignore();
    return true;
}
    
long
ParseContext::
expect_long(long min, long max, const char * error)
{
    long result;
    if (!match_long(result, min, max)) exception(error);
    return result;
}

bool
ParseContext::
match_unsigned_long(unsigned long & val_, unsigned long min,
                         unsigned long max)
{
    Revert_Token tok(*this);
    unsigned long val = 0;
    if (!MLDB::match_unsigned_long(val, *this)) return false;
    if (val < min || val > max) return false;
    val_ = val;
    tok.ignore();
    return true;
}

unsigned long
ParseContext::
expect_unsigned_long(unsigned long min, unsigned long max,
                          const char * error)
{
    unsigned long result;
    if (!match_unsigned_long(result, min, max)) exception(error);
    return result;
}

bool
ParseContext::
match_long_long(long long & val_, long long min, long long max)
{
    Revert_Token tok(*this);
    long long val = 0;
    if (!MLDB::match_long_long(val, *this)) return false;
    if (val < min || val > max) return false;
    val_ = val;
    tok.ignore();
    return true;
}
    
long long
ParseContext::
expect_long_long(long long min, long long max, const char * error)
{
    long long result;
    if (!match_long_long(result, min, max)) exception(error);
    return result;
}

bool
ParseContext::
match_unsigned_long_long(unsigned long long & val_, unsigned long long min,
                         unsigned long long max)
{
    Revert_Token tok(*this);
    unsigned long long val = 0;
    if (!MLDB::match_unsigned_long_long(val, *this)) return false;
    if (val < min || val > max) return false;
    val_ = val;
    tok.ignore();
    return true;
}

unsigned long long
ParseContext::
expect_unsigned_long_long(unsigned long long min, unsigned long long max,
                          const char * error)
{
    unsigned long long result;
    if (!match_unsigned_long_long(result, min, max)) exception(error);
    return result;
}

bool
ParseContext::
match_float(float & val, float min, float max, bool lenient)
{
    Revert_Token t(*this);
    if (!MLDB::match_float(val, *this, lenient)) return false;
    if (val < min || val > max) return false;
    t.ignore();
    return true;
}

float
ParseContext::
expect_float(float min, float max, const char * error, bool lenient)
{
    float val;
    if (!match_float(val, min, max, lenient))
        exception(error);
    return val;
}

bool
ParseContext::
match_double(double & val, double min, double max, bool lenient)
{
    Revert_Token t(*this);
    if (!MLDB::match_float(val, *this, lenient)) return false;
    if (val < min || val > max) return false;
    t.ignore();
    return true;
}

double
ParseContext::
expect_double(double min, double max, const char * error, bool lenient)
{
    double val;
    if (!match_double(val, min, max, lenient))
        exception(error);
    return val;
}

std::string
ParseContext::
where() const
{
    if (!eof()) {
        ssize_t toGrab = std::min<ssize_t>(32, cur_ - current_->pos);
        string leading;
        if (toGrab > 0)
            leading = string(cur_ - toGrab, cur_);

        toGrab = std::min<ssize_t>(32, ebuf_ - cur_ - 1);
        string trailing;
        if (toGrab > 0)
            trailing = string(cur_ + 1, cur_ + toGrab);
        
        return filename_ + ":" + to_string(line_) + ":"
            + to_string(col_) + " ('"
            + leading + ">>>" + *cur_ + "<<<" + trailing + "')";
    }
    return filename_ + ":" + to_string(line_) + ":"
        + to_string(col_);
}

void
ParseContext::
exception(const std::string & message) const
{
    throw Exception(where() + ": " + message, filename_, line_, col_);
}

void
ParseContext::
exception(const char * message) const
{
    throw Exception(where() + ": " + string(message), filename_, line_, col_);
}

void
ParseContext::
exception_fmt(const char * fmt, ...) const
{
    va_list ap;
    va_start(ap, fmt);
    string str = vformat(fmt, ap);
    va_end(ap);
    exception(str);
}

bool
ParseContext::
match_literal_str(const char * start, size_t len)
{
    Revert_Token token(*this);

    //cerr << "got revert token" << endl;
    //cerr << "len = " << len << " eof() = " << eof() << " char = " << *cur_
    //     << " match = " << *cur_ << endl;

    while (len && !eof() && *start++ == *cur_) {
        //cerr << "len = " << len << endl;
        operator ++ ();  --len;
    }

    if (len == 0) token.ignore();
    return (len == 0);
}

void
ParseContext::
next_buffer()
{
    //cerr << "next_buffer: ofs_ = " << ofs_ << " line_ = " << line_
    //     << " col_ = " << col_ << endl;
    //cerr << buffers_.size() << " buffers, eof = "
    //     << (current_ == buffers_.end()) << endl;

    if (current_ == buffers_.end()) {
        return; // eof
        throw MLDB::Exception("ParseContext: asked for new buffer when already "
                            " at end");
    }
    else {
        ++current_;
        
        if (current_ == buffers_.end())
            current_ = read_new_buffer();
        
        if (current_ != buffers_.end()) {
            cur_ = current_->pos;
            ebuf_ = cur_ + current_->size;
            //cerr << "got buffer with " << current_->size << " chars"
            //     << endl;
        }

        /* Free any buffers if we can. */
        free_buffers();
    }

    //cerr << "after next_buffer: ofs_ = " << ofs_ << " line_ = " << line_
    //     << " col_ = " << col_ << endl;
    //cerr << buffers_.size() << " buffers, eof = "
    //     << (current_ == buffers_.end()) << endl;
    //int i = 0;
    //for (std::list<Buffer>::const_iterator it = buffers_.begin();
    //     it != buffers_.end();  ++it, ++i) {
    //    cerr << "buffer " << i << " of " << buffers_.size() << ": ofs "
    //         << it->ofs << " size " << it->size << endl;
    //}
}

void
ParseContext::
goto_ofs(uint64_t ofs, size_t line, size_t col,
         bool in_destructor)
{
    //cerr << "goto_ofs: ofs = " << ofs << " line = " << line << " col = "
    //     << col << endl;
    //cerr << "current: ofs_ = " << ofs_ << " line_ = " << line_
    //     << " col_ = " << col_ << endl;
    //cerr << buffers_.size() << " buffers" << endl;

    // If we're already there, then it's a no-op
    if (ofs_ == ofs) {
        return;
    }

    ofs_ = ofs;
    line_ = line;
    col_ = col;

    int i = 0, s = buffers_.size();
    /* TODO: be more efficient... */
    for (std::list<Buffer>::iterator it = buffers_.begin();
         it != buffers_.end();  ++it, ++i) {
        //cerr << "buffer " << i << " of " << buffers_.size() << ": ofs "
        //     << it->ofs << " size " << it->size << endl;
        if (ofs < it->ofs + it->size
            || (ofs == 0 && it->ofs + it->size == 0)
            || (i == s - 1 && ofs == it->ofs + it->size)) {
            /* In here. */
            cur_ = it->pos + (ofs - it->ofs);
            ebuf_ = it->pos + it->size;
            current_ = it;
            return;
        }
    }

    // Don't throw in a destructor even if we are completely messed up.
    // Instead we ignore the token.  That way, we don't terminate the
    // program.
    if (in_destructor) {
        return;
    }

    exception_fmt("ParseContext::goto_ofs(): couldn't find position %zd (l%zdc%zd)", ofs, line, col);
}

std::string
ParseContext::
text_between(uint64_t ofs1, uint64_t ofs2) const
{
    std::string result;

    for (auto it = buffers_.begin();
         it != buffers_.end() && ofs1 < ofs2;  ++it) {

        if (ofs1 < it->ofs + it->size
            || (ofs1 == 0 && it->ofs + it->size == 0)) {
            /* In here. */
            const char * cur = it->pos + (ofs1 - it->ofs);
            const char * ebuf = it->pos + it->size;

            int64_t bufToDo = std::min<int64_t>(ofs2 - ofs1, ebuf - cur);
            result.append(cur, cur + bufToDo);
            ofs1 += bufToDo;
        }
    }

    return result;
}

void
ParseContext::
free_buffers()
{
    /* Free buffers so long as a) it's not the current buffer, and b)
       the first token isn't inside it. */
    for (std::list<Buffer>::iterator it = buffers_.begin();
         it != current_;  /* no inc */) {
        if (first_token_ && (first_token_->ofs < it->ofs + it->size))
            break;  // first token is in this buffer
        std::list<Buffer>::iterator to_erase = it;
        ++it;
        if (to_erase->del) {
            delete[] (const_cast<char *>(to_erase->pos));
        }
        buffers_.erase(to_erase);
    }
}

std::list<ParseContext::Buffer>::iterator
ParseContext::
read_new_buffer()
{
    if (!stream_) return buffers_.end();

    if (stream_->eof()) return buffers_.end();

    //cerr << "stream is OK" << endl;

    if (stream_->bad() || stream_->fail())
        exception("stream is bad/has failed 1");
    
    static const size_t MAX_STACK_CHUNK_SIZE = 65536;

    //char tmpbuf_stack[chunk_size_];
    char tmpbuf_stack[std::min(chunk_size_, MAX_STACK_CHUNK_SIZE)];
    char * tmpbuf = tmpbuf_stack;
    std::shared_ptr<char> tmpbuf_dynamic;

    if (chunk_size_ > MAX_STACK_CHUNK_SIZE) {
        tmpbuf_dynamic.reset(new char[chunk_size_], [] (char *p) { delete[] p; });
        tmpbuf = tmpbuf_dynamic.get();
    }
    
    stream_->read(tmpbuf, chunk_size_);
    size_t read = stream_->gcount();

    //cerr << "read " << read << " bytes" << endl;

    if (stream_->bad())
        exception("stream is bad/has failed 2");
    
    if (read == 0) return buffers_.end();

    uint64_t last_ofs = (buffers_.empty() ? ofs_
                         : buffers_.back().ofs + buffers_.back().size);
    
    //cerr << "last_ofs = " << last_ofs << endl;

    list<Buffer>::iterator result
        = buffers_.insert(buffers_.end(),
                          Buffer(last_ofs, new char[read], read, true));
    
    //cerr << "  now " << buffers_.size() << " buffers active" << endl;

    memcpy(const_cast<char *>(buffers_.back().pos), tmpbuf, read);

    return result;
}

void
ParseContext::
set_chunk_size(size_t size)
{
    if (size == 0)
        throw MLDB::Exception("ParseContext::chunk_size(): invalid chunk size");
    chunk_size_ = size;
}

size_t
ParseContext::
readahead_available() const
{
    if (eof()) return 0;
    size_t in_current_buffer = ebuf_ - cur_;

    size_t in_future_buffers = 0;
    for (std::list<Buffer>::const_iterator it = boost::next(current_),
             end = buffers_.end();
         it != end;  ++it) {
        in_future_buffers += it->size;
    }

    return in_current_buffer + in_future_buffers;
}

size_t
ParseContext::
total_buffered() const
{
    if (eof()) return 0;

    size_t result = 0;
    for (std::list<Buffer>::const_iterator it = buffers_.begin(),
             end = buffers_.end();
         it != end;  ++it) {
        result += it->size;
    }

    return result;
}

} // namespace MLDB

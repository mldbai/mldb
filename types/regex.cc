/** regex.cc
    Jeremy Barnes, 31 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Regular expression class that can deal with both ICU and non-ICU
    regexes.
*/

#include <boost/regex/icu.hpp>
#include <boost/regex.hpp>
#include "regex.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/value_description.h"
#include "mldb/ext/re2/re2/re2.h"
#include "mldb/base/optimized_path.h"
#include "mldb/ext/concurrentqueue/concurrentqueue.h"

#include <mutex>
#include <iostream>


using namespace std;
using moodycamel::ConcurrentQueue;


// NOTE: boost::regex is used due to issues with std::regex in GCC 4.6
// Once we move past there, we can remove boost::regex and replace with
// std::regex, assuming that the locale properly supports the same
// functionality as ICU.

namespace MLDB {

#define DO_FLAG_SYNTAX(flag)                            \
    if (flags & std::regex_constants::flag)             \
        result = result | boost::regex_constants::flag;

static boost::regex::flag_type
syntaxFlagsToBoost(std::regex_constants::syntax_option_type flags)
{
    int result = 0;

    if (flags & std::regex_constants::icase)
        result = result | boost::regex_constants::icase;
    if (flags & std::regex_constants::nosubs)
        result = result | boost::regex_constants::nosubs;
    if (flags & std::regex_constants::optimize)
        result = result | boost::regex_constants::optimize;
    if (flags & std::regex_constants::collate)
        result = result | boost::regex_constants::collate;
    if (flags & std::regex_constants::ECMAScript)
        result = result | boost::regex_constants::ECMAScript;
    if (flags & std::regex_constants::basic)
        result = result | boost::regex_constants::basic;
    if (flags & std::regex_constants::extended)
        result = result | boost::regex_constants::extended;
    if (flags & std::regex_constants::awk)
        result = result | boost::regex_constants::awk;
    if (flags & std::regex_constants::grep)
        result = result | boost::regex_constants::grep;
    if (flags & std::regex_constants::egrep)
        result = result | boost::regex_constants::egrep;
    
    return result;
}

/** Convert the flags to the equivalent RE2 options, and return if
    it's possible to run a regex with these flags in RE2.
*/
static bool
syntaxFlagsToRE2(std::regex_constants::syntax_option_type flags,
                 RE2::Options & result)
{
    result.Copy(RE2::Options());
    result.set_encoding(RE2::Options::EncodingUTF8);
    
    if (flags & std::regex_constants::icase) {
        result.set_case_sensitive(false);
        flags = flags & ~std::regex_constants::icase;
    }
    else result.set_case_sensitive(true);

    if (flags & std::regex_constants::nosubs) {
        result.set_never_capture(true);
        flags = flags & ~std::regex_constants::nosubs;
    }
    else result.set_never_capture(false);

    if (flags & std::regex_constants::optimize) {
        result.set_max_mem(RE2::Options::kDefaultMaxMem * 4);
        flags = flags & ~std::regex_constants::optimize;
    }

    if (flags & std::regex_constants::ECMAScript) {
        flags = flags & ~std::regex_constants::ECMAScript;
    }
    if (flags & std::regex_constants::extended) {
        result.set_posix_syntax(true);
        flags = flags & ~std::regex_constants::extended;
    }

    if (flags) {
        // leftover flags, we can't do with re2
        return false;
    }
    
    return true;
}

#define DO_FLAG(flag)                                   \
    if ((flags & std::regex_constants::flag) != 0)       \
        result = result | boost::regex_constants::flag;

static boost::regex_constants::match_flag_type
matchFlagsToBoost(std::regex_constants::match_flag_type flags)
{
    auto result = boost::regex_constants::match_flag_type();

    DO_FLAG(match_not_bol);
    DO_FLAG(match_not_eol);
    DO_FLAG(match_not_bow);
    DO_FLAG(match_not_eow);
    DO_FLAG(match_any);
    DO_FLAG(match_not_null);
    DO_FLAG(match_continuous);
    DO_FLAG(match_prev_avail);
    DO_FLAG(format_default);
    DO_FLAG(format_sed);
    DO_FLAG(format_no_copy);
    DO_FLAG(format_first_only);
    
    return result;
}

/*****************************************************************************/
/* MATCH RESULTS                                                             */
/*****************************************************************************/

struct MatchResults::Impl {
    boost::match_results<std::string::const_iterator> matched;
    const Utf8String & str;

    Impl(const Utf8String & str)
        : str(str)
    {
    }

    SubMatch wrap(const boost::sub_match<std::string::const_iterator> & match) const
    {
        SubMatch result;
        result.first = str.wrapIterator(match.first);
        result.second = str.wrapIterator(match.second);
        result.matched = match.matched;
        return result;
    }

    SubMatch getMatch(int n) const
    {
        return wrap(matched.operator [] (n));
    }

    SubMatch getPrefix() const
    {
        return wrap(matched.prefix());
    }

    SubMatch getSuffix() const
    {
        return wrap(matched.suffix());
    }
};

MatchResults::MatchResults()
{
}
    
bool
MatchResults::
empty() const
{
    ExcAssert(impl);
    return impl->matched.empty();
}

size_t
MatchResults::
size() const
{
    ExcAssert(impl);
    return impl->matched.size();
}

size_t
MatchResults::
max_size() const
{
    ExcAssert(impl);
    return impl->matched.max_size();
}

MatchResults::difference_type
MatchResults::
length(size_t n) const
{
    ExcAssert(impl);
    return impl->getMatch(n).length();
}

Utf8String
MatchResults::
str(size_t n) const
{
    ExcAssert(impl);
    return impl->getMatch(n).str();
}

SubMatch
MatchResults::
operator [] (size_t n) const
{
    ExcAssert(impl);
    return impl->getMatch(n);
}

SubMatch
MatchResults::
prefix() const
{
    ExcAssert(impl);
    return impl->getPrefix();
}

SubMatch
MatchResults::
suffix() const
{
    ExcAssert(impl);
    return impl->getSuffix();
}

namespace {

static const OptimizedPath attemptRe2("mldb.regex.attemptRe2");

} // file scope


/*****************************************************************************/
/* REGEX                                                                     */
/*****************************************************************************/

struct Regex::Impl {
    Impl(const Utf8String & surface,
         std::regex::flag_type syntaxFlags)
        : numUses(0), surface_(surface), flags_(syntaxFlags),
          utf8(nullptr)
    {
        if (attemptRe2.take()) {
            RE2::Options options;
            if (syntaxFlagsToRE2(syntaxFlags, options)) {
                re2.reset(new RE2(surface.rawData(), options));
                if (!re2->ok()) {
                    cerr << "error initializing RE2 version: "
                         << re2->error() << endl;
                    re2.reset();
                }
            }
        }

        // Dry run so exceptions will happen here not later
        syntaxFlagsToBoost(syntaxFlags);

        if (!re2) {
            ensureBoost();
        }
    }

    ~Impl()
    {
        // Strongest possible memory barrier to ensure that any other
        // threads that have released a RE2 have all of their stores
        // committed, so that this thread can free then all.  Note
        // that it's undefined behavior to access an object that's
        // running its destructor from another thread, so we are OK
        // once this barrier is done.
        std::atomic_thread_fence(std::memory_order_seq_cst);

        // Free the contents of the queue
        RE2 * result = nullptr;
        while (re2Queue.try_dequeue(result)) {
            delete result;
        }
    }
    
    std::shared_ptr<RE2> getRe2() const
    {
        if (!re2) {
            return nullptr;  // re2 is not possible here
        }

        // The first 1,000 times, we use a single thread.  After that
        // it's only on thread contention that we use more.  The goal
        // is to avoid creating thread-specific objects when in fact
        // the bottleneck isn't the number of threads.
        if (numUses < 1000) {
            numUses += 1;
            return std::shared_ptr<RE2>(re2.get(), [] (RE2 *) {});            
        }

        RE2 * result = nullptr;
        if (!re2Queue.try_dequeue(result)) {
            RE2::Options options;
            syntaxFlagsToRE2(flags_, options);
            result = new RE2(surface_.rawData(), options);
        }

        ExcAssert(result);

        try {
            return std::shared_ptr<RE2>(result,
                                        [=] (RE2 * ptr) { returnRe2(ptr); });
        } catch (...) {
            delete result;
            throw;
        }
    }

    mutable std::atomic<uint64_t> numUses;
    mutable ConcurrentQueue<RE2 *> re2Queue;

    void returnRe2(RE2 * ptr) const
    {
        re2Queue.enqueue(ptr);
    }
    
    const boost::u32regex & ensureBoost() const
    {
        if (utf8.load())
            return *utf8.load();
        
        std::unique_lock<std::mutex> guard(utf8Mutex);
        if (utf8.load())
            return *utf8.load();
        
        utf8Owner.reset(new boost::u32regex
                        (boost::make_u32regex(surface_.rawData(),
                                              syntaxFlagsToBoost(flags_))));
        utf8.store(utf8Owner.get());

        return *utf8.load();
    }
    
    std::regex::flag_type flags() const
    {
        return flags_;
    }

    const Utf8String & surface() const
    {
        return surface_;
    }

    int mark_count() const
    {
        if (re2)
            return re2->NumberOfCapturingGroups();

        ensureBoost();
        return utf8.load()->mark_count();
    }

    /// Surface form of regular expression that was used to construct it
    Utf8String surface_;

    /// Flags used in constructor
    std::regex::flag_type flags_;

    /// Regex compiled to match UTF-8 data
    mutable std::atomic<const boost::u32regex *> utf8;

    /// RE2 version of regex; only if there are no flags
    std::unique_ptr<RE2> re2;

    mutable std::mutex utf8Mutex;
    mutable std::unique_ptr<boost::u32regex> utf8Owner;
    
    /// Regex compiled to match ASCII data.  Only initialized if the
    /// regex string is pure ASCII.
    //std::unique_ptr<boost::regex> ascii;
};

Regex::
Regex() noexcept
{
}

Regex::
Regex(const Utf8String & r, std::regex::flag_type flags)
{
    assign(r, flags);
}

Regex::
Regex(const std::string & r, std::regex::flag_type flags)
{
    assign(r, flags);
}

Regex::
Regex(const char * r, std::regex::flag_type flags)
{
    assign(r, flags);
}

Regex::
Regex(const wchar_t * r, std::regex::flag_type flags)
{
    assign(r, flags);
}

Regex::
~Regex()
{
}

Regex &
Regex::
operator = (Regex && other) noexcept
{
    Regex newMe(std::move(other));
    swap(newMe);
    return *this;
}

Regex &
Regex::
operator = (const Regex & other) noexcept
{
    Regex newMe(std::move(other));
    swap(newMe);
    return *this;
}
    
Regex &
Regex::
assign(const Utf8String & r, std::regex::flag_type flags)
{
    impl.reset(new Impl(r, flags));
    return *this;
}

Regex &
Regex::
assign(const std::string & r, std::regex::flag_type flags)
{
    impl.reset(new Impl(r, flags));
    return *this;
}

Regex &
Regex::
assign(const char * r, std::regex::flag_type flags)
{
    impl.reset(new Impl(r, flags));
    return *this;
}

Regex &
Regex::
assign(const wchar_t * r, std::regex::flag_type flags)
{
    impl.reset(new Impl(r, flags));
    return *this;
}

void
Regex::
swap(Regex & other) noexcept
{
    std::swap(impl, other.impl);
}

int
Regex::
mark_count() const
{
    ExcAssert(impl);
    return impl->mark_count();
}

int
Regex::
flags() const
{
    ExcAssert(impl);
    return impl->flags();
}

const Utf8String &
Regex::
surface() const
{
    ExcAssert(impl);
    return impl->surface();
}

bool
Regex::
operator == (const Regex & other) const
{
    if (initialized() != other.initialized())
        return false;
    if (!initialized())
        return true; // both are null

    return surface() == other.surface()
        && flags() == other.flags();
}

bool
Regex::
operator != (const Regex & other) const
{
    return ! operator == (other);
}

bool
Regex::
operator < (const Regex & other) const
{
    if (initialized() < other.initialized())
        return true;
    if (initialized() > other.initialized())
        return false;
    if (!initialized())
        return true; // both are null

    // If we get here, neither are null
    ExcAssert(initialized() && other.initialized());

    // Now we compare the surface strings
    return surface() < other.surface()
                       || (surface() == other.surface()
                           && flags() < other.flags());
}


/*****************************************************************************/
/* REGEX ALGORITHMS                                                          */
/*****************************************************************************/

Utf8String regex_replace(const Utf8String & str,
                         const Regex & regex,
                         const Utf8String & format,
                         std::regex_constants::match_flag_type flags)
{
    ExcAssert(regex.impl);

    if (regex.impl->re2 && flags == std::regex_constants::match_default) {
        std::string s = str.rawString();
        RE2::Replace(&s, *regex.impl->getRe2(), format.rawString());
        return std::move(s);
    }

    std::basic_string<int32_t> matchStr(str.begin(), str.end());
    std::basic_string<int32_t> replacementStr(format.begin(), format.end());

    auto result = boost::u32regex_replace(matchStr, regex.impl->ensureBoost(),
                                          replacementStr);
    
    return Utf8String(std::basic_string<char32_t>(result.begin(), result.end()));
}

bool regex_search(const Utf8String & str,
                  const Regex & regex,
                  std::regex_constants::match_flag_type flags)
{
    ExcAssert(regex.impl);
    return boost::u32regex_search(str.rawString(), regex.impl->ensureBoost(),
                                  matchFlagsToBoost(flags));
    
    // TODO: optimization for when both str and regex are ASCII only
    
}

bool regex_search(const Utf8String & str,
                  MatchResults & matches,
                  const Regex & regex,
                  std::regex_constants::match_flag_type flags)
{
    ExcAssert(regex.impl);

    auto impl = std::make_shared<MatchResults::Impl>(str);

    bool res = boost::u32regex_search(str.rawString(),
                                      impl->matched,
                                      regex.impl->ensureBoost(),
                                      matchFlagsToBoost(flags));

    matches.impl = std::move(impl);

    return res;
}

bool regex_match(const Utf8String & str,
                 const Regex & regex,
                 std::regex_constants::match_flag_type flags)
{
    return regex_match(str.rawData(), str.rawLength(), regex, flags);
}

bool regex_match(const Utf8String & str,
                 MatchResults & matches,
                 const Regex & regex,
                 std::regex_constants::match_flag_type flags)
{
    ExcAssert(regex.impl);

    regex.impl->ensureBoost();

    auto impl = std::make_shared<MatchResults::Impl>(str);

    bool res = boost::u32regex_match(str.rawString(),
                                     impl->matched,
                                     regex.impl->ensureBoost(),
                                     matchFlagsToBoost(flags));
    
    matches.impl = std::move(impl);

    return res;
}

bool regex_match(const char * utf8Str, size_t len,
                 const Regex & regex,
                 std::regex_constants::match_flag_type flags)
{
    ExcAssert(regex.impl);

    if (regex.impl->re2 && flags == std::regex_constants::match_default) {
        return RE2::FullMatch(re2::StringPiece(utf8Str, len),
                              *regex.impl->getRe2());
    }

    regex.impl->ensureBoost();

    return boost::u32regex_match(utf8Str, utf8Str + len,
                                 regex.impl->ensureBoost(),
                                 matchFlagsToBoost(flags));
}


/*****************************************************************************/
/* VALUE DESCRIPTION                                                         */
/*****************************************************************************/

struct RegexDescription
    : public ValueDescriptionT<Regex> {

    virtual void parseJsonTyped(Regex * val,
                                JsonParsingContext & context) const
    {
        if (context.isNull()) {
            context.expectNull();
            *val = Regex();
        }
        *val = context.expectStringUtf8();
    }

    virtual void printJsonTyped(const Regex * val,
                                JsonPrintingContext & context) const
    {
        if (!val->initialized()) {
            context.writeNull();
        }
        else {
            context.writeStringUtf8(val->surface());
        }
    }
};

DEFINE_VALUE_DESCRIPTION_NS(Regex, RegexDescription);


} // namespace MLDB

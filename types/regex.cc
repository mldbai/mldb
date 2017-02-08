// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** regex.cc
    Jeremy Barnes, 31 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Regular expression class that can deal with both ICU and non-ICU
    regexes.
*/

#include <boost/regex/icu.hpp>
#include <boost/regex.hpp>
#include "regex.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/value_description.h"


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


/*****************************************************************************/
/* REGEX                                                                     */
/*****************************************************************************/

struct Regex::Impl {
    Impl(const Utf8String & surface,
         std::regex::flag_type syntaxFlags)
        : surface_(surface), flags_(syntaxFlags)
    {
        utf8 = boost::make_u32regex(surface.rawData(),
                                    syntaxFlagsToBoost(syntaxFlags));
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
        return utf8.mark_count();
    }

    /// Surface form of regular expression that was used to construct it
    Utf8String surface_;

    /// Flags used in constructor
    std::regex::flag_type flags_;

    /// Regex compiled to match UTF-8 data
    boost::u32regex utf8;

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
    std::basic_string<int32_t> matchStr(str.begin(), str.end());
    std::basic_string<int32_t> replacementStr(format.begin(), format.end());

    auto result = boost::u32regex_replace(matchStr, regex.impl->utf8,
                                          replacementStr);
    
    return Utf8String(std::basic_string<char32_t>(result.begin(), result.end()));
}

bool regex_search(const Utf8String & str,
                  const Regex & regex,
                  std::regex_constants::match_flag_type flags)
{
    ExcAssert(regex.impl);
    return boost::u32regex_search(str.rawString(), regex.impl->utf8,
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
                                      regex.impl->utf8,
                                      matchFlagsToBoost(flags));

    matches.impl = std::move(impl);

    return res;
}

bool regex_match(const Utf8String & str,
                 const Regex & regex,
                 std::regex_constants::match_flag_type flags)
{
    ExcAssert(regex.impl);
    return boost::u32regex_match(str.rawString(), regex.impl->utf8,
                                 matchFlagsToBoost(flags));
}

bool regex_match(const Utf8String & str,
                 MatchResults & matches,
                 const Regex & regex,
                 std::regex_constants::match_flag_type flags)
{
    ExcAssert(regex.impl);

    auto impl = std::make_shared<MatchResults::Impl>(str);

    bool res = boost::u32regex_match(str.rawString(),
                                     impl->matched,
                                     regex.impl->utf8,
                                     matchFlagsToBoost(flags));
    
    matches.impl = std::move(impl);

    return res;
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
        context.writeStringUtf8(val->surface());
    }
};

DEFINE_VALUE_DESCRIPTION_NS(Regex, RegexDescription);


} // namespace MLDB

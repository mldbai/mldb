/** regex.h                                                        -*- C++ -*-
    UTF-8 aware regex class, wrapping the functionality available
    elsewhere.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <regex>
#include "string.h"
#include "mldb/base/exc_assert.h"
#include "mldb/types/value_description_fwd.h"


namespace MLDB {


/*****************************************************************************/
/* SUB MATCH                                                                 */
/*****************************************************************************/

/** Mirrors std::sub_match, but operates properly with UTF-8 */

struct SubMatch: std::pair<Utf8String::const_iterator,
                           Utf8String::const_iterator> {
    SubMatch()
        : matched(false)
    {
    }

    bool matched;

    size_t length() const
    {
        ExcAssert(matched);
        return std::distance(first, second);
    }

    Utf8String str() const
    {
        ExcAssert(matched);
        return Utf8String(first, second);
    }

    operator Utf8String () const
    {
        return str();
    }
};

/*****************************************************************************/
/* MATCH RESULTS                                                             */
/*****************************************************************************/

/** Mirrors std::match_results, but operates properly with UTF-8 */

struct MatchResults {
    typedef std::ptrdiff_t difference_type;
    typedef SubMatch const_reference;
    typedef SubMatch reference;
    typedef Utf8String string_type;

    MatchResults();
    
    bool ready() const
    {
        return impl.get();
    }

    bool empty() const;

    size_t size() const;

    size_t max_size() const;

    difference_type length(size_t n) const;

    Utf8String str(size_t n) const;

    SubMatch operator [] (size_t n) const;

    SubMatch prefix() const;

    SubMatch suffix() const;

    struct Impl;
    std::shared_ptr<Impl> impl;
};


/*****************************************************************************/
/* REGEX                                                                     */
/*****************************************************************************/

/** Regular expression class that can handle both UTF-8 and ASCII regular
    expressions transparently.

    This should have the same interface as std::regex.
*/

struct Regex {
    Regex() noexcept;

    typedef std::regex_constants::syntax_option_type flag_type;

    static constexpr flag_type DEFAULT_FLAGS = std::regex_constants::ECMAScript;

    Regex(const Utf8String & r, std::regex::flag_type flags = DEFAULT_FLAGS);
    Regex(const std::string & r, std::regex::flag_type flags = DEFAULT_FLAGS);
    Regex(const char * r, std::regex::flag_type flags = DEFAULT_FLAGS);
    Regex(const wchar_t * r, std::regex::flag_type flags = DEFAULT_FLAGS);

    Regex(const Regex & other) = default;
    Regex(Regex && other) = default;
    
    ~Regex();

    Regex & operator = (Regex && other) noexcept;
    Regex & operator = (const Regex & other) noexcept;
    
    Regex & assign(const Utf8String & r, std::regex::flag_type flags = DEFAULT_FLAGS);
    Regex & assign(const std::string & r, std::regex::flag_type flags = DEFAULT_FLAGS);
    Regex & assign(const char * r, std::regex::flag_type flags = DEFAULT_FLAGS);
    Regex & assign(const wchar_t * r, std::regex::flag_type flags = DEFAULT_FLAGS);

    void swap(Regex & other) noexcept;

    int mark_count() const;

    int flags() const;

    /** Is this initialized? */
    bool initialized() const { return !!impl; }

    /** Return the surface form that was used to build this regex. */
    const Utf8String & surface() const;

    bool operator == (const Regex & other) const;
    bool operator != (const Regex & other) const;
    bool operator < (const Regex & other) const;

    /** Default match results type. */
    typedef std::match_results<Utf8String::const_iterator> match_results;

    struct Impl;
    std::shared_ptr<Impl> impl;
};

// Allow swapping in standard algorithms
inline void swap(Regex & r1, Regex & r2) noexcept
{
    r1.swap(r2);
}

PREDECLARE_VALUE_DESCRIPTION(Regex);


/*****************************************************************************/
/* REGEX ALGORITHMS                                                          */
/*****************************************************************************/

Utf8String regex_replace(const Utf8String & str,
                         const Regex & regex,
                         const Utf8String & format,
                         std::regex_constants::match_flag_type flags
                         = std::regex_constants::match_default);

bool regex_search(const Utf8String & str,
                  const Regex & regex,
                  std::regex_constants::match_flag_type flags
                      = std::regex_constants::match_default);

bool regex_search(const Utf8String & str,
                  MatchResults & matches,
                  const Regex & regex,
                  std::regex_constants::match_flag_type flags
                      = std::regex_constants::match_default);

bool regex_match(const Utf8String & str,
                 const Regex & regex,
                 std::regex_constants::match_flag_type flags
                     = std::regex_constants::match_default);

bool regex_match(const Utf8String & str,
                 MatchResults & matches,
                 const Regex & regex,
                 std::regex_constants::match_flag_type flags
                     = std::regex_constants::match_default);

} // namespace MLDB

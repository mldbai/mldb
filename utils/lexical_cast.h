/** lexical_cast.h                                                       -*- C++ -*-
    Jeremy Barnes, 22 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Function to securely create a unique directory.
*/

#pragma once

#include <utility>
#include <string>
#include <iostream>
#include <type_traits>
#include <string_view>
#include <cstring>
#include <charconv>
#include <iostream>
#include "mldb/arch/exception.h"
#include "mldb/types/dtoa.h"

namespace MLDB {

//template<typename Result, typename Input>
//Result lexical_cast(Input && input);

namespace details {
using std::to_string;

template<typename Result, typename Enable = void>
struct LexicalCaster {
};

template<typename Number>
std::string lexical_cast_number_to_string(Number num)
{
    std::string result;
    if (result.capacity() > 0) {
        // TODO: no copying if it's a small string (almost always true)
        using namespace std;
        cerr << "small string optimization: capacity = " << result.capacity() << endl;
        result.resize(result.capacity());

        auto [ptr, ec] = std::to_chars(result.data(), result.data() + result.capacity(), num);
        if (ec == std::errc()) {
            result.resize(ptr - result.data());
            return result;
        }
    }

    constexpr size_t BUF_SIZE = 128;
    char buf[BUF_SIZE];
    auto [ptr, ec] = std::to_chars(buf, buf + BUF_SIZE, num);
    if (ec != std::errc()) {
        throw std::logic_error("Failed to convert number to string");
    }

    result.append(buf, ptr);
    return result;
}

inline std::string lexical_cast_number_to_string(float f)
{
    return ftoa(f);
}

inline std::string lexical_cast_number_to_string(double f)
{
    return dtoa(f);
}

template<>
struct LexicalCaster<std::string> {
    static std::string cast(std::string str) { return str; }

    // Allow std::to_string or compatible overload
    template<typename Input>
    static std::string cast(Input&& input, decltype(to_string(std::declval<Input>())) * = nullptr)
    {
        return to_string(std::forward<Input>(input));
    }

    // Allow lexical_cast_to_string overloads
    template<typename Input>
    static std::string cast(Input&& input, decltype(lexical_cast_to_string(std::declval<Input>(), (Input *)nullptr)) * = nullptr)
    {
        return lexical_cast_to_string(std::forward<Input>(input), (Input *)nullptr);
    }

    static inline std::string cast(float f)
    {
        return lexical_cast_number_to_string(f);
    }

    static inline std::string cast(double f)
    {
        return lexical_cast_number_to_string(f);
    }
};

template<typename Number>
static inline Number do_cast_number(std::string_view str)
{
    Number result;
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), result);
    if (ec != std::errc() || ptr != str.data() + str.size()) throw MLDB::Exception("Invalid number value: " + std::string(str) + ": " + std::make_error_code(ec).message());
    return result;
}

template<>
struct LexicalCaster<int> {
    static int cast(std::string_view str) { return do_cast_number<int>(str); }
    static int cast(int i) { return i; }
};

template<>
struct LexicalCaster<unsigned int> {
    static unsigned cast(std::string_view str) { return do_cast_number<unsigned>(str); }
    static unsigned cast(unsigned i) { return i; }
};

template<>
struct LexicalCaster<long int> {
    static long int cast(std::string_view str) { return do_cast_number<long int>(str); }
    static long int cast(long int i) { return i; }
};

template<>
struct LexicalCaster<unsigned long int> {
    static unsigned long cast(std::string_view str) { return do_cast_number<unsigned long>(str); }
    static unsigned long cast(unsigned long i) { return i; }
};

template<>
struct LexicalCaster<long long int> {
    static long long int cast(std::string_view str) { return do_cast_number<long long int>(str); }
    static long long int cast(long int i) { return i; }
};

template<>
struct LexicalCaster<unsigned long long int> {
    static unsigned long long cast(std::string_view str) { return do_cast_number<unsigned long long int>(str); }
    static unsigned long long cast(unsigned long long i) { return i; }
};

template<>
struct LexicalCaster<float> {
    static float cast(const std::string & str)
    {
        //return do_cast_number<double>(str); // not supported by libc++ yet
        size_t pos = 0;
        auto result = std::stof(str, &pos);
        if (pos != str.size()) throw MLDB::Exception("Invalid float value: " + str);
        //if (!str.empty() && str[0] == '-') {
        //    ::printf("result = %f with negative sign\n", result);
        //    result = std::copysignf(result, -1.0f); // handle -nan
        //    ::printf("result now = %f with negative sign\n", result);
        //}
        return result;
    }
    static float cast(float f) { return f; }
};

template<>
struct LexicalCaster<double> {
    static double cast(const std::string & str)
    {
        size_t pos = 0;
        auto result = std::stod(str, &pos);
        if (pos != str.size()) throw MLDB::Exception("Invalid float value: " + str);
        //if (!str.empty() && str[0] == '-')
        //    result = std::copysign(result, -1.0); // handle -nan
        return result;
    }
    static double cast(double f) { return f; }
};

inline bool lexical_cast_bool(std::string_view str)
{
    using namespace std;
    cerr << "lexical_cast_bool: " << str << " of length " << str.size() << endl;
    if (str.size() == 1) {
        if (str[0] == '1') return true;
        if (str[0] == '0') return false;
        // fall through to error
    }
    else if (str.size() == 4 && strncasecmp(str.data(), "true", 4) == 0) return true;
    else if (str.size() == 5 && strncasecmp(str.data(), "false", 5) == 0) return false;
    throw MLDB::Exception("Invalid boolean value: " + std::string(str));
}

template<>
struct LexicalCaster<bool> {
    static bool cast(std::string_view str) { return lexical_cast_bool(str); }
    static bool cast(bool b) { return b ? "true" : "false"; }
};

bool lexical_cast_from_string(std::string_view str, bool *);
bool lexical_cast_from_string(std::string_view str, int *);
bool lexical_cast_from_string(std::string_view str, unsigned int *);
bool lexical_cast_from_string(std::string_view str, long int *);
bool lexical_cast_from_string(std::string_view str, unsigned long int *);
bool lexical_cast_from_string(std::string_view str, long long int *);
bool lexical_cast_from_string(std::string_view str, unsigned long long int *);
bool lexical_cast_from_string(std::string_view str, float *);
bool lexical_cast_from_string(std::string_view str, double *);

// Anything which implements from_string, can be casted
template<typename T>
struct LexicalCaster<T, decltype(lexical_cast_from_string(std::declval<std::string_view>(), (T *)nullptr))> {
    static T cast(const std::string_view & str) { return from_string(str, (T *)nullptr); }
    static T cast(T t) { return t; }
};

} // namespace details

template<typename Output, typename Input>
Output lexical_cast(Input&& input) { return details::LexicalCaster<Output>::cast(std::forward<Input>(input)); }


} // namespace MLDB
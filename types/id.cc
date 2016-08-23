// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* id.cc
   Jeremy Barnes, 17 February 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

*/

#include <boost/algorithm/string.hpp>
#include "id.h"
#include "mldb/arch/bit_range_ops.h"
#include "mldb/arch/format.h"
#include "mldb/arch/exception.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/utils/less.h"
#include "mldb/types/value_description.h"
#include "mldb/ext/jsoncpp/value.h"
#include "mldb/ext/cityhash/src/city.h" // Google city hash function
#include "mldb/ext/jsoncpp/json.h"
#include <iostream>
#include "mldb/ext/jsoncpp/value.h"
#include <boost/multiprecision/cpp_int.hpp>

#if JML_BITS == 32
#endif

using namespace ML;
using namespace std;


namespace Datacratic {

#if JML_BITS < 64

typedef boost::multiprecision::number
    <boost::multiprecision::cpp_int_backend
     <128, 128,
      boost::multiprecision::unsigned_magnitude,
      boost::multiprecision::unchecked, void> > UInt128;

static inline UInt128 make128(uint64_t l, uint64_t h)
{
    UInt128 result = h;
    result <<= 64;
    result += l;
    return result;
}

static inline uint64_t getLow(UInt128 res)
{
    return res.convert_to<uint64_t>();
}

static inline uint64_t getHigh(UInt128 res)
{
    return (res >> 64).convert_to<uint64_t>();
}

static inline unsigned divmod10(UInt128 & val)
{
    unsigned res = (val % 10).convert_to<unsigned>();
    val /= 10;
    return res;
}

static inline unsigned divmod64(UInt128 & val)
{
    unsigned res = (val & 63).convert_to<unsigned>();
    val >>= 6;
    return res;
}

#else // 64+ bits

typedef __uint128_t UInt128;

static inline UInt128 make128(uint64_t l, uint64_t h)
{
    UInt128 result = h;
    result <<= 64;
    result += l;
    return result;
}

static inline uint64_t getLow(UInt128 res)
{
    return res;
}

static inline uint64_t getHigh(UInt128 res)
{
    return res >> 64;
}

static inline unsigned divmod10(UInt128 & val)
{
    unsigned res = val % 10;
    val /= 10;
    return res;
}

static inline unsigned divmod64(UInt128 & val)
{
    unsigned res = val & 63;
    val >>= 6;
    return res;
}
#endif



/*****************************************************************************/
/* ID                                                                        */
/*****************************************************************************/


static const size_t max64_base10_len = sizeof("9223372036854775807") - 1;

static const signed char hexToDecLookups[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
};

JML_ALWAYS_INLINE int hexToDec(unsigned c) JML_CONST_FN;

JML_ALWAYS_INLINE int hexToDec(unsigned c)
{
    //cerr << "c = " << (char)c
    //     << " index " << (c & 0x7f) << " value = "
    //     << (int)lookups[c & 0x7f] << endl;
    int mask = (c <= 0x7f);
    return hexToDecLookups[c & 0x7f] * mask - 1 + mask;
}

// Not quite base64... these are re-arranged so that their ASCII order
// corresponds to their integer value so they sort uniformly
static const signed char base64ToDecLookups[128] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  0, -1, -1, -1,  1,
     2,  3,  4,  5,  6,  7,  8,  9, 10, 11, -1, -1, -1, -1, -1, -1,

    -1, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, -1, -1, -1, -1, -1,
    -1, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52,
    53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, -1, -1, -1, -1, -1,
};

JML_ALWAYS_INLINE int base64ToDec(unsigned c)
{
    int mask = (c <= 0x7f);
    return base64ToDecLookups[c & 0x7f] * mask - 1 + mask;
}

inline int hexToDec3(int c)
{
    int d = c & 0x1f;
    int i = (c & 0x60) >> 5;
    d += (i == 1) * -16;
    d += (i >= 2) * 9;
    bool h = isxdigit(c);
    return h * d - (!h);
}

inline int hexToDec2(int c)
{
    int v;

    if (c >= '0' && c <= '9')
        v = c - '0';
    else if (c >= 'a' && c <= 'f')
        v = c + 10 - 'a';
    else if (c >= 'A' && c <= 'F')
        v = c + 10 - 'A';
    else
        v = -1;

    return v;
}

Id::Id(const char * value, Type type)
    : type(NONE), val1(0), val2(0)
{
    parse(value, strlen(value), type);
}
    
Id::Id(const wchar_t * value, Type type)
{
    Utf8String s(value);
    parse(s.rawData(), s.rawLength(), type);
}

void
Id::
parse(const char * value, size_t len, Type type)
{
    Id r;

    auto finish = [&] ()
        {
            //if (r.toString() != value)
            //    throw ML::Exception("Id::parse() modified an Id: input " + value
            //                        + " output " + r.toString());
            if (r.type != type && type != UNKNOWN)
                throw ML::Exception("Id::parse() changed type from %d to %d parsing %s",
                                    r.type, type, value);

            *this = std::move(r);
        };
    
    if ((type == UNKNOWN || type == NONE) && len == 0) {
        r.type = NONE;
        r.val1 = r.val2 = 0;
        finish();
        return;
    }

    if ((type == UNKNOWN || type == NULLID) && len == 4
        && strcmp(value, "null") == 0) {
        //throw ML::Exception("null id");
        r.type = NULLID;
        r.val1 = r.val2 = 0;
        finish();
        return;
    }

    if ((type == UNKNOWN || type == BIGDEC) && (len == 1 && value[0] == '0')) {
        r.type = BIGDEC;
        r.val1 = r.val2 = 0;
        finish();
        return;
    }

    while ((type == UNKNOWN || type == UUID || type == UUID_CAPS) && len == 36) {
        // not really a while...
        // Try a uuid
        // AGID: --> 0828398c-5965-11e0-84c8-0026b937c8e1

        if (value[8] != '-') break;
        if (value[13] != '-') break;
        if (value[18] != '-') break;
        if (value[23] != '-') break;

        unsigned f1;
        short f2, f3, f4;
        unsigned long long f5;

        const char * p = value;
        bool failed = false;
        int capsType = -1; // -1 = unknown, 0 = no, 1 = yes

        auto scanRange = [&] (int start, int len) -> unsigned long long
            {
                unsigned long long val = 0;
                for (unsigned i = start;  i != start + len;  ++i) {
                    int c = p[i];
                    {
                        // case handling
                        if (c >= 'a' && c <= 'f') {
                            if (capsType == -1) {
                                capsType = 0;
                            }
                            else if (capsType != 0) {
                                failed = true;
                                return val;
                            }
                        }
                        else if (c >= 'A' && c <= 'F') {
                            if (capsType == -1) {
                                capsType = 1;
                            }
                            else if (capsType != 1) {
                                failed = true;
                                return val;
                            }
                        }
                    }
                    int v = hexToDec(c);
                    if (v == -1) {
                        failed = true;
                        return val;
                    }
                    val = (val << 4) + v;
                }
                return val;
            };

        f1 = scanRange(0, 8);
        f2 = scanRange(9, 4);
        f3 = scanRange(14, 4);
        if (failed) break;
        f4 = scanRange(19, 4);
        f5 = scanRange(24, 12);
        if (failed) break;

        r.type = capsType == 1 ? UUID_CAPS : UUID;
        r.f1 = f1;  r.f2 = f2;  r.f3 = f3;  r.f4 = f4;  r.f5 = f5;
        finish();
        return;
    }

    if ((type == UNKNOWN || type == GOOG128)
        && len == 26 && value[0] == 'C' && value[1] == 'A'
        && value[2] == 'E' && value[3] == 'S' && value[4] == 'E') {

        // Google ID: --> CAESEAYra3NIxLT9C8twKrzqaA

        auto res = make128(0, 0);

        auto b64Decode = [] (int c) -> int
            {
                if (c >= '0' && c <= '9')
                    return c - '0';
                else if (c >= 'A' && c <= 'Z')
                    return 10 + c - 'A';
                else if (c >= 'a' && c <= 'z')
                    return 36 + c - 'a';
                else if (c == '-')
                    return 62;
                else if (c == '_')
                    return 63;
                else return -1;
            };

        bool error = false;
        for (unsigned i = 5;  i < 26 && !error;  ++i) {
            int v = b64Decode(value[i]);
            if (v == -1) error = true;
            res = (res << 6) | v;
        }

        if (!error) {
            r.type = GOOG128;
            r.valLow = getLow(res);
            r.valHigh = getHigh(res);
            finish();
            return;
        }
    }

    if ((type == UNKNOWN || type == BIGDEC)
        && value[0] != '0' && len < 40 /* TODO: better condition */) {
        // Try a big integer
        //ANID: --> 7394206091425759590
        uint64_t res64(0);
        bool error = false;

        int maxLowLen = min(len, max64_base10_len);
        for (unsigned i = 0; i < maxLowLen; ++i) {
            if (!isdigit(value[i])) {
                error = true;
                break;
            }
            res64 = 10 * res64 + value[i] - '0';
        }

        if (!error) {
            if (len <= max64_base10_len) {
                r.type = BIGDEC;
                r.val1 = res64;
                finish();
                return;
            }
            else {
                auto res128 = make128(res64, 0);
                for (unsigned i = maxLowLen; i < len; ++i) {
                    if (!isdigit(value[i])) {
                        error = true;
                        break;
                    }
                    res128 = res128 * 10 + (value[i] - '0');
                }
                if (!error) {
                    r.type = BIGDEC;
                    r.valLow = getLow(res128);
                    r.valHigh = getHigh(res128);
                    finish();
                    return;
                }
            }
        }
    }

    if ((type == UNKNOWN || type == BASE64_96) && len == 16) {
        auto scanRange = [&] (const char * p, size_t l) -> int64_t
            {
                uint64_t res = 0;
                for (unsigned i = 0;  i < l;  ++i) {
                    int c = base64ToDec(p[i]);
                    if (c == -1) return -1;
                    res = res << 6 | c;
                }
                return res;
            };
        
        int64_t high = scanRange(value, 8);
        int64_t low  = scanRange(value + 8, 8);

        if (low != -1 && high != -1) {
            r.type = BASE64_96;
            r.valLow = low | (high << 48);
            r.valHigh = high >> 16;
            finish();
            return;
        }
    }   

    //cerr << "len = " << len
    //     << " value = " << value << " type = " << (int)type << endl;

    while ((type == UNKNOWN || type == HEX128LC) && len == 32) {
        uint64_t high, low;

        const char * p = value;
        bool failed = false;

        auto scanRange = [&] (int start, int len) -> unsigned long long
            {
                uint64_t val = 0;
                for (unsigned i = start;  i != start + len;  ++i) {
                    int c = p[i];
                    int v = hexToDec(c);

                    //cerr << "c = " << c << " " << p[i] << " v = " << v << endl;

                    if (v == -1) {
                        failed = true;
                        return val;
                    }
                    val = (val << 4) + v;
                }

                //cerr << "val = " << val << " failed = " << failed << endl;

                return val;
            };

        high = scanRange(0, 16);
        if (failed)
            break;
        low = scanRange(16, 16);
        if (failed)
            break;

        r.type = HEX128LC;
        r.val1 = high;
        r.val2 = low;
        finish();
        return;
    }

    // Short string if possible
    if (len <= 16) {
        r.type = SHORTSTR;
        val1 = val2 = 0;
        std::copy(value, value + len, r.shortStr);
        finish();
        return;
    }

    // Fall back to string

    r.type = STR;
    r.len = len;
    char * s = new char[r.len + 4];
    StringRep * sr = new (s) StringRep(1);  // one reference
    r.str = sr;
    r.ownstr = true;
#if 0
    memcpy(s, value, len);
#else
    std::copy(value, value + len, sr->data);
#endif
    finish();
    return;
}

const Id &
Id::
compoundId1() const
{
    ExcAssertEqual(type, COMPOUND2);
    return *cmp1;
}

const Id &
Id::
compoundId2() const
{
    ExcAssertEqual(type, COMPOUND2);
    return *cmp2;
}
    
    
size_t
Id::
toStringLength() const
{
    switch (type) {
    case NONE:
        return 0;
    case NULLID:
        return 4;
    case UUID:
    case UUID_CAPS:
        // AGID: --> 0828398c-5965-11e0-84c8-0026b937c8e1
        return 36;
    case GOOG128: {
        // Google ID: --> CAESEAYra3NIxLT9C8twKrzqaA
        return 26;
    }
    case BIGDEC: {
        // TODO: can do better than this...
        return toString().size();
    }
    case BASE64_96: {
        return 16;
    }
    case HEX128LC: {
        return 32;
    }
    case COMPOUND2:
        return compoundId1().toStringLength() + 1 + compoundId2().toStringLength();
    case STR:
        return len;
    case SHORTSTR:
        return strnlen(shortStr, 16);
    default:
        throw ML::Exception("unknown ID type");
    }
}

std::string
Id::
toString() const
{
    switch (type) {
    case NONE:
        return "";
    case NULLID:
        return "null";
    case UUID:
        return ML::format(
            "%08x-%04x-%04x-%04x-%012llx",
            (unsigned)f1, (unsigned)f2, (unsigned)f3, (unsigned)f4,
            (unsigned long long)f5);
    case UUID_CAPS:
        return ML::format(
            "%08X-%04X-%04X-%04X-%012llX",
            (unsigned)f1, (unsigned)f2, (unsigned)f3, (unsigned)f4,
            (unsigned long long)f5);
    case GOOG128: {
        // Google ID: --> CAESEAYra3NIxLT9C8twKrzqaA
        string result = "CAESE                     ";

        auto b64Encode = [] (unsigned i) -> int
            {
                if (i < 10) return i + '0';
                if (i < 36) return i - 10 + 'A';
                if (i < 62) return i - 36 + 'a';
                if (i == 62) return '-';
                if (i == 63) return '_';
                throw ML::Exception("bad goog base64 char");
            };

        auto v = make128(valLow, valHigh);
        for (unsigned i = 0;  i < 21;  ++i) {
            result[25 - i] = b64Encode(divmod64(v));
        }
        return result;
    }
    case BIGDEC: {
        string result;
        if (val2 == 0) {
            if (val1 == 0) {
                return "0";
            }
            result.reserve(max64_base10_len);
            uint64_t v = val1;
            while (v) {
                int c = v % 10;
                v /= 10;
                result += c + '0';
            }
        }
        else {
            auto v = make128(valLow, valHigh);
            result.reserve(32);
            while (v != 0) {
                int c = divmod10(v);
                result += c + '0';
            }
        }
        std::reverse(result.begin(), result.end());
        return result;
    }
    case BASE64_96: {
        string result = "                ";

        auto b64Encode = [] (unsigned i) -> int
            {
                if (i == 0) return '+';
                if (i == 1) return '/';
                if (i < 12) return '0' + i - 2;
                if (i < 38) return 'A' + i - 12;
                if (i < 64) return 'a' + i - 38;
                throw ML::Exception("bad base64 char");
            };
        
        auto v = make128(val1, val2);
        for (unsigned i = 0;  i < 16;  ++i) {
            result[15 - i] = b64Encode(divmod64(v));
        }
        return result;
    }
    case HEX128LC: {
        return ML::format("%016llx%016llx",
                          (unsigned long long)val1,
                          (unsigned long long)val2);
    }
    case COMPOUND2:
        return compoundId1().toString() + ":" + compoundId2().toString();
    case STR:
        return std::string(str->data, str->data + len);
    case SHORTSTR:
        return std::string(shortStr, shortStr + strnlen(shortStr, 16));
    default:
        throw ML::Exception("unknown ID type");
    }
}

Utf8String
Id::
toUtf8String() const
{
    return Utf8String(toString());
}

bool
Id::
empty() const
{
    return !notNull();
}

uint64_t
Id::
hash() const
{
    if (type == NONE || type == NULLID) return 0;
    if (type == SHORTSTR) {
        return CityHash64(shortStr, strnlen(shortStr, 16));
    }
    if (JML_UNLIKELY(type >= STR)) return complexHash();
    return Hash128to64(std::make_pair(val1, val2));
}

bool
Id::
complexEqual(const Id & other) const
{
    if (type == STR)
        return len == other.len && (str == other.str || std::equal(str->data, str->data + len, other.str->data));
    else if (type == COMPOUND2) {
        return compoundId1() == other.compoundId1()
            && compoundId2() == other.compoundId2();
    }
    else throw ML::Exception("unknown Id type");
}

bool
Id::
complexLess(const Id & other) const
{
    if (type == SHORTSTR || type == STR) {
        // TODO: when both are SHORTSTR and on big-endian architectures or
        // where the movbe instruction is supported, we should be able to
        // do a much more efficient comparison.  Possibly it could also be
        // supported by the SSE4 string comparison instructions, eg
        // pcmpstri (eg see here: http://halobates.de/pcmpstr-js/pcmp.html)

        //cerr << "complex less for string" << endl;
        //cerr << "type = " << (int)type << " other.type = " << (int)other.type << endl;

        //cerr << "a.val1 = " << ML::format("%016llx", (long long)val1) << endl;
        //cerr << "a.val2 = " << ML::format("%016llx", (long long)val2) << endl;
        //cerr << "b.val1 = " << ML::format("%016llx", (long long)other.val1) << endl;
        //cerr << "b.val2 = " << ML::format("%016llx", (long long)other.val2) << endl;

        // If the other isn't a string, we don't continue
        if (other.type != STR && other.type != SHORTSTR) {
            return type < other.type;
        }
         
        size_t len1 = toStringLength();
        const char * data1 = stringData();
        size_t len2 = other.toStringLength();
        const char * data2 = other.stringData();

        return std::lexicographical_compare(data1, data1 + len1, data2, data2 + len2);
    }
    else if (type == COMPOUND2) {
        return ML::less_all(compoundId1(), other.compoundId1(),
                            compoundId2(), other.compoundId2());
    }
    else throw ML::Exception("unknown Id type");
}

uint64_t
Id::
complexHash() const
{
    if (type == STR)
        return CityHash64(str->data, len);
    else if (type == COMPOUND2) {
        return Hash128to64(make_pair(compoundId1().hash(),
                                     compoundId2().hash()));
    }
    //else if (type == CUSTOM)
    //    return controlFn(CF_HASH, data);
    else throw ML::Exception("unknown Id type");
}

void
Id::
complexDestroy()
{
    if (type < STR) return;
    if (type == STR) {
        if (ownstr) {
            int before = str->ref.fetch_add(-1, std::memory_order_relaxed);

            if (before == 1) {
                delete[] (char *)str;
            }
        }
        str = 0;
        ownstr = false;
    }
    else if (type == COMPOUND2) {
        delete cmp1;
        delete cmp2;
        cmp1 = cmp2 = 0;
    }
    //else if (type == CUSTOM)
    //    controlFn(CF_DESTROY, data);
    else throw ML::Exception("unknown Id type");
}

void
Id::
complexFinishCopy()
{
    if (type == STR) {
        if (!ownstr) return;
        str->ref.fetch_add(1, std::memory_order_relaxed);
    }
    else if (type == COMPOUND2) {
        //cerr << "cmp1 = " << cmp1 << " cmp2 = " << cmp2 << " type = "
        //     << (int)type << endl;
        cmp1 = new Id(compoundId1());
        cmp2 = new Id(compoundId2());
    }
    //else if (type == CUSTOM)
    //    data = (void *)controlFn(CF_COPY, data);
    else throw ML::Exception("unknown Id type");
}

Json::Value
Id::
toJson() const
{
    if (notNull())
        return toString();
    else return Json::Value();
}

Id
Id::
fromJson(const Json::Value & val)
{
    if (val.isInt())
        return Id(val.asInt());

    else if (val.isUInt())
        return Id(val.asUInt());

    else if (val.isNull())
        return Id();

    else return Id(val.asString());
}

bool
Id::
stringEqual(const std::string & other) const
{
    if (type == STR) {
        if (len != other.length())
            return false;
        return std::equal(str->data, str->data + len, other.c_str());
    } else if (type == SHORTSTR) {
        size_t l = strnlen(shortStr, 16);
        return l == other.length() && strncmp(other.c_str(), shortStr, l) == 0;
    }

    return toString() == other;
}

bool
Id::
stringEqual(const char * other) const
{
    size_t otherLen = strlen(other);
    if (type == STR) {
        if (len != otherLen)
            return false;
        return std::equal(str->data, str->data + len, other);
    } else if (type == SHORTSTR) {
        size_t l1 = strnlen(shortStr, 16);
        size_t l2 = strlen(other);
        return l1 == l2 && strncmp(other, shortStr, l1) == 0;
    }
    
    return strcmp(toString().c_str(), other) == 0;
}

bool
Id::
stringLess(const std::string & other) const
{
    if (type == STR) {
        return std::lexicographical_compare(str->data, str->data + len,
                                            other.c_str(), other.c_str() + other.length());
    } else if (type == SHORTSTR) {
        size_t l = strnlen(shortStr, 16);
        return l == other.length() && strncmp(other.c_str(), shortStr, l) == 0;
    }
    
    return toString() < other;
}

bool
Id::
stringGreaterEqual(const std::string & other) const
{
    return toString() >= other;
}

const char *
Id::
stringData() const
{
    if (type == STR)
        return str->data;
    else if (type == SHORTSTR)
        return shortStr;
    else throw ML::Exception("stringData() on non-string Id");
}

std::ostream & operator << (std::ostream & stream, const Id & id)
{
    return stream << id.toString();
}

std::istream & operator >> (std::istream & stream, Id & id)
{
    std::string s;
    stream >> s;
    id.parse(s);
    return stream;
}


/*****************************************************************************/
/* VALUE DESCRIPTIONS                                                        */
/*****************************************************************************/

struct IdDescription 
    : public ValueDescriptionI<Datacratic::Id, ValueKind::ATOM, IdDescription> {

    virtual void parseJsonTyped(Datacratic::Id * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(const Datacratic::Id * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(const Datacratic::Id * val) const;
};

//extern template class ValueDescriptionT<Datacratic::Id>;
//extern template class ValueDescriptionI<Datacratic::Id, ValueKind::ATOM, IdDescription>;

struct StringIdDescription: public IdDescription {

    virtual void printJsonTyped(const Datacratic::Id * val,
                                JsonPrintingContext & context) const;
};


DEFINE_VALUE_DESCRIPTION(Id, IdDescription);

template<typename Context>
void parseJson(Id * output, Context & context)
{
    using namespace std;

    if (context.isString()) {
        char buffer[4096];
        ssize_t realSize = context.expectStringUtf8(buffer, sizeof(buffer));
        if (realSize > -1) {
            *output = Id(buffer, realSize);
        }
        else {
            Utf8String value = context.expectStringUtf8();
            *output = Id(value);
        }
        return;
    }

    unsigned long long i;
    if (context.matchUnsignedLongLong(i)) {
        // cerr << "got unsigned " << i << endl;
        *output = Id(i);
        return;
    }

    signed long long l;
    if (context.matchLongLong(l)) {
        // cerr << "got signed " << l << endl;
        *output = Id(l);
        return;
    }

    double d;
    if (context.matchDouble(d)) {
        if ((long long)d != d)
            context.exception("IDs must be integers");
        *output = Id((long long)d);
        return;
    }

    if (context.isNull()) {
        context.expectNull();
        *output = Id();
        output->type = Id::NULLID;
        return;
    }

    std::cerr << context.expectJson() << endl;

    throw ML::Exception("unhandled id conversion type");
}

void
IdDescription::
parseJsonTyped(Datacratic::Id * val,
               JsonParsingContext & context) const
{
    Datacratic::parseJson(val, context);
}

void
IdDescription::
printJsonTyped(const Datacratic::Id * val,
               JsonPrintingContext & context) const
{
    if (val->type == Id::Type::BIGDEC &&
        val->val2 == 0 && val->val1 <= std::numeric_limits<int32_t>::max()) {
        context.writeInt(val->val1);
    } else {
        context.writeStringUtf8(Utf8String(val->toString()));
    }
}

bool
IdDescription::
isDefaultTyped(const Datacratic::Id * val) const
{
    return !val->notNull();
}

template class ValueDescriptionI<Datacratic::Id, ValueKind::ATOM, IdDescription>;

void
StringIdDescription::
printJsonTyped(const Datacratic::Id * val,
               JsonPrintingContext & context) const
{
    context.writeStringUtf8(val->toUtf8String());
}

} // namespace Datacratic

/** cell_value.h                                                   -*- C++ -*-
    Jeremy Barnes, 24 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <atomic>
#include "mldb/types/hash_wrapper.h"
#include "mldb/types/date.h"
#include "mldb/types/value_description_fwd.h"




struct JsonPrintingContext;

namespace MLDB {


typedef HashWrapper<4> CellValueHash;
struct PathElement;
struct Path;

/*****************************************************************************/
/* STRING CHARACTERISTICS                                                    */
/*****************************************************************************/

/** Enumeration that tells us the characteristics of a string that's
    represented as a const char * and a length.
*/

enum StringCharacteristics {
    STRING_UNKNOWN,           ///< Unknown ASCII or UTF8 and validity
    STRING_IS_VALID_ASCII,    ///< Known valid ASCII string, don't check
    STRING_IS_VALID_UTF8_NOT_ASCII ///< Valid UTF-8 with at least one non-ascii char
};

/*****************************************************************************/
/* CELL VALUE                                                                */
/*****************************************************************************/

/** Represents a value of a cell in the 3D sparse matrix. */

struct CellValue {

    MLDB_ALWAYS_INLINE CellValue() noexcept
        : bits1(0), bits2(0), flags(0)
    {
    }
    //CellValue(bool boolValue)
    //{
    //    initInt(boolValue);
    //}

    CellValue(char intValue) noexcept { initInt(intValue); }
    CellValue(unsigned char intValue) noexcept { initInt(intValue); }
    CellValue(signed char intValue) noexcept { initInt(intValue); }
    CellValue(int intValue) noexcept { initInt(intValue); }
    CellValue(unsigned int intValue) noexcept { initInt(intValue); }
    CellValue(short int intValue) noexcept { initInt(intValue); }
    CellValue(unsigned short int intValue) noexcept { initInt(intValue); }
    CellValue(long int intValue) noexcept { initInt(intValue); }
    CellValue(unsigned long int intValue) noexcept;
    CellValue(long long int intValue) noexcept { initInt(intValue); }
    CellValue(unsigned long long int intValue) noexcept;

    CellValue(double floatValue) noexcept
        : floatVal(floatValue), type(ST_FLOAT), unused1(0)
    {
        bits2 = 0;
        collapseFloatToInt();
    }

    CellValue(float floatValue) noexcept
        : floatVal(floatValue), type(ST_FLOAT), unused1(0)
    {
        bits2 = 0;
        collapseFloatToInt();
    }

    CellValue(const std::string & stringValue);
    CellValue(const Utf8String & stringValue);
    CellValue(const char * stringValue);

    /** Create from a string that doesn't require any parsing.

        The characteristics tell us what we already know about
        encoding and validity.
    */
    CellValue(const char * stringValue, size_t length,
              StringCharacteristics characteristics = STRING_UNKNOWN);
    CellValue(Date timestampstatic) noexcept;
    
    CellValue(const CellValue & other);
    MLDB_ALWAYS_INLINE CellValue(CellValue && other) noexcept
        : bits1(other.bits1), bits2(other.bits2), flags(other.flags)
    {
        other.type = ST_EMPTY;
    }

    /** Construct from a path. */
    CellValue(const Path & path);
    
    /** Construct an interval CellValue.  Note that months and days have
        a limit of 16 bits, and that only one of months, days and seconds
        can have a negative sign (if all are positive, it's a positive
        interval;  if one is negative it's a negative interval; if more than
        one is negative it's an error).

        This will throw if the values are out of range of the 16 bits, and
        perform all of the necessary normalization into the internal format.
    */
    static CellValue fromMonthDaySecond( int64_t months, int64_t days, double seconds);

    /** Construct a blob CellValue from the given string, which may be
        stolen to reduce memory use.
    */
    static CellValue blob(std::string blobContents);

    /** Construct a blob CellValue from the memory range, which will be
        copied in.
    */
    static CellValue blob(const char * p, size_t size);

    CellValue & operator = (const CellValue & other)
    {
        CellValue newMe(other);
        swap(newMe);
        return *this;
    }

    MLDB_ALWAYS_INLINE CellValue & operator = (CellValue && other) noexcept
    {
        CellValue newMe(std::move(other));
        swap(newMe);
        return *this;

    }
    
    MLDB_ALWAYS_INLINE void swap(CellValue & other) noexcept
    {
        auto t1 = flags;  flags = other.flags;  other.flags = t1;
        auto t2 = bits1;  bits1 = other.bits1;  other.bits1 = t2;
        auto t3 = bits2;  bits2 = other.bits2;  other.bits2 = t3;
    }
    
    MLDB_ALWAYS_INLINE ~CellValue()
    {
        if (type == ST_ASCII_LONG_STRING || type == ST_UTF8_LONG_STRING
            || type == ST_LONG_BLOB)
            deleteString();
    }

    static CellValue parse(const std::string & str);
    static CellValue parse(const Utf8String & str);
    static CellValue parse(const char * start, size_t len,
                           StringCharacteristics characteristics);

    bool empty() const
    {
        return type == EMPTY;
    }

    std::string toString() const;
    Utf8String toUtf8String() const;
    std::basic_string<char32_t> toWideString() const;
    bool hasNonAsciiChar(const char *start, unsigned int len) const;
    void checkForNonAsciiChar(const char *start, unsigned int len) const;

    /** Pointer to the immutable static storage for the string.  Has the
        same lifetime as the CellValue object.
    */
    const char * stringChars() const;

    /** Length of the result of the toString() function */
    uint32_t toStringLength() const;

    /** Pointer to the immutable static storage for the blob.  Has the
        same lifetime as the CellValue object.
    */
    const unsigned char * blobData() const;

    /** Length of the data stored for the blob. */
    uint32_t blobLength() const;

    /** Is it exactly representable as a 32 bit signed integer? */
    bool isExactInt32() const;

    /** Is it exactly representable as a 32 bit unsigned integer? */
    bool isExactUint32() const;

    /** Is it exactly representable as a double? */
    bool isExactDouble() const;

    double isDouble() const
    {
        return type == ST_FLOAT;
    }
    
    double toDouble() const
    {
        if (MLDB_LIKELY(type == ST_FLOAT))
            return floatVal;
        return toDoubleImpl();
    }

    int64_t toInt() const;
    uint64_t toUInt() const;

    bool isNumber() const;

    /** Is this a number that's positive? */
    bool isPositiveNumber() const;

    /** Is this a number that's negative? */
    bool isNegativeNumber() const;

    /** Is it a timestamp? */
    bool isTimestamp() const
    {
        return type == ST_TIMESTAMP;
    }

    /** Is it an interval? */
    bool isTimeinterval() const
    {
        return type == ST_TIMEINTERVAL;
    }

    /** Return the timestamp */
    Date toTimestamp() const;

    /** Return the time interval.  Returns integral months, integral days
        and integral seconds in a tuple.
    */
    std::tuple<int64_t, int64_t, double> toMonthDaySecond() const;

    bool asBool() const;

    bool isTrue() const { return asBool(); }

    bool isFalse() const;

    /// Broad categorization of types, independent of storage
    enum CellType {
        EMPTY,
        INTEGER,
        FLOAT,
        ASCII_STRING,
        UTF8_STRING,
        TIMESTAMP,
        TIMEINTERVAL,
        BLOB,
        PATH,
        NUM_CELL_TYPES
    };

    CellType cellType() const;

    bool isNumeric() const
    {
        CellType t = cellType();
        return (t == INTEGER || t == FLOAT);
    }

    bool isString() const
    {
        CellType t = cellType();
        return t == ASCII_STRING || t == UTF8_STRING;
    }

    bool isAsciiString() const
    {
        CellType t = cellType();
        return t == ASCII_STRING;
    }

    bool isUtf8String() const
    {
        CellType t = cellType();
        return t == UTF8_STRING;
    }

    bool isInteger() const
    {
        return cellType() == INTEGER;
    }

    bool isUnsignedInteger() const
    {
        return type == ST_UNSIGNED;
    }

    bool isInt64() const;
    bool isUInt64() const;

    bool isBlob() const
    {
        return cellType() == BLOB;
    }

    bool isPath() const
    {
        return cellType() == PATH;
    }

    bool isNaN() const
    {
        CellType t = cellType();
        if (t == INTEGER) {
            return std::isnan(toInt());
        }
        else if (t == FLOAT) {
            return std::isnan(toDouble());
        }
        else {
            return false;
        }
    }

    bool isInf() const
    {
        CellType t = cellType();
        if (t == INTEGER) {
            return std::isinf(toInt());
        }
        else if (t == FLOAT) {
            return std::isinf(toDouble());
        }
        else {
            return false;
        }
    }

    CellValue coerceToInteger() const;
    CellValue coerceToNumber() const;
    CellValue coerceToString() const;
    CellValue coerceToBoolean() const;
    CellValue coerceToTimestamp() const;
    Date mustCoerceToTimestamp() const;
    CellValue coerceToBlob() const;
    PathElement coerceToPathElement() const;
    Path coerceToPath() const;
    
    /** This is always the SIPhash of the toString() representation.
        Only for blobs, which have no toString(), is it calculated
        on the raw.
    */
    CellValueHash hash() const;

    operator CellValueHash() const
    {
        return this->hash();
    }

    int compare(const CellValue & other) const;

    bool operator == (const CellValue & other) const;
    bool operator != (const CellValue & other) const
    {
        return ! operator == (other);
    }
    bool operator <  (const CellValue & other) const;

    bool operator <= (const CellValue & other) const
    {
        return ! other.operator < (*this);
    }
    
    bool operator >  (const CellValue & other) const
    {
        return other.operator < (*this);
    }
    
    bool operator >= (const CellValue & other) const
    {
        return !operator < (other);
    }

    /** Print the value to the given JSON context.  This allows for
        JSON to be extracted without going through a Json::Value
        object.  This version provides full fidelity in type conversion,
        but the JSON returned is less natural.
    */
    void extractStructuredJson(JsonPrintingContext & context) const;

    /** Print the value to the given JSON context.  This allows for
        JSON to be extracted without going through a Json::Value
        object.  This version provides a simplified JSON representation,
        for example converting timestamps into strings, and as a result
        will lose fidelity in the output for things that don't have a
        natural JSON representation.
    */
    void extractSimplifiedJson(JsonPrintingContext & context) const;

    size_t memusage() const;

private:
    double toDoubleImpl() const;
    
    void initInt(int64_t intValue) noexcept
    {
        type = ST_INTEGER;
        bits2 = 0;
        unused1 = 0;
        intVal = intValue;
    }

    void collapseFloatToInt()
    {
        // See if we can reduce the float to an integer
        int64_t asInt = floatVal;
        if (asInt == floatVal) {
            // Store as an integer
            intVal = asInt;
            type = ST_INTEGER;
        }
    }

    /** Initialize string from a source which is expected to be
        valid ASCII (it will be checked). */
    void initStringFromAscii(const char * val, size_t len,
                             bool checkValidity);

    /** Initialize string from a source which is expected to be
        valid UTF8 (it will be checked). */
    void initStringFromUtf8(const char * val, size_t len,
                            bool checkValidity);
    
    /** Implementation of the two initStringFromxxx methods.
        It checks the validity.*/
    void initString(const char * val, size_t len,
                    bool isUtf8, bool checkValidity);

    /** Initialize a blob. */
    void initBlob(const char * data, size_t len);

    void deleteString();

    std::string printInterval() const;

    Utf8String trimmedExceptionString() const;

    enum StorageType {
        ST_EMPTY,
        ST_INTEGER,
        ST_UNSIGNED,
        ST_FLOAT,
        ST_ASCII_SHORT_STRING,
        ST_ASCII_LONG_STRING,
        ST_UTF8_SHORT_STRING,
        ST_UTF8_LONG_STRING,
        ST_UNOWNED_STRING,
        ST_TIMESTAMP,
        ST_TIMEINTERVAL,
        ST_SHORT_BLOB,
        ST_LONG_BLOB,
        ST_SHORT_PATH,
        ST_LONG_PATH
    };

    struct StringRepr {
        StringRepr() noexcept
            : hash(0), ref(0)
        {
        }

        std::atomic<uint64_t> hash;
        std::atomic<int> ref;
        char repr[0];
    };

    struct TimeIntervalRepr {
        uint16_t months;
        uint16_t days;
        double   seconds;
    } __attribute__((__packed__));
    
    /// How many bytes to provide for internal strings
    static constexpr size_t INTERNAL_LENGTH = 12;

    union {
        struct { uint64_t bits1; uint32_t bits2; } __attribute__((__packed__));
        double floatVal;
        int64_t intVal;
        uint64_t uintVal;
        char shortString[INTERNAL_LENGTH];
        StringRepr * longString;
        double timestamp;
        TimeIntervalRepr timeInterval;
    } __attribute__((__packed__));

    union {
        uint32_t flags;
        struct {
            uint32_t type:4;
            uint32_t unused1:28;
        };
        struct {
            uint32_t strType:4;
            uint32_t strFlags:4;
            uint32_t strLength:24;
        };
        struct {
            uint32_t floatType:4;
            uint32_t floatPrecision:8;
            uint32_t floatFormat:2;
        };
    };

    static bool isStringType(StorageType type);
    static bool isBlobType(StorageType type);
    static bool isPathType(StorageType type);
} __attribute__((__packed__)) ;

inline void swap(CellValue & val1, CellValue & val2)
{
    return val1.swap(val2);
}

PREDECLARE_VALUE_DESCRIPTION(CellValue);
DECLARE_ENUM_DESCRIPTION_NAMED(CellTypeDescription, CellValue::CellType);

std::string to_string(const CellValue & cell);

std::string keyToString(const CellValue & cell);

CellValue stringToKey(const std::string & str, CellValue *);

std::ostream & operator << (std::ostream & stream, const CellValue & val);

std::string to_string(const CellValue::CellType & type);

std::string keyToString(const CellValue::CellType & cell);

CellValue::CellType stringToKey(const std::string & str, CellValue::CellType *);

std::ostream & operator << (std::ostream & stream, const CellValue::CellType & val);

} // namespace MLDB


// Allow std::unordered_xxx<CellValue> to work
namespace std {

template<typename T> struct hash;

template<>
struct hash<MLDB::CellValue> : public std::unary_function<MLDB::CellValue, size_t>
{
    size_t operator()(const MLDB::CellValue & val) const { return val.hash(); }
};

} // namespace std

/** cell_value_impl.h                                              -*- C++ -*-
    Jeremy Barnes, 24 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/types/date.h"
#include "mldb/types/value_description_fwd.h"



namespace MLDB {


typedef HashWrapper<4> CellValueHash;


/*****************************************************************************/
/* CELL VALUE                                                                */
/*****************************************************************************/

#if 0
CellValue::
CellValue() noexcept
    : bits1(0), bits2(0), flags(0)
{

}
#endif

CellValue::
CellValue(unsigned long long intValue) noexcept
    : intVal(intValue), type(ST_INTEGER), unused1(0)
{
    bits2 = 0;
    if (intValue >= (1ULL << 63))
        type = ST_UNSIGNED;
}

CellValue::
CellValue(unsigned long intValue) noexcept
    : intVal(intValue), type(ST_INTEGER), unused1(0)
{
    bits2 = 0;
    if (intValue >= (1ULL << 63))
        type = ST_UNSIGNED;
}

bool 
CellValue::
hasNonAsciiChar(const char *start, unsigned int len) const
{
    for(unsigned i = 0 ; i < len;++i)
    {
        if ((unsigned int)start[i] > 127)
            return true;
    }
    return false;
}

void
CellValue::
checkForNonAsciiChar(const char *start, unsigned int len) const
{
    if(hasNonAsciiChar(start, len))
    {
        throw HttpReturnException(400, "non-ascii character found in string");
    }
}

CellValue::
CellValue(const std::string & stringValue)
    : bits1(0), bits2(0), flags(0)
{
    initStringFromAscii(stringValue.c_str(), stringValue.length(),
                        true /* check validity */);
}

CellValue::
CellValue(const char * stringValue)
    : bits1(0), bits2(0), flags(0)
{
    initStringFromAscii(stringValue, strlen(stringValue),
                        true /* check validity */);
}

CellValue::
CellValue(const char * stringValue, size_t length,
          StringCharacteristics characteristics)
    : bits1(0), bits2(0), flags(0)
{
    switch (characteristics) {
    case STRING_UNKNOWN:
        initStringFromUtf8(stringValue, length, true /* check validity */);
        break;
    case STRING_IS_VALID_UTF8_NOT_ASCII:
        initStringFromUtf8(stringValue, length, false /* check validity */);
        break;
    case STRING_IS_VALID_ASCII:
        initStringFromAscii(stringValue, length, false /* check validity */);
        break;
    default:
        throw MLDB::Exception("Unknown string characteristic");
    }
}

CellValue::
CellValue(const Utf8String & stringValue)
    : bits1(0), bits2(0), flags(0)
{
    initStringFromUtf8(stringValue.rawData(), stringValue.rawLength(),
                       false /* check validity */);
}

CellValue::
CellValue(Date timestamp) noexcept
    : timestamp(timestamp.secondsSinceEpoch()), type(ST_TIMESTAMP)
{
    bits2 = 0;
}

bool
CellValue::
isStringType(StorageType type)
{
    return type == ST_ASCII_SHORT_STRING || type == ST_ASCII_LONG_STRING || type == ST_UTF8_SHORT_STRING || type == ST_UTF8_LONG_STRING;
}

bool
CellValue::
isBlobType(StorageType type)
{
    return type == ST_SHORT_BLOB || type == ST_LONG_BLOB;
}

bool
CellValue::
isPathType(StorageType type)
{
    return type == ST_SHORT_PATH || type == ST_LONG_PATH;
}

} // namespace MLDB


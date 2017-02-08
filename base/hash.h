/* hash.h                                                           -*- C++ -*-
   Jeremy Barnes, 25 October 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Md5 hash functions.
*/

#pragma once

#include <string>

namespace MLDB {

std::string base64Encode(const std::string & str);
std::string base64Encode(const char * buf, size_t nBytes);

std::string base64Decode(const std::string & str);
std::string base64Decode(const char * buf, size_t nBytes);

std::string md5HashToHex(const std::string & str);
std::string md5HashToHex(const char * buf, size_t nBytes);

std::string md5HashToBase64(const std::string & str);
std::string md5HashToBase64(const char * buf, size_t nBytes);

std::string hmacSha1Base64(const std::string & stringToSign,
                           const std::string & privateKey);
std::string hmacSha256Base64(const std::string & stringToSign,
                             const std::string & privateKey);

} // namespace ML

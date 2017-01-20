/* md5.cc
   Jeremy Barnes, 25 October 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "hash.h"
#include "mldb/arch/format.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
#define CRYPTOPP_ENABLE_NAMESPACE_WEAK 1
#include "crypto++/sha.h"
#include "crypto++/md5.h"
#include "crypto++/hmac.h"
#include "crypto++/base64.h"
#include "crypto++/filters.h"
#pragma GCC diagnostic pop

using namespace std;

namespace MLDB {

std::string base64Encode(const std::string & str)
{
    return base64Encode(str.c_str(), str.length());
}

std::string base64Encode(const char * buf, size_t nBytes)
{
    std::string result;
    result.reserve(nBytes * 8 / 6 + 4);
    CryptoPP::Base64Encoder baseEncoder(nullptr, false /* insert line breaks */);
    baseEncoder.Attach(new CryptoPP::StringSink(result));
    baseEncoder.Put((const byte *)buf, nBytes);
    baseEncoder.MessageEnd();

    return result;
}

std::string base64Decode(const std::string & str)
{
    return base64Decode(str.c_str(), str.length());
}

std::string base64Decode(const char * buf, size_t nBytes)
{
    std::string result;
    result.reserve(nBytes * 6 / 8);
    CryptoPP::Base64Decoder baseDecoder;
    baseDecoder.Attach(new CryptoPP::StringSink(result));
    baseDecoder.Put((const byte *)buf, nBytes);
    baseDecoder.MessageEnd();

    return result;
}

std::string md5HashToHex(const std::string & str)
{
    return md5HashToHex(str.c_str(), str.length());
}

std::string md5HashToHex(const char * buf, size_t nBytes)
{
    typedef CryptoPP::Weak::MD5 Hash;
    size_t digestLen = Hash::DIGESTSIZE;
    byte digest[digestLen];
    Hash hash;
    hash.CalculateDigest(digest, (byte *)buf, nBytes);

    string md5;
    for (unsigned i = 0;  i < digestLen;  ++i) {
        md5 += MLDB::format("%02x", digest[i]);
    }

    return md5;
}

std::string md5HashToBase64(const std::string & str)
{
    return md5HashToBase64(str.c_str(), str.length());
}

std::string md5HashToBase64(const char * buf, size_t nBytes)
{
    typedef CryptoPP::Weak::MD5 Hash;
    size_t digestLen = Hash::DIGESTSIZE;
    byte digest[digestLen];
    Hash hash;
    hash.CalculateDigest(digest, (byte *)buf, nBytes);

    // base64
    char outBuf[256];

    CryptoPP::Base64Encoder baseEncoder(nullptr, false /* add line endings */);
    baseEncoder.Put(digest, digestLen);
    baseEncoder.MessageEnd();
    size_t got = baseEncoder.Get((byte *)outBuf, 256);
    outBuf[got] = 0;

    //cerr << "got " << got << " characters" << endl;

    return std::string(outBuf);
}

std::string hmacSha1Base64(const std::string & stringToSign,
                           const std::string & privateKey)
{
    typedef CryptoPP::SHA1 Hash;
    
    size_t digestLen = Hash::DIGESTSIZE;
    byte digest[digestLen];
    CryptoPP::HMAC<Hash> hmac((byte *)privateKey.c_str(), privateKey.length());
    hmac.CalculateDigest(digest,
                         (byte *)stringToSign.c_str(),
                         stringToSign.length());

    // base64
    char outBuf[256];

    CryptoPP::Base64Encoder baseEncoder(nullptr, false /* add line endings */);
    baseEncoder.Put(digest, digestLen);
    baseEncoder.MessageEnd();
    size_t got = baseEncoder.Get((byte *)outBuf, 256);
    outBuf[got] = 0;
    string base64digest(outBuf, outBuf + got - 1);
        return base64digest;
}

std::string hmacSha256Base64(const std::string & stringToSign,
                             const std::string & privateKey)
{
    typedef CryptoPP::SHA256 Hash;
    
    size_t digestLen = Hash::DIGESTSIZE;
    byte digest[digestLen];
    CryptoPP::HMAC<Hash> hmac((byte *)privateKey.c_str(), privateKey.length());
    hmac.CalculateDigest(digest,
                         (byte *)stringToSign.c_str(),
                         stringToSign.length());

    // base64
    char outBuf[256];

    CryptoPP::Base64Encoder baseEncoder(nullptr, false /* add line endings */);
    baseEncoder.Put(digest, digestLen);
    baseEncoder.MessageEnd();
    size_t got = baseEncoder.Get((byte *)outBuf, 256);
    outBuf[got] = 0;
    string base64digest(outBuf, outBuf + got - 1);
        return base64digest;
}

} // namespace MLDB

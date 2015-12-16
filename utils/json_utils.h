/** json_utils.h                                                   -*- C++ -*-
    Jeremy Barnes, 10 November 2013
    Copyright (c) 2013 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#pragma once

#include "mldb/ext/jsoncpp/json.h"

namespace Datacratic {

/** Print an abbreviated version of the given value, with the
    information that takes lots of spaced summarized.
*/
std::string jsonPrintAbbreviated(const Json::Value & val,
                                 int maxCharsPerItem = 100,
                                 int maxCharsOverall = -1);

typedef __int128_t int128_t;

/** Type used for the seed of a hash. */
union HashSeed {
    char b[16];
    int64_t i64[2];
    __int128_t i128;
};

/** Hash seed used by default for hashes that need to be stable over
    invocations.  Note that if the default seed is known, then it
    is not possible to protect against hash collisions.
*/
extern const HashSeed defaultSeedStable;

/** Create a hash from the given JSON value, with the given hash seed.
    This uses the SIP hash, which is collision resistant and can
    prevent against DOS attacks.

    You should use the defaultSeedStable if you need the hash to be
    repeatable over invocations.  Otherwise, you should set the
    seed to something that's not easily guessable in order to protect
    against hash collision attacks.
*/
uint64_t jsonHash(const Json::Value & val,
                  HashSeed seed = defaultSeedStable);

/** Flatten each of the JSON arguments, which should be arrays, into
    a single array containing all of the values.
*/
Json::Value flatten(const std::vector<Json::Value> & args);

/** A somewhat arbitrary ordering of JSON values.  If they aren't
    the same type then it will throw.
*/
Json::Value jsonMin(const Json::Value & v1,
                    const Json::Value & v2);

/** A somewhat arbitrary ordering of JSON values.  If they aren't
    the same type then it will throw.
*/
Json::Value jsonMax(const Json::Value & v1,
                    const Json::Value & v2);

/** Return the maximum value of each of the given JSON values. */
Json::Value jsonMaxVector(const std::vector<Json::Value> & args);

/** Return the minimum value of each of the given JSON values. */
Json::Value jsonMinVector(const std::vector<Json::Value> & args);


} // namespace Datacratic


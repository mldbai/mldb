/* content_descriptor.h                                            -*- C++ -*-
   Jeremy Barnes, 1 September 2018
   Copyright (c) 2018 Mldb.ai Inc.  All rights reserved.

   Structures to allow content to be obtained from a descriptor rather than
   simply by a URL.  Allows a much richer subsystem then simply "give me
   what is at this URL".
*/

#pragma once

#include <vector>
#include <map>
#include "mldb/types/url.h"
#include "mldb/types/string.h"
#include "mldb/types/any.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/block/memory_region.h"

namespace MLDB {

struct filter_istream;
struct Url;


/*****************************************************************************/
/* CONTENT HASH                                                              */
/*****************************************************************************/

struct ContentHash {
    Utf8String type;   ///< Type of the content hash; defines how it is used
    Utf8String value;  ///< Value of the content hash.  Typically a hex string
};

DECLARE_STRUCTURE_DESCRIPTION(ContentHash);


/*****************************************************************************/
/* CONTENT HASHES                                                            */
/*****************************************************************************/

struct ContentHashes: public std::vector<ContentHash> {
};

PREDECLARE_VALUE_DESCRIPTION(ContentHashes);


/*****************************************************************************/
/* CONTENT DESCRIPTOR                                                        */
/*****************************************************************************/

struct ContentDescriptor {
    ContentHashes content;

    Url getUrl() const;
    Utf8String getUrlString() const;
    std::string getUrlStringUtf8() const;
};

PREDECLARE_VALUE_DESCRIPTION(ContentDescriptor);


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

filter_istream getContentStream(const ContentDescriptor & descriptor,
                                const std::map<Utf8String, Any> & options
                                    = std::map<Utf8String, Any>());


} // namespace MLDB


/* filter_streams_registry.h                                       -*- C++ -*-
   Jeremy Barnes, 12 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   This file is part of "Jeremy's Machine Learning Library", copyright (c)
   1999-2015 Jeremy Barnes.
   
   Apache 2.0 license.
*/

#pragma once

#include "filter_streams.h"
#include <functional>
#include <string>
#include <memory>
#include <map>


namespace MLDB {

/*****************************************************************************/
/* REGISTRY                                                                  */
/*****************************************************************************/

/* The type of a function a uri handler invokes when an exception is thrown at
 * closing time. This serves as a workaround of the silent catching that
 * boost::iostreams::stream_buffer performs when a streambuf is being
 * destroyed. To be effective, it requires that "close" be called on all
 * streams before destruction. */
typedef std::function<void (const std::exception_ptr & excPtr)> OnUriHandlerException;

typedef std::function<UriHandler
                      (const std::string & scheme,
                       const Utf8String & resource,
                       std::ios_base::openmode mode,
                       const std::map<std::string, std::string> & options,
                       const OnUriHandlerException & onException)>
UriHandlerFactory;

void registerUriHandler(const std::string & scheme,
                        const UriHandlerFactory & handler);

std::string & getMemStreamString(const Utf8String & name);
void setMemStreamString(const Utf8String & name,
                        const std::string & contents);
void deleteMemStreamString(const Utf8String & name);
void deleteAllMemStreamStrings();

} // namesapce MLDB

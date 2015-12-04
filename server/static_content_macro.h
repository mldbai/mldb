// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** static_content_macro.h                                         -*- C++ -*-
    Jeremy Barnes, 23 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Macros for static content handling.
*/

#pragma once

#include "mldb/types/string.h"
#include <functional>
#include <memory>


struct hoedown_buffer;
struct hoedown_renderer_data;

namespace Datacratic {
namespace MLDB {


struct MldbServer;


/*****************************************************************************/
/* MACRO DATA                                                                */
/*****************************************************************************/

struct MacroData {
    std::string dir;            ///< Base directory for serving
    bool hideInternalEntities;  ///< Are internal entities hidden from doc?
    MldbServer * server;        ///< MLDB server we're running inside of
};


/*****************************************************************************/
/* MACRO CONTEXT                                                             */
/*****************************************************************************/

struct MacroContext: public MacroData {
    MacroContext(const MacroData * macroData,
                 hoedown_buffer * output,
                 const hoedown_buffer * text);

    const MacroData * macroData;
    hoedown_buffer * output;
    const hoedown_buffer * text;

    void writeHtml(const Utf8String & text);
    void writeText(const Utf8String & text);
    void writeInternalLink(Utf8String url,
                           const Utf8String & anchorText,
                           bool followInternalRedirect);
};

typedef std::function<void (MacroContext & context,
                            const std::string & macroName,
                            const Utf8String & args)>
MldbMacro;

std::shared_ptr<void>
registerMacro(const std::string & macroName,
              MldbMacro macro,
              bool failOnError = true);

void callMacro(MacroContext & context,
               const std::string & macroName,
               const Utf8String & args);


struct RegisterMacro {
    RegisterMacro(const std::string & macroName,
                  MldbMacro macro,
                  bool failOnError = true);
    std::shared_ptr<void> handle;
};               


} // namespace MLDB
} // namespace Datacratic

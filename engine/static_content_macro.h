/** static_content_macro.h                                         -*- C++ -*-
    Jeremy Barnes, 23 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Macros for static content handling.  These allow for a plugin to extend
    the documentation system to make calls into MLDB.
*/

#pragma once

#include "mldb/types/string.h"
#include <functional>
#include <memory>


struct hoedown_buffer;
struct hoedown_renderer_data;


namespace MLDB {


struct MldbEngine;


/*****************************************************************************/
/* MACRO DATA                                                                */
/*****************************************************************************/

struct MacroData {
    std::string dir;            ///< Base directory for serving
    bool hideInternalEntities;  ///< Are internal entities hidden from doc?
    MldbEngine * engine = nullptr; ///< MLDB server we're running inside of
};

Utf8String renderMarkdown(const Utf8String & str, const MacroData & macroData);

Utf8String renderMarkdown(const char * buf /* utf8 */, size_t len,
                          const MacroData & macroData);
    
// Render a full HTML page from the given body content
Utf8String renderHtmlPage(MldbEngine * engine, const Utf8String & htmlBody);


/*****************************************************************************/
/* MACRO CONTEXT                                                             */
/*****************************************************************************/

struct MacroContext: public MacroData {
    MacroContext(const MacroData * macroData,
                 hoedown_buffer * output,
                 const hoedown_buffer * text);

    const MacroData * macroData = nullptr;
    hoedown_buffer * output = nullptr;
    const hoedown_buffer * text = nullptr;  // may stay null after init

    void writeHtml(const Utf8String & text);
    void writeText(const Utf8String & text);
    void writeMarkdown(const Utf8String & markdown);
    void writeInternalLink(Utf8String url,
                           const Utf8String & anchorText,
                           bool followInternalRedirect);
    Utf8String prefixUrl(Utf8String url) const;

protected:
    MacroContext() = default;
    void init(const MacroData * macroData,
              hoedown_buffer * output,
              const hoedown_buffer * text = nullptr);
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


/*****************************************************************************/
/* STANDALONE MACRO CONTEXT                                                  */
/*****************************************************************************/

/** Macro context that manages its own render buffer to enable markdown to be
    easily rendered into HTML.
*/

struct StandaloneMacroContext: public MacroContext {
    StandaloneMacroContext(MldbEngine * engine,
                           bool hideInternalEntities = true);
    ~StandaloneMacroContext();

    // Return just the content as HTML
    Utf8String getHtml() const;

    // Return an entire HTML page including headers, body, content and footers
    Utf8String getHtmlPage() const;
    
private:
    struct Itl;
    std::unique_ptr<Itl> itl;
};


} // namespace MLDB


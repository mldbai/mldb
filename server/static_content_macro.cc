// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** static_content_macro.cc
    Jeremy Barnes, 23 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "static_content_macro.h"

#include "mldb/ext/hoedown/src/buffer.h"
#include "mldb/ext/hoedown/src/html.h"
#include "mldb/ext/hoedown/src/document.h"
#include "mldb/ext/hoedown/src/escape.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/mldb_entity.h"
#include "mldb/base/scope.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/jml/utils/string_functions.h"


using namespace std;


namespace MLDB {


/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

// Defined in static_content_handler.cc
std::string renderMarkdown(const std::string & str, const MacroData & macroData);

using namespace Json;

static std::string printTypeName(std::string t)
{
    if (t.find("MLDB::") == 0)
        t = string(t, 6);
    return t;
};

std::string insertAfterFragment(const std::string & uri, const std::string & toInsert )
{
    auto pos = uri.find('#');
    return uri.substr(0, pos) + toInsert + (pos == string::npos ? "" : uri.substr(pos)); 
}

static std::string getTypeName(const ValueDescription & description,
                               MldbServer * server)
{
    std::string resultSoFar;
    std::string closing;
    if (!description.documentationUri.empty()) {
        resultSoFar += "<a href=\"" + server->prefixUrl(insertAfterFragment(description.documentationUri, ".html")) + "\">";
        closing = "</a>";
    }

    auto wrap = [&] (const std::string & s)
        {
            return resultSoFar + s + closing;
        };

    switch (description.kind) {
    case ValueKind::INTEGER:   return wrap("int");
    case ValueKind::FLOAT:     return wrap("float");
    case ValueKind::BOOLEAN:   return wrap("bool");
    case ValueKind::STRING:    return wrap("string");
    case ValueKind::ARRAY:     return wrap("ARRAY [ " + getTypeName(description.contained(), server) + " ]");
    case ValueKind::STRUCTURE: return wrap(printTypeName(printTypeName(description.typeName)));
    case ValueKind::ENUM:      return wrap(printTypeName(description.typeName));
    case ValueKind::ATOM:      return wrap(printTypeName(description.typeName));
    case ValueKind::LINK:      return wrap("LINK "     + printTypeName(description.typeName));
    case ValueKind::TUPLE:     {
        std::string result = "TUPLE [";
        bool first = true;
        for (auto & tp: description.getTupleElementDescriptions()) {
            if (!first)
                result += ",";
            first = false;
            result = result + " " + getTypeName(*tp, server);
        }
        result += " ]";
        return result;
    }
    case ValueKind::OPTIONAL:  return wrap(getTypeName(description.contained(), server)) + " (Optional)";
    case ValueKind::VARIANT:   return wrap("VARIANT "  + printTypeName(description.typeName));
    case ValueKind::MAP: {
        return wrap("MAP { " + getTypeName(description.getKeyValueDescription(), server)
                    + " : " + getTypeName(description.contained(), server) + " }");
    }
    case ValueKind::ANY:       return description.typeName == "Json::Value" ? "JSON" : printTypeName(description.typeName);
    default:
        throw MLDB::Exception("Unknown description kind");
    }
}

static std::string getDefaultValue(const ValueDescription & description)
{
    void * val = description.constructDefault();
    std::ostringstream stream;
    StreamJsonPrintingContext context(stream);
    description.printJson(val, context);
    description.destroy(val);

    if (stream.str() == "null" || stream.str() == "\"\"" || stream.str() == "[]" || stream.str() == "{}")
        return "";

    return stream.str();
}

static void renderType(MacroContext & context,
                       const Utf8String & cppType,
                       bool jsonFragment = false)
{
    const ValueDescription * vd = ValueDescription::get(cppType.rawString()).get();
        
    if (!vd) {
        context.writeHtml("Type with name '" + cppType + "' not found");
        return;
    }

    if (vd->kind == ValueKind::STRUCTURE) {
        if(jsonFragment) {
            bool first = true;
            auto onField = [&] (const ValueDescription::FieldDescription & fd)
                {
                    if(first) {
                        first = false;
                    }
                    else {
                        context.writeHtml(",");
                    }
                    context.writeHtml(
                        MLDB::format("\n        \"%s\": &lt;%s&gt;",
                                   fd.fieldName.c_str(),
                                   getTypeName(*fd.description,
                                               context.server).c_str()));
                };
            vd->forEachField(nullptr, onField);
        }
        else
        {
            context.writeHtml("<table class=\"params table\" width='100%'><tr><th align='right'>Field, Type, Default</th><th>Description</th></tr>\n");

            auto onField = [&] (const ValueDescription::FieldDescription & fd)
                {
                    context.writeHtml(MLDB::format("<tr><td align='right'><p><strong>%s</strong> <br/> <nobr>%s</nobr> <br/> <code>%s</code></p></td><td>%s</td></tr>\n",
                                         fd.fieldName.c_str(),
                                         getTypeName(*fd.description, context.server).c_str(),
                                         getDefaultValue(*fd.description).c_str(),
                                                 renderMarkdown(fd.comment.c_str(), context)));
                };
            vd->forEachField(nullptr, onField);
            context.writeHtml("</table>");
        }
    }
    else if (vd->kind == ValueKind::ENUM) {
        context.writeHtml("<h4>Enumeration <code>" + printTypeName(cppType.rawString()) +"</code></h4>");
        context.writeHtml("<table class=\"params table\"><tr><th>Value</th><th>Description</th></tr>\n");
        for (auto & v: vd->getEnumValues()) {
            context.writeHtml(MLDB::format("<tr><td><code>%s</code></td><td><p>%s</td></tr>\n",
                                 std::get<1>(v).c_str(),
                                 std::get<2>(v).c_str()));
        }
        context.writeHtml("</table>");
    }
    else {
        context.writeHtml("non-structure " + cppType);
    }
}


/*****************************************************************************/
/* MACRO CONTEXT                                                             */
/*****************************************************************************/

MacroContext::
MacroContext(const MacroData * macroData,
             hoedown_buffer * output,
             const hoedown_buffer * text)
    : macroData(macroData),
      output(output),
      text(text)
{
}

void
MacroContext::
writeHtml(const Utf8String & text)
{
    hoedown_buffer_put(output, (const uint8_t *)text.rawData(), text.rawLength());
}

void
MacroContext::
writeText(const Utf8String & text)
{
    hoedown_escape_html(output, (uint8_t *)text.rawData(), text.rawLength(), 0);
}

void
MacroContext::
writeMarkdown(const Utf8String & markdown)
{
    writeHtml(renderMarkdown(markdown.rawString(), *macroData));
}

Utf8String
MacroContext::
prefixUrl(Utf8String url) const
{
    return macroData->server->prefixUrl(url);
}

void
MacroContext::
writeInternalLink(Utf8String url,
                  const Utf8String & anchorText,
                  bool followInternalRedirect)
{
    if (followInternalRedirect) {
        RestRequest request("GET", url.rawString(), {}, "");
        InProcessRestConnection connection;
        macroData->server->handleRequest(connection, request);
        if (connection.responseCode == 301) {
            url = connection.headers.getValue("location");
        }
    }

    writeHtml("<a href=\"");
    writeText(prefixUrl(url));
    writeHtml("\">");
    writeHtml(anchorText);
    writeHtml("</a>");
}

namespace {

struct MacroEntry {
    MldbMacro macro;
};

// On static initialization and destruction, it's possible that an
// earlier library attempts to register before this library has
// initialized.  The static member in macroData() allows this to
// happen; however when the macros are deregistered on destruction,
// this library will already have all members (including the macroData)
// destroyed.  The macroDataIsAlive variable allows us to track if
// this is the case, and if so to not try to deregister the member
// in the (destroyed) map.
bool macroDataIsAlive = false;

struct MacroData {
    MacroData()
    {
        macroDataIsAlive = true;
    }

    ~MacroData()
    {
        macroDataIsAlive = false;
    }

    std::recursive_mutex mutex;
    std::map<std::string, MacroEntry> macros;
};

static MacroData & macroData()
{
    static MacroData data;
    return data;
}

} // file scope

std::shared_ptr<void>
registerMacro(const std::string & macroName,
              MldbMacro macro,
              bool failOnError)
{
    std::unique_lock<std::recursive_mutex> guard(macroData().mutex);

    MacroEntry entry;
    entry.macro = std::move(macro);

    if (!macroData().macros.insert(make_pair(macroName, entry)).second) {
        // TODO: fail on error
        throw HttpReturnException(500, "Error registering documentation macro "
                                  + macroName + ": name already registered");
    }

    auto unregister = [=] (void *)
        {
            if (!macroDataIsAlive)
                return;
            macroData().macros.erase(macroName);
        };
    
    return std::shared_ptr<void>(nullptr, unregister);
}

void callMacro(MacroContext & context,
               const std::string & macroName,
               const Utf8String & args)
{
    std::unique_lock<std::recursive_mutex> guard(macroData().mutex);

    auto it = macroData().macros.find(macroName);
    
    if (it == macroData().macros.end()) {
        context.writeText("Unable to find macro %%" + macroName + " with arguments " + args);
        return;
    }
    
    try {
        MLDB_TRACE_EXCEPTIONS(false);
        it->second.macro(context, macroName, args);
    } catch (const std::exception & exc) {
        context.writeText("Error executing macro %%" + macroName + " with arguments " + args + ": " + exc.what());
    } MLDB_CATCH_ALL {
        context.writeText("Error executing macro %%" + macroName + " with arguments " + args + ": unknown exception");
    }
}

void typeMacro(MacroContext & context,
               const std::string & macroName,
               const Utf8String & args)
{
    renderType(context, args);
}

void nblinkMacro(MacroContext & context,
                 const std::string & macroName,
                 const Utf8String & args)
{
    string address = args.rawString();

    int offset = address.find("/")+1;

    if(offset == string::npos){
        offset = 0;
    }
    string nb(address,offset);

    for (size_t pos = address.find(' '); 
         pos != string::npos; 
         pos = address.find(' ', pos))
        {
            address.replace(pos, 1, "%20");
        }
        
    Utf8String url = "/doc/nblink.html#";
    context.writeHtml("<a href=\"" + context.prefixUrl(url)
                      + address + "\" " + "target=\"_blank\">"+nb+"</a>");
}

void doclinkMacro(MacroContext & context,
                  const std::string & macroName,
                  const Utf8String & argsStr)
{
    vector<string> args = ML::split(argsStr.rawString(), ' ');
    if (args.size() < 2) {
        context.writeHtml("Macro %%doclink needs 2 parameters");
        return;
    }
    string type = args.at(0);
    string kind = args.at(1);
    context.writeInternalLink("/v1/types/" + kind + "s/" + type + "/doc",
                              "<code>"+type+"</code> "+kind + " type",
                              true /* follow redirect */);
}

void codeexampleMacro(MacroContext & context,
                      const std::string & macroName,
                      const Utf8String & argsStr)
{
    try {
        vector<string> args = ML::split(argsStr.rawString(), ' ');
        string filename = context.macroData->dir + "/" + args.at(0);
        string language;
        if (args.size() > 1)
            language = " class=\"" + args[1] + "\"";

        ML::File_Read_Buffer buf(filename);
        
        string result(buf.start(), buf.end());
        
        context.writeHtml("<pre><code" + language + ">");
        
        context.writeText(result);

        context.writeHtml("</code></pre>");
    } catch (const std::exception & exc) {
        context.writeHtml("Error running codeexample macro: " + string(exc.what()));
    }
}

void configMacro(MacroContext & context,
                 const std::string & macroName,
                 const Utf8String & argsStr)
{
    try {
        vector<string> args = ML::split(argsStr.rawString(), ' ');
        if (args.size() < 2) {
            context.writeHtml("Macro %%config needs 2 parameters");
            return;
        }
        string kind = args.at(0);
        string type = args.at(1);
            
        RestRequest request("GET", "/v1/types/" + kind + "s/" + type + "/info", {}, "");
        InProcessRestConnection connection;
        context.macroData->server->handleRequest(connection, request);

        if (connection.responseCode != 200) {
            context.writeHtml("Error running %%config macro with params " + kind + " " + type);
            context.writeHtml("<pre><code>");
            context.writeText(connection.response);
            context.writeHtml("</code></pre>");
            return;
        }

        context.writeHtml("<p>A new " + kind + " of type <code>"+type+"</code> named <code>&lt;id&gt;</code> can be created as follows:</p>");
        context.writeHtml("<pre><code class=\"language-python\">");
        context.writeText("mldb.put(\"/v1/" + kind + "s/\"+<id>, {\n"+
                              "    \"type\": \"" + type + "\"");
        
        Json::Value params = Json::parse(connection.response);
        string typeName;
        bool withParams = false;
        if (!params.isNull()) {
            typeName = params["configType"]["typeName"].asString();
            withParams = !typeName.empty() && !params["configType"]["fields"].isNull();
        }

        if (withParams) {
            context.writeText(",\n    \"params\": {");
            renderType(context, typeName, true);
            context.writeText("\n    }");
        }

        context.writeHtml("\n})</code></pre>");

        if (withParams) {
            context.writeHtml("<p>with the following key-value definitions for <code>params</code>:</p>");
            renderType(context, typeName);
        }

    } catch (const std::exception & exc) {
        context.writeHtml("Error running config macro: " + string(exc.what()));
    }
}

void availabletypesMacro(MacroContext & context,
                         const std::string & macroName,
                         const Utf8String & argsStr)
{
    try {
        vector<string> args = ML::split(argsStr.rawString(), ' ');
        if (args.size() < 2) {
            context.writeHtml("Macro %%availabletypes needs 2 parameters");
            return;
        }
        string kind = args.at(0);
        string format = args.at(1);

        RestRequest request("GET", "/v1/types/" + kind + "s",
                            {{"details", "true"}}, "");
        InProcessRestConnection connection;
        context.macroData->server->handleRequest(connection, request);
            
        if (connection.responseCode != 200) {
            context.writeHtml("Error running %%availabletypes macro for kind " + kind);
            context.writeHtml("<pre><code>");
            context.writeText(connection.response);
            context.writeHtml("</code></pre>");
            return;
        }

        auto isInternal = [] (const Json::Value& flags) {
            for (auto & flag : flags) {
                if (flag.asString() == MldbEntity::INTERNAL_ENTITY) {
                    return true;
                }
            }
            return false;
        };

        // filter internal entities
        auto internalEntitiesFilter = [&] (const Json::Value& params) -> Json::Value {
            if (context.macroData->hideInternalEntities) {
                Json::Value filteredParams;
                for (auto & p : params) {
                    if (!isInternal(p[1]["flags"])) {
                        filteredParams.append(p);
                    }
                }
                return filteredParams;
            }
            else
                return params;
        };

        Json::Value params = internalEntitiesFilter(Json::parse(connection.response));

        if(format == "list") {
            context.writeHtml("<ul>\n");
            for (auto & p: params) {
                string name = p[0].asString();
                string doc = p[1]["docRoute"].asString();

                context.writeHtml("<li>");
                context.writeInternalLink(doc, name, true /* internal redirects */);
                context.writeHtml("</li>\n");
            }
            context.writeHtml("</ul>\n");
        }
        else if(format == "table") {
            context.writeHtml("<table class=\"table\"><tr><th>Type</th><th>Description</th><th>Doc</th></tr>\n");
            for (auto & p: params) {
                string name = p[0].asString();
                string desc = p[1]["description"].asString();
                string doc = p[1]["docRoute"].asString();

                context.writeHtml("<tr><td><code>");
                context.writeText(name);
                context.writeHtml("</code></td><td>");
                context.writeText(desc);
                context.writeHtml("</td><td>");
                context.writeInternalLink(doc, "[doc]", true /* internal redirects */);
                context.writeHtml("</td></tr>\n");
            }
                
            context.writeHtml("</table>");
        }
            
        //context.writeHtml("<pre><code>");
        //context.writeText(params.toStyledString());
        //context.writeHtml("</code></pre>");
    } catch (const std::exception & exc) {
        context.writeHtml("Error running config macro: " + string(exc.what()));
    }
}

RegisterMacro::
RegisterMacro(const std::string & macroName,
              MldbMacro macro,
              bool failOnError)
{
    handle = registerMacro(macroName, macro, failOnError);
}


// Register all of the built-in macro types
auto regType = RegisterMacro("type", typeMacro);
auto regNbLink = RegisterMacro("nblink", nblinkMacro);
auto regDoclink = RegisterMacro("doclink", doclinkMacro);
auto regCodeExample = RegisterMacro("codeexaple", codeexampleMacro);
auto regConfig = RegisterMacro("config", configMacro);
auto regAvailableTypes = RegisterMacro("availabletypes", availabletypesMacro);

} // namespace MLDB


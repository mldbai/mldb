// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** static_content_handler.cc
    Jeremy Barnes, 5 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Handler for static content.
*/

#include <typeinfo>
#include "mldb/server/static_content_handler.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/ext/hoedown/src/buffer.h"
#include "mldb/ext/hoedown/src/html.h"
#include "mldb/ext/hoedown/src/document.h"
#include "mldb/ext/hoedown/src/escape.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/mldb_entity.h"
#include "static_content_macro.h"

using namespace std;

namespace Datacratic {
namespace MLDB {


void renderMacro(hoedown_buffer *ob,
                 const hoedown_buffer *text,
                 const hoedown_renderer_data *,
                 const MacroData & macroData)
{
    MacroContext context(&macroData, ob, text);

    string s((const char *)text->data, 2, text->size - 2);
    auto pos = s.find(' ');
    string name(s, 0, pos);
    string args(s, pos + 1);
    callMacro(context, name, args);
}

void handleNormalText (hoedown_buffer *ob, const hoedown_buffer *text, const hoedown_renderer_data *data)
{
    if (!text)
        return;

    auto state = (hoedown_html_renderer_state *)data->opaque;

    if (text->data && text->size > 2 && text->data[0] == '%' && text->data[1] == '%') {
        renderMacro(ob, text, data, *(MacroData*)state->user_data);
        return;
    }

    hoedown_escape_html(ob, text->data, text->size, 0);
    
}

int handleImage (hoedown_buffer *ob,
                 const hoedown_buffer *link,
                 const hoedown_buffer *title,
                 const hoedown_buffer *alt,
                 const hoedown_renderer_data *data)
{
    auto state = (hoedown_html_renderer_state *)data->opaque;

    if (!link || !link->size) return 0;

    if (link && link->size > 2 && link->data[0] == '%' && link->data[1] == '%') {
        renderMacro(ob, link, data, *(MacroData*)state->user_data);
        return 1;
    }

    HOEDOWN_BUFPUTSL(ob, "<img src=\"");
    hoedown_escape_href(ob, link->data, link->size);
    HOEDOWN_BUFPUTSL(ob, "\" alt=\"");

    if (alt && alt->size)
        hoedown_escape_html(ob, alt->data, alt->size, 0);

    if (title && title->size) {
        HOEDOWN_BUFPUTSL(ob, "\" title=\"");
        hoedown_escape_html(ob, title->data, title->size, 0); }

    hoedown_buffer_puts(ob, "\"/>");
    return 1;
}

// Link handler
int handleLink(hoedown_buffer *ob,
               const hoedown_buffer *content,
               const hoedown_buffer *link,
               const hoedown_buffer *title,
               const hoedown_renderer_data *data)
{
    string linkSource;
    if (link && link->size)
        linkSource = string(link->data, link->data + link->size);
    
    //cerr << "Rendering link " << linkSource << endl;
    
    if (linkSource.rfind(".md") == linkSource.size() - 3
        && linkSource.find("://") == string::npos) {
        linkSource += ".html";
        //cerr << "  rendering as " << linkSource << endl;
    } 

    auto state = (hoedown_html_renderer_state *)data->opaque;

    HOEDOWN_BUFPUTSL(ob, "<a href=\"");

    if (!linkSource.empty())
        hoedown_escape_href(ob, (const uint8_t *)linkSource.c_str(), linkSource.size());

    if(linkSource.find("http") == 0) {
        HOEDOWN_BUFPUTSL(ob, "\" target=\"_blank");
    }
    
    if (title && title->size) {
        HOEDOWN_BUFPUTSL(ob, "\" title=\"");
        hoedown_escape_html(ob, title->data, title->size, 0);
    }

    if (state->link_attributes) {
        hoedown_buffer_putc(ob, '\"');
        state->link_attributes(ob, link, data);
        hoedown_buffer_putc(ob, '>');
    } else {
        HOEDOWN_BUFPUTSL(ob, "\">");
    }

    if (content && content->size) hoedown_buffer_put(ob, content->data, content->size);
    HOEDOWN_BUFPUTSL(ob, "</a>");
    return 1;
}

RestRequestRouter::OnProcessRequest
getStaticRouteHandler(string dir, MldbServer * server, bool hideInternalEntities)
{
    if (dir.find("://") == string::npos)
        dir = "file://" + dir;

    return [dir, server, hideInternalEntities] (RestConnection & connection,
                  const RestRequest & request,
                  const RestRequestParsingContext & context)
        {
        
            string path = context.resources.back().rawData();

            if (path.find("..") != string::npos) {
                throw ML::Exception("not dealing with path with .. in it");
            }

            string filename = dir + "/" + path;

            //cerr << "looking for " << filename << " for resource " << path << endl;

            auto sendFile = [&] (const std::string & filename,
                                 const std::string & mimeType)
                {
                    if (!tryGetUriObjectInfo(filename)) {
                        connection.sendErrorResponse
                        (404,
                         "File '" + filename + "' doesn't exist", "text/plain");
                        return RestRequestRouter::MR_YES;
                    }

                    string filenameToLoad = filename;
                    if (filenameToLoad.find("file://") == 0)
                        filenameToLoad = string(filenameToLoad, 7);

                    cerr << "Loading file " << filename << " as " << filenameToLoad << endl;
                    ML::File_Read_Buffer buf(filenameToLoad);
            
                    string result(buf.start(), buf.end());
                    connection.sendResponse(200, result, mimeType);
                    return RestRequestRouter::MR_YES;
                };

            //cerr << "getting filename " << filename << endl;

            bool isMdHtml = false;

            if (filename.rfind(".md") == filename.size() - 3
                || (isMdHtml = (filename.rfind(".md.html") == filename.size() - 8))) {
                string mimeType;
                mimeType = "text/html";

                //if (tryGetUriObjectInfo(filename)) {
                //    return sendFile(filename, mimeType);
                //}

                string markdownFile = filename;
                if (isMdHtml) {
                    // Strip off the last 5 characters, ie .html
                    markdownFile = string(filename, 0, filename.length() - 5);
                }

                if (!tryGetUriObjectInfo(markdownFile)) {
                    connection.sendErrorResponse
                        (404,
                         "File '" + markdownFile + "' doesn't exist", "text/plain");
                    return RestRequestRouter::MR_YES;
                }

                MacroData macroData;
                macroData.dir = dir;
                macroData.server = server;
                macroData.hideInternalEntities = hideInternalEntities;

                string filenameToLoad = markdownFile;
                if (filenameToLoad.find("file://") == 0)
                    filenameToLoad = string(filenameToLoad, 7);

                cerr << "Loading file " << markdownFile << " as " << filenameToLoad << endl;

                ML::File_Read_Buffer buf(filenameToLoad);

                auto renderer = hoedown_html_renderer_new(HOEDOWN_HTML_USE_XHTML,
                                                          0 /* data.toc_level */,
                                                          &macroData);                
                auto renderer_free = hoedown_html_renderer_free;

                renderer->normal_text = handleNormalText;
                renderer->image = handleImage;
                renderer->link = handleLink;

#if 0
                //case RENDERER_HTML_TOC:
                renderer = hoedown_html_toc_renderer_new(data.toc_level);
                renderer_free = hoedown_html_renderer_free;
#endif
            
                /* Perform Markdown rendering */
                auto ob = hoedown_buffer_new(16384);
                int extensions
                    = HOEDOWN_EXT_MATH
                    | HOEDOWN_EXT_TABLES
                    | HOEDOWN_EXT_FENCED_CODE;
                
                auto document = hoedown_document_new(renderer, (hoedown_extensions)extensions, 10 /*data.max_nesting*/);
                



                hoedown_document_render(document, ob, (const uint8_t *)buf.start(), buf.size());

                // Add MathJax
                std::string result;
                result += "<!DOCTYPE html>\n";
                result += "<html>\n";
                result += "<head>\n";
                result += "<meta charset='utf-8' />\n";
                result += "<title>MLDB Documentation</title>\n";
                result += "<script src='https://cdn.mathjax.org/mathjax/latest/MathJax.js?config=TeX-AMS-MML_HTMLorMML'></script>\n";
                result += "<link rel='stylesheet' href='//cdnjs.cloudflare.com/ajax/libs/highlight.js/8.5/styles/default.min.css'>\n";
                result += "<link rel='stylesheet' href='/resources/css/doc.css'>\n";
                result += "<script src='//cdnjs.cloudflare.com/ajax/libs/highlight.js/8.5/highlight.min.js'></script>\n";
                result += "<script>\n";
                result += "  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){\n";
                result += "  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),\n";
                result += "  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)\n";
                result += "  })(window,document,'script','//www.google-analytics.com/analytics.js','ga');\n";
                result += "  if(window.location.origin == 'http://mldb.ai')\n";
                result += "  { ga('create', 'UA-16909325-9', 'auto'); }\n";
                result += "  else\n";
                result += "  { ga('create', 'UA-16909325-10', {'cookieDomain': 'none'}); }\n";
                result += "  ga('send', 'pageview');\n";
                result += "</script>\n";
                result += "</head>\n";
                result += "<body style='margin-left: 50px; max-width: 1000px'>\n";
                result += std::string((const char *)ob->data, (const char *)(ob->data + ob->size));
                result += "<script>";
                result += "hljs.configure({languages:['http', 'javascript', 'python', 'sql', 'json']});";
                result += "hljs.initHighlightingOnLoad();";
                result += "</script>";
                result += "</body>\n";
                result += "</html>\n";

                //cerr << "result = " << result << endl;

                /* Cleanup */
                hoedown_buffer_free(ob);
                hoedown_document_free(document);
                renderer_free(renderer);
            

                connection.sendResponse(200, result, mimeType);
                return RestRequestRouter::MR_YES;
            }

            if (!tryGetUriObjectInfo(filename)) {
                connection.sendErrorResponse
                    (404,
                     "File '" + filename + "' doesn't exist", "text/plain");
                return RestRequestRouter::MR_YES;
            }

            string mimeType = "text/plain";
            if (filename.find(".html") != string::npos) {
                mimeType = "text/html";
            }
            else if (filename.find(".js") != string::npos) {
                mimeType = "application/javascript";
            }
            else if (filename.find(".css") != string::npos) {
                mimeType = "text/css";
            }
            else if (filename.find(".svg") != string::npos) {
                mimeType = "image/svg+xml";
            }
            return sendFile(filename, mimeType);
        };
}

void serveDocumentationDirectory(RestRequestRouter & parent,
                                 const std::string & route,
                                 const std::string & dir,
                                 MldbServer * server,
                                 bool hideInternalEntities)
{
    parent.addRoute(Rx(route + "/(.*)", "<resource>"),
                    "GET", "Static content",
                    getStaticRouteHandler(dir, server, hideInternalEntities),
                    Json::Value());
}

} // namespace MLDB
} // namespace Datacratic

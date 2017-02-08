// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** static_content_handler.cc
    Jeremy Barnes, 5 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

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
#include "mldb/base/scope.h"
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>


using namespace std;
namespace fs = boost::filesystem;


namespace MLDB {


void renderMacro(hoedown_buffer *ob,
                 const hoedown_buffer *text,
                 const hoedown_renderer_data *,
                 const MacroData & macroData)
{
    MacroContext context(&macroData, ob, text);
    context.server = macroData.server;

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

    if (linkSource.rfind(".md") == linkSource.size() - 3
        && linkSource.find("://") == string::npos) {
        linkSource += ".html";
    }

    auto state = (hoedown_html_renderer_state *)data->opaque;

    HOEDOWN_BUFPUTSL(ob, "<a href=\"");

    if (!linkSource.empty())
        hoedown_escape_href(ob, (const uint8_t *)linkSource.c_str(),
                            linkSource.size());

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

    if (content && content->size) hoedown_buffer_put(ob, content->data,
                                                     content->size);
    HOEDOWN_BUFPUTSL(ob, "</a>");
    return 1;
}

std::string renderMarkdown(const char * buf, size_t len,
                           const MacroData & macroData)
{
    auto renderer = hoedown_html_renderer_new(HOEDOWN_HTML_USE_XHTML,
                                              0 /* data.toc_level */,
                                              (void *)&macroData);                
    Scope_Exit(hoedown_html_renderer_free(renderer));

    renderer->normal_text = handleNormalText;
    renderer->image = handleImage;
    renderer->link = handleLink;

    /* Perform Markdown rendering */
    auto ob = hoedown_buffer_new(16384);
    Scope_Exit(hoedown_buffer_free(ob));

    int extensions
        = HOEDOWN_EXT_MATH
        | HOEDOWN_EXT_TABLES
        | HOEDOWN_EXT_FENCED_CODE;
                
    auto document = hoedown_document_new(renderer,
                                         (hoedown_extensions)extensions,
                                         10 /*data.max_nesting*/);
    Scope_Exit(hoedown_document_free(document));
    
    hoedown_document_render(document, ob, (const uint8_t *)buf, len);

    std::string result((const char *)ob->data, (const char *)(ob->data + ob->size));
    return result;
}

std::string renderMarkdown(const std::string & str, const MacroData & macroData)
{
    return renderMarkdown(str.c_str(), str.length(), macroData);
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
            const string & path = context.resources.back().rawString();

            if (path.find("..") != string::npos) {
                throw MLDB::Exception("not dealing with path with .. in it");
            }

            string filename = (fs::path(dir) / fs::path(path)).string();

            //cerr << "looking for " << filename << " for resource " << path << endl;

            auto sendFile = [&connection, &server] (const std::string & filename,
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

                    //cerr << "Loading file " << filename << " as " << filenameToLoad << endl;
                    ML::File_Read_Buffer buf(filenameToLoad);
            
                    string result(buf.start(), buf.end());
                    boost::algorithm::replace_all(result, "{{HTTP_BASE_URL}}", server->httpBaseUrl);
                    connection.sendResponse(200, result, mimeType);
                    return RestRequestRouter::MR_YES;
                };

            //cerr << "getting filename " << filename << endl;

            bool isMdHtml = false;

            if (filename.rfind(".md") == filename.size() - 3
                || (isMdHtml = (filename.rfind(".md.html") == filename.size() - 8))) {
                string mimeType;
                mimeType = "text/html";

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

                //cerr << "Loading file " << markdownFile << " as " << filenameToLoad << endl;

                ML::File_Read_Buffer buf(filenameToLoad);

                // Render the document
                std::string result;
                result += "<!DOCTYPE html>\n";
                result += "<html>\n";
                result += "<head>\n";
                result += "<meta charset='utf-8' />\n";
                result += "<title>MLDB Documentation</title>\n";
                result += "<script src='https://cdn.mathjax.org/mathjax/latest/MathJax.js?config=TeX-AMS-MML_HTMLorMML'></script>\n";
                result += "<link rel='stylesheet' href='" + server->prefixUrl("/resources/css/prism.css") + "'>\n";
                result += "<link rel='stylesheet' href='" + server->prefixUrl("/resources/css/doc.css") + "'>\n";
                result += "<script src='" + server->prefixUrl("/resources/js/jquery-1.11.2.min.js") + "'></script>\n";
                result += "<script src='" + server->prefixUrl("/resources/js/prism.js") + "'></script>\n";
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
                result += renderMarkdown(buf.start(), buf.size(), macroData);
                result += "</body>\n";
                result += "</html>\n";

                boost::algorithm::replace_all(result, "{{HTTP_BASE_URL}}", server->httpBaseUrl);
                connection.sendResponse(200, result, mimeType);
                return RestRequestRouter::MR_YES;
            }

            Url url = makeUrl(filename);
            struct stat stats;
            int res = ::stat(url.path().c_str(), &stats);

            // this is our Apache's rewrite rules

            // (1) [301] directory resources redirected to directory with trailing slash
            if (res != -1 // path exist
                && (stats.st_mode & S_IFMT) == S_IFDIR  // it is a directory
                && filename.back() != '/' // it does not have a trailing backslash
                ) {
                // force redirect to the proper path
                connection.sendRedirect(301, context.resources.front().rawString() + "/");
                return RestRequestRouter::MR_YES;
            }

            // (2) [200, 403] implicit access to index.html for directory resources
            if (res != -1 // path exist
                && (stats.st_mode & S_IFMT) == S_IFDIR  // it is a directory
                ) {
                filename += "index.html";
                if (!tryGetUriObjectInfo(filename)) {
                    connection.sendErrorResponse(403, "Directory listing not allowed", "text/plain");
                    return RestRequestRouter::MR_YES;
                }
            }

            // (3) [404] page not found on resouces that do not exist
            if (res == -1 // path do not exist
                ) {
                connection.sendErrorResponse(404, "File '" + filename + "' doesn't exist", "text/plain");
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
            else if (filename.find(".pdf") != string::npos) {
                mimeType = "application/pdf";
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
    parent.addRoute(Rx(route + "(/.*)", "<resource>"),
                    "GET", "Static content",
                    getStaticRouteHandler(dir, server, hideInternalEntities),
                    Json::Value());
}

} // namespace MLDB


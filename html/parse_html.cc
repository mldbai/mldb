/** parse_html.cc
    Jeremy Barnes, 29 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Function to parse HTML.
*/

#include "html_plugin.h"
#include "parse_html.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/html/ext/hubbub/parser.h"
#include "mldb/base/scope.h"
#include "mldb/sql/builtin_functions.h"
#include "mldb/html/css.h"
#include "mldb/jml/utils/string_functions.h"

using namespace std;


namespace MLDB {



/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

struct ParseHandler {
    static hubbub_error handleToken(const hubbub_token *token, void *pw)
    {
        try {
            ParseHandler * parse = (ParseHandler *)pw;

            switch (token->type) {
            case HUBBUB_TOKEN_DOCTYPE:
                return convertError(parse->handleDoctype(convertDoctype(token->data.doctype)));

            case HUBBUB_TOKEN_START_TAG:
                return convertError(parse->handleStartTag(convertTag(token->data.tag)));

            case HUBBUB_TOKEN_END_TAG:
                return convertError(parse->handleEndTag(convertTag(token->data.tag)));

            case HUBBUB_TOKEN_COMMENT:
                return convertError(parse->handleComment(convertText(token->data.comment)));

            case HUBBUB_TOKEN_CHARACTER:
                return convertError(parse->handleText(convertText(token->data.character)));

            case HUBBUB_TOKEN_EOF:
                return convertError(parse->handleEof());
            default:
                return HUBBUB_UNKNOWN;
            }
        } MLDB_CATCH_ALL {
            return HUBBUB_UNKNOWN;
        }
    }

    static const hubbub_doctype & convertDoctype(const hubbub_doctype & tp)
    {
        return tp;
    }

    static const hubbub_tag & convertTag(const hubbub_tag & t)
    {
        return t;
    }

    static const hubbub_string & convertText(const hubbub_string & t)
    {
        return t;
    }

    static hubbub_error convertError(hubbub_error error)
    {
        return error;
    }

    static void hubbub_error_handler(uint32_t line, uint32_t col,
                                     const char *message, void *pw)
    {
        try {
            ParseHandler * parse = (ParseHandler *)pw;
            parse->handleError(line, col, message);
        } MLDB_CATCH_ALL {
            return;
        }
    }

    static Utf8String toString(const hubbub_string & str)
    {
        return Utf8String(string((const char *)str.ptr, (const char *)str.ptr + str.len));
    }

    static Utf8String getAttr(const hubbub_tag & tag,
                              const char * attrName)
    {
        size_t len = strlen(attrName);
        for (unsigned i = 0;  i < tag.n_attributes;  ++i) {
            auto & attr = tag.attributes[i];
            if (len == attr.name.len
                && strncasecmp(attrName,
                               (const char *)tag.attributes[i].name.ptr,
                               len) == 0)
                return toString(tag.attributes[i].value);
        }
        return Utf8String();
    }
    
    virtual hubbub_error handleError(uint32_t line, uint32_t col,
                                     const char * message)
    {
        return HUBBUB_OK;
    }

    virtual hubbub_error handleDoctype(const hubbub_doctype & doctype)
    {
        return HUBBUB_OK;
    }

    virtual hubbub_error handleStartTag(const hubbub_tag & tag)
    {
        //cerr << "start tag " << toString(tag.name) << " with "
        //     << tag.n_attributes << " attributes" << endl;
        //for (unsigned i = 0;  i < tag.n_attributes;  ++i) {
        //    cerr << "  " << toString(tag.attributes[i].name) << " = "
        //         << toString(tag.attributes[i].value) << endl;
        //}
        
        return HUBBUB_OK;
    }

    virtual hubbub_error handleEndTag(const hubbub_tag & tag)
    {
        //cerr << "end tag " << toString(tag.name) << endl;
        return HUBBUB_OK;
    }

    virtual hubbub_error handleComment(const hubbub_string & comment)
    {
        return HUBBUB_OK;
    }

    virtual hubbub_error handleText(const hubbub_string & text)
    {
        //cerr << "got text " << toString(text) << endl;
        return HUBBUB_OK;
    }

    virtual hubbub_error handleEof()
    {
        return HUBBUB_OK;
    }

};

void parseHtml(const CellValue & html,
               ParseHandler & handler)
{
    hubbub_parser * parser;
    auto err = hubbub_parser_create("utf-8", true /* fix encoding */,
                                    &parser);
    if (err != HUBBUB_OK) {
        throw AnnotatedException(500, "Error initializing HTTP parser: "
                                  + string(hubbub_error_to_string(err)));
    }
    Scope_Exit(hubbub_parser_destroy(parser));

    hubbub_parser_optparams params;
    params.token_handler.handler = &ParseHandler::handleToken;
    params.token_handler.pw = &handler;

    err = hubbub_parser_setopt(parser, HUBBUB_PARSER_TOKEN_HANDLER,
                               &params);
    
    if (err != HUBBUB_OK) {
        throw AnnotatedException(500, "Error setting up HTTP parser: "
                                  + string(hubbub_error_to_string(err)));
    }
    
    err = hubbub_parser_parse_chunk(parser,
                                    (const uint8_t *)html.blobData(),
                                    html.blobLength());
    if (err != HUBBUB_OK) {
        throw AnnotatedException(500, "Error running HTTP parser: "
                                  + string(hubbub_error_to_string(err)));
    }
    
    err = hubbub_parser_completed(parser);

    if (err != HUBBUB_OK) {
        throw AnnotatedException(500, "Error finishing HTTP parser: "
                                  + string(hubbub_error_to_string(err)));
    }

    hubbub_charset_source source;
    cerr << "charset is " << hubbub_parser_read_charset(parser, &source)
         << endl;
}

struct ExtractHandler: public ParseHandler {

    std::shared_ptr<Css::Selector> selector = Css::Selector::STAR;
    
    Css::Path path;

    Utf8String pathToString() const
    {
        return path.toString();
        Utf8String result;
        for (auto & p: path) {
            if (!result.empty())
                result += " ";
            result += p.toString();
        }
        return result;
    }

    virtual hubbub_error handleError(uint32_t line, uint32_t col,
                                     const char * message)
    {
        return HUBBUB_OK;
    }

    virtual hubbub_error handleDoctype(const hubbub_doctype & doctype)
    {
        return HUBBUB_OK;
    }

    virtual hubbub_error handleStartTag(const hubbub_tag & tag)
    {
#if 0
        cerr << "start tag " << toString(tag.name) << " with "
             << tag.n_attributes << " attributes" << endl;
        for (unsigned i = 0;  i < tag.n_attributes;  ++i) {
            cerr << "  " << toString(tag.attributes[i].name) << " = "
                 << toString(tag.attributes[i].value) << endl;
        }
#endif     
   
        Css::PathElement element;
        element.tag = toString(tag.name);
        element.id = getAttr(tag, "id");
        for (std::string & s: ML::split(getAttr(tag, "class").rawString(), ' ')) {
            element.classes.emplace_back(std::move(s));
        }

        path.emplace_back(std::move(element));

        return HUBBUB_OK;
    }

    virtual hubbub_error handleEndTag(const hubbub_tag & tag)
    {
        if (path.empty())
            return HUBBUB_OK;
        if (toString(tag.name) != path.back().tag)
            return HUBBUB_OK;
        path.pop_back();
        return HUBBUB_OK;
    }

    virtual hubbub_error handleComment(const hubbub_string & comment)
    {
        return HUBBUB_OK;
    }

    virtual hubbub_error handleText(const hubbub_string & text)
    {
        return HUBBUB_OK;
    }

    virtual hubbub_error handleEof()
    {
        //ExcAssert(path.empty());
        return HUBBUB_OK;
    }
};

#if 0
/*****************************************************************************/
/* PARSE HTML FUNCTION                                                       */
/*****************************************************************************/



DEFINE_STRUCTURE_DESCRIPTION(ParseHtmlConfig);

ParseHtmlConfigDescription::
ParseHtmlConfigDescription()
{
}

ParseHtml::
ParseHtml(MldbEngine * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<ParseHtmlConfig>();   
}

Any
ParseHtml::
getStatus() const
{
    return Any();
}

ExpressionValue
ParseHtml::
apply(const FunctionApplier & applier,
      const ExpressionValue & context) const
{
    StructValue result;

    ExpressionValue text = context.getColumn("text");
    Utf8String textString = text.toUtf8String();
    
    ParseHandler parser;
    parseHtml(textString, parser);

    Utf8String output("output");
    result.emplace_back("output",
                        ExpressionValue(output, text.getEffectiveTimestamp()));
    
    return std::move(result);
}

FunctionInfo
ParseHtml::
getFunctionInfo() const
{
    std::vector<KnownColumn> in, out;
    
    in.emplace_back(ColumnPath("text"),
                    std::make_shared<BlobValueInfo>(),
                    COLUMN_IS_DENSE, 0);
    out.emplace_back(ColumnPath("output"),
                     std::make_shared<UnknownRowValueInfo>(),
                     COLUMN_IS_DENSE, 0);
    
    return { std::make_shared<RowValueInfo>(std::move(in),
                                            SCHEMA_CLOSED),
             std::make_shared<RowValueInfo>(std::move(out),
                                           SCHEMA_CLOSED)
            };
}

RegisterFunctionType<ParseHtml, ParseHtmlConfig>
regParseHtml(htmlPackage(),
             "html.parse",
             "Parse HTML into a structured document",
             "ParseHtml.md.html");

#endif

/*****************************************************************************/
/* EXTRACT LINKS FUNCTION                                                    */
/*****************************************************************************/

struct LinkExtracter: public ExtractHandler {

    vector<Utf8String> links;

    virtual hubbub_error handleStartTag(const hubbub_tag & tag)
    {
        ExtractHandler::handleStartTag(tag);

        if (toString(tag.name) == "a"
            && selector->match(path)) {
            Utf8String loc = getAttr(tag, "href");
            links.push_back(loc);
            
            cerr << "link " << pathToString() << " at loc " << loc << endl;
        }
        return HUBBUB_OK;
    }

#if 0
    virtual hubbub_error handleEndTag(const hubbub_tag & tag)
    {
        cerr << "end tag " << toString(tag.name) << endl;
        return HUBBUB_OK;
    }

    virtual hubbub_error handleText(const hubbub_string & text)
    {
        cerr << "got text " << toString(text) << endl;
        return HUBBUB_OK;
    }
#endif
    
};

struct ExtractLinksOptions {
    std::shared_ptr<Css::Selector> selector = Css::Selector::STAR;
};

DECLARE_STRUCTURE_DESCRIPTION(ExtractLinksOptions);
DEFINE_STRUCTURE_DESCRIPTION(ExtractLinksOptions);

ExtractLinksOptionsDescription::
ExtractLinksOptionsDescription()
{
    addField("selector", &ExtractLinksOptions::selector,
             "CSS selector for which elements should be extracted");
}

ExpressionValue
extract_links(const std::vector<ExpressionValue> & args,
              const SqlRowScope & context)
{
    checkArgsSize(args.size(), 1, 2, "extract_links");

    ExtractLinksOptions options;

    if (args.size() > 1) {
        options = args[1].extractT<ExtractLinksOptions>();
    }

    CellValue input = args[0].coerceToBlob();

    LinkExtracter parser;
    parser.selector = options.selector;
    parseHtml(input, parser);
    
    vector<CellValue> vals;
    vals.reserve(parser.links.size());
    for (auto & l: parser.links) {
        vals.emplace_back(std::move(l));
    }

    return ExpressionValue(std::move(vals),
                           args[0].getEffectiveTimestamp());
}

BoundFunction
bind_extract_links(const Utf8String &functionName,
                   const std::vector<BoundSqlExpression> & args,
                   SqlBindingScope & context)
{
    // Return an escaped string from a path
    checkArgsSize(args.size(), 1, 2, "extract_links");

    return {
        extract_links,
        std::make_shared<EmbeddingValueInfo>(ST_UTF8STRING)
    };
}

static RegisterFunction
register_extract_links("extract_links", bind_extract_links);


/*****************************************************************************/
/* EXTRACT TEXT FUNCTION                                                    */
/*****************************************************************************/

struct TextExtracter: public ParseHandler {

    Utf8String text;

    virtual hubbub_error handleText(const hubbub_string & text)
    {
        this->text += toString(text);
        return HUBBUB_OK;
    }
};

ExpressionValue
extract_text(const std::vector<ExpressionValue> & args,
              const SqlRowScope & context)
{
    checkArgsSize(args.size(), 1);

    CellValue input = args[0].coerceToBlob();

    TextExtracter parser;
    parseHtml(input, parser);
    
    return ExpressionValue(std::move(parser.text),
                           args[0].getEffectiveTimestamp());
}

BoundFunction
bind_extract_text(const Utf8String &functionName,
                   const std::vector<BoundSqlExpression> & args,
                   SqlBindingScope & context)
{
    // Return an escaped string from a path
    checkArgsSize(args.size(), 1);

    return {
        extract_text,
        std::make_shared<Utf8StringValueInfo>()
    };
}

static RegisterFunction
register_extract_text("extract_text", bind_extract_text);

} // namespace MLDB

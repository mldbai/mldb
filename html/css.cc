/** css.cc
    Jeremy Barnes, 30 September 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    CSS selector implementation.
*/

#include "css.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/value_description.h"

using namespace std;

namespace MLDB {
namespace Css {


namespace {

void skipCssWhitespace(MLDB::ParseContext & context)
{
    context.skip_whitespace();
}

Utf8String expectElementType(MLDB::ParseContext & context)
{
    return context.expect_text(" \n[:.#>+~{,*", false,
                               "expected CSS element type");
}

Utf8String expectIdName(MLDB::ParseContext & context)
{
    return expectElementType(context);
}

Utf8String expectClassName(MLDB::ParseContext & context)
{
    return expectElementType(context);
}

Utf8String expectAttrName(MLDB::ParseContext & context)
{
    return expectElementType(context);
}

Utf8String expectAttrValue(MLDB::ParseContext & context)
{
    return expectElementType(context);
}

} // file scope



/*****************************************************************************/
/* PATH ELEMENT                                                              */
/*****************************************************************************/

PathElement
PathElement::
parse(MLDB::ParseContext & context)
{
    PathElement result;
    result.tag = expectElementType(context);

    while (!context.eof() && !isspace(*context)) {
        switch (*context) {
        case '#':
            ++context;
            if (!result.id.empty())
                context.exception("double id name");
            result.id = expectIdName(context);
            break;
        case '.':
            ++context;
            result.classes.emplace_back(expectClassName(context));
            break;
        case '[':
            ++context;
            context.skip_whitespace();
            while (!context.match_literal(']')) {
                Utf8String attrName = expectAttrName(context);
                context.skip_whitespace();
                Utf8String attrValue;
                if (context.match_literal('=')) {
                    attrValue = expectAttrValue(context);
                }
                result.attrs.emplace_back(std::move(attrName),
                                          std::move(attrValue));
                if (context.match_literal(']'))
                    break;
                if (!context.match_whitespace()) {
                    context.exception("expected space between attribute values");
                }
            }
            break;
        default:
            context.exception("unknown path element separator");
        }
    }

    return result;
}


/*****************************************************************************/
/* PATH                                                                      */
/*****************************************************************************/

Path
Path::
parse(const Utf8String & path)
{
    MLDB::ParseContext context(path.rawString(),
                              path.rawData(),
                              path.rawLength(),
                              1, 1);
    return parse(context);
}

Path
Path::
parse(MLDB::ParseContext & context)
{
    Path result;

    context.skip_whitespace();
    
    while (!context.eof()) {
        result.emplace_back(PathElement::parse(context));
        if (!context.eof())
            context.expect_whitespace();
    }

    return result;
}

Utf8String
Path::
toString() const
{
    Utf8String result;
    for (auto & p: *this) {
        if (!result.empty())
            result += " ";
        result += p.toString();
    }
    return result;
}

const std::shared_ptr<Selector> Selector::STAR = Selector::parse("*");

std::shared_ptr<Selector>
Selector::
parse(const Utf8String & selector,
      const Utf8String & filename,
      int lineNumber, int columnNumber)
{
    MLDB::ParseContext context(filename.rawString(),
                              selector.rawData(),
                              selector.rawLength(),
                              lineNumber, columnNumber);
    return parse(context);
}

std::shared_ptr<Selector>
parseSelectorSequence(MLDB::ParseContext & context)
{
    skipCssWhitespace(context);

    auto sequence = std::make_shared<SelectorSequence>();
    
    if (context.eof() || *context == ',' || context.match_literal('*')) {
        sequence->initial = std::make_shared<UniversalSelector>();
    }
    else {
        Utf8String elementType = expectElementType(context);
        sequence->initial = std::make_shared<TypeSelector>(std::move(elementType));
    }

    for (;;) {
        if (context.eof()) {
            return std::make_shared<SelectorTerminal>(sequence);
        }

        switch (*context) {
        case ',':
            return std::make_shared<SelectorTerminal>(sequence);
        case '\n':
        case '\t':
            context.exception("can't chain elements yet");
        case '.':
            ++context;
            sequence->modifiers.emplace_back
                (std::make_shared<ClassModifier>(expectClassName(context)));
            break;
        case ':':
            context.exception("pseudo terminals not supported yet");
        case '[':
            context.exception("attributes not supported yet");
        case '#':
            ++context;
            sequence->modifiers.emplace_back
                (std::make_shared<IdModifier>(expectIdName(context)));
            break;
        case ' ': {
            ++context;
            auto r = Selector::parse(context);
            context.expect_eof();
            return std::make_shared<SelectorOp>
                (std::make_shared<SelectorTerminal>(sequence),
                 std::move(r),
                 SelectorOp::DESCENDENT);
        }
        case '>': {
            ++context; 
            auto r = Selector::parse(context);
            context.expect_eof();
            return std::make_shared<SelectorOp>
                (std::make_shared<SelectorTerminal>(sequence),
                 std::move(r),
                 SelectorOp::CHILD);
        }
        case '+': {
            ++context; 
            auto r = Selector::parse(context);
            context.expect_eof();
            return std::make_shared<SelectorOp>
                (std::make_shared<SelectorTerminal>(sequence),
                 std::move(r),
                 SelectorOp::ADJACENT_SIBLING);
        }
        case '~': {
            ++context; 
            auto r = Selector::parse(context);
            context.expect_eof();
            return std::make_shared<SelectorOp>
                (std::make_shared<SelectorTerminal>(sequence),
                 std::move(r),
                 SelectorOp::SIBLING);
        }
        default:
            context.exception("unknown selector operator");
        }
    }
}

std::shared_ptr<Selector>
Selector::
parse(MLDB::ParseContext & context)
{
    std::vector<std::shared_ptr<Selector> > elements;

    for (;;) {
        elements.emplace_back(parseSelectorSequence(context));
        skipCssWhitespace(context);
        if (context.eof() || !context.match_literal(',')) {
            break;
        }
    }
    
    if (elements.empty()) {
        auto sequence = std::make_shared<SelectorSequence>();
        sequence->initial = std::make_shared<UniversalSelector>();
        return std::make_shared<SelectorTerminal>(sequence);
    }
    else if (elements.size() == 1) {
        return elements[0];
    }
    else {
        return std::make_shared<SelectorUnion>(std::move(elements));
    }
}


/*****************************************************************************/
/* VALUE DESCRIPTIONS                                                        */
/*****************************************************************************/

struct SelectorPtrDescription
    : public ValueDescriptionT<std::shared_ptr<Selector> > {

    virtual void parseJsonTyped(std::shared_ptr<Selector> * val,
                                JsonParsingContext & context) const
    {
        Utf8String str = context.expectStringUtf8();
        *val = Selector::parse(str);
    }

    virtual void printJsonTyped(const std::shared_ptr<Selector> * val,
                                JsonPrintingContext & context) const
    {
        if (!(*val)) {
            context.writeString("*");
            return;
        }
        context.writeStringUtf8((*val)->toString());
    }
    
    virtual bool isDefaultTyped(const std::shared_ptr<Selector> * val) const
    {
        return !*val || (*val)->toString() == "*";
    }
};

DEFINE_VALUE_DESCRIPTION_NS(std::shared_ptr<Selector>,
                            SelectorPtrDescription);


} // namespace Css
} // namespace MLDB

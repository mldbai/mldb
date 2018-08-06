/** css.h                                                          -*- C++ -*-
    Jeremy Barnes, 30 September 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    Cascading style sheet selectors.
*/

#pragma once

#include "mldb/types/string.h"
#include <memory>
#include <vector>
#include <algorithm>
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/base/parse_context.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {
namespace Css {


/*****************************************************************************/
/* PATH ELEMENT                                                              */
/*****************************************************************************/

struct PathElement {
    Utf8String tag;
    Utf8String id;
    std::vector<Utf8String> classes;
    std::vector<std::pair<Utf8String, Utf8String> > attrs;

    bool hasClass(const Utf8String & className) const
    {
        return std::find(classes.begin(), classes.end(), className)
            != classes.end();
    }

    bool hasId(const Utf8String & id) const
    {
        return this->id == id;
    }

    Utf8String toString() const
    {
        Utf8String result = tag;
        if (!id.empty())
            result += "#" + id;
        for (auto & c: classes) {
            result += "." + c;
        }
        
        if (!attrs.empty()) {
            result += "[";
            bool first = true;
            for (auto & a: attrs) {
                if (!first) {
                    result += " ";
                }
                first = false;
                result += a.first + "=" + a.second;
            }
            result += "]";
        }

        return result;
    }

    static PathElement parse(MLDB::ParseContext & context);
};


/*****************************************************************************/
/* PATH                                                                      */
/*****************************************************************************/

struct Path: public std::vector<PathElement> {
    Utf8String toString() const;

    static Path parse(MLDB::ParseContext & context);
    static Path parse(const Utf8String & path);
};


/*****************************************************************************/
/* SELECTOR                                                                  */
/*****************************************************************************/

/** CSS Selectors for filtering elements from HTML documents.

    See here:

    https://www.w3.org/TR/css3-selectors/
*/

struct Selector {
    virtual ~Selector()
    {
    }

    virtual Utf8String toString() const = 0;

    virtual bool match(const Path & path) const = 0;

    virtual std::vector<std::pair<int, int> >
    matchLocations(const Path & path) const = 0;

    static const std::shared_ptr<Selector> STAR;

    static std::shared_ptr<Selector>
    parse(MLDB::ParseContext & context);

    static std::shared_ptr<Selector>
    parse(const Utf8String & selector,
          const Utf8String & filename = "",
          int lineNumber = 1, int columnNumber = 1);
};

struct SimpleSelector {
    virtual ~SimpleSelector()
    {
    }

    virtual Utf8String toString() const = 0;

    virtual bool match(const PathElement & path) const = 0;
};

struct InitialSelector: public SimpleSelector {
};

struct SelectorModifier: public SimpleSelector {
};

struct UniversalSelector: public InitialSelector {

    virtual bool match(const PathElement & path) const
    {
        return true;
    }

    virtual Utf8String toString() const
    {
        return "*";
    }
};

struct TypeSelector: public InitialSelector {

    TypeSelector(Utf8String tagName)
        : tagName(std::move(tagName))
    {
    }

    Utf8String tagName;

    virtual bool match(const PathElement & path) const
    {
        return path.tag == tagName;
    }

    virtual Utf8String toString() const
    {
        return tagName;
    }
};

struct AttributeModifier: public SelectorModifier {
    enum Op {
        OP_EXISTS,
        OP_EQUALS,
        OP_IN_SET,
        OP_BEGINS_WITH,
        OP_ENDS_WITH,
        OP_CONTAINS,
        OP_HYPHEN_LIST_BEGINS
    };

    Utf8String attribute;
    Op op;
    Utf8String argument;
};

struct ClassModifier: public SelectorModifier {
    ClassModifier(Utf8String className)
        : className(std::move(className))
    {
    }

    Utf8String className;
    
    virtual bool match(const PathElement & path) const
    {
        return path.hasClass(className);
    }

    virtual Utf8String toString() const
    {
        return "." + className;
    }
};

struct IdModifier: public SelectorModifier {
    IdModifier(Utf8String idName)
        : idName(std::move(idName))
    {
    }

    Utf8String idName;
    
    virtual bool match(const PathElement & path) const
    {
        return path.hasId(idName);
    }

    virtual Utf8String toString() const
    {
        return "." + idName;
    }
};

struct PseudoClassModifier: public SelectorModifier {
};

struct SelectorSequence: public SimpleSelector {
    std::shared_ptr<InitialSelector> initial;
    std::vector<std::shared_ptr<SelectorModifier> > modifiers;

    virtual bool match(const PathElement & path) const
    {
        if (!initial->match(path))
            return false;
        for (auto & modifier: modifiers) {
            if (!modifier->match(path))
                return false;
        }
        return true;
    }

    virtual Utf8String toString() const
    {
        Utf8String result = initial->toString();
        for (auto & m: modifiers)
            result += m->toString();
        return result;
    }
};

struct SelectorTerminal: public Selector {
    SelectorTerminal(std::shared_ptr<SelectorSequence> terminal)
        : terminal(terminal)
    {
    }

    std::shared_ptr<SelectorSequence> terminal;

    virtual Utf8String toString() const
    {
        return terminal->toString();
    }

    virtual bool match(const Path & path) const
    {
        return !path.empty() && terminal->match(path.back());
    }

    virtual std::vector<std::pair<int, int> >
    matchLocations(const Path & path) const
    {
        std::vector<std::pair<int, int> > result;
        for (size_t i = 0;  i < path.size();  ++i) {
            if (terminal->match(path[i]))
                result.emplace_back(i, i);
        }
        return result;
    }
};

struct SelectorOp: public Selector {
    enum Combinator {
        DESCENDENT,
        CHILD,
        ADJACENT_SIBLING,
        SIBLING
    };

    SelectorOp(std::shared_ptr<Selector> left,
               std::shared_ptr<Selector> right,
               Combinator op)
        : left(std::move(left)),
          right(std::move(right)),
          op(op)
    {
    }
    
    std::shared_ptr<Selector> left, right;
    Combinator op;
    
    virtual Utf8String toString() const
    {
        Utf8String result = left->toString();
        switch (op) {
        case DESCENDENT: result += " ";  break;
        case CHILD: result += " > ";  break;
        case ADJACENT_SIBLING: result += " + ";  break;
        case SIBLING: result += " ~ ";  break;
        }
        result += right->toString();
        
        return result;
    }

    virtual bool match(const Path & path) const
    {
        auto loc = matchLocations(path);
        for (auto & l: loc) {
            if (l.second == path.size() - 1)
                return true;
        }
        return false;
    }

    virtual std::vector<std::pair<int, int> >
    matchLocations(const Path & path) const
    {
        std::vector<std::pair<int, int> > result;

        switch (op) {
        case DESCENDENT: {
            auto locRight = right->matchLocations(path);
            for (std::pair<int, int> l: left->matchLocations(path)) {
                for (std::pair<int, int> r: locRight) {
                    if (r.first > l.second)
                        result.emplace_back(l.first, r.second);
                }
            }
            break;
        }
        case CHILD: {
            auto locRight = right->matchLocations(path);
            for (std::pair<int, int> l: left->matchLocations(path)) {
                for (std::pair<int, int> r: locRight) {
                    if (r.first == l.second + 1)
                        result.emplace_back(l.first, r.second);
                }
            }
            break;
        }
        case ADJACENT_SIBLING:
        case SIBLING:
            throw AnnotatedException(600, "Sibling selectors not supported");
        }

        return result;
    }
};

struct SelectorUnion: public Selector {
    SelectorUnion()
    {
    }

    SelectorUnion(std::vector<std::shared_ptr<Selector> > elements)
        : elements(std::move(elements))
    {
        ExcAssert(!this->elements.empty());
    }
    
    std::vector<std::shared_ptr<Selector> > elements;
    
    virtual Utf8String toString() const
    {
        Utf8String result = elements.at(0)->toString();
        for (size_t i = 1;  i < elements.size();  ++i) {
            result += ", " + elements[i]->toString();
        }
        return result;
    }

    virtual bool match(const Path & path) const
    {
        for (auto & e: elements) {
            if (e->match(path))
                return true;
        }
        return false;
    }

    virtual std::vector<std::pair<int, int> >
    matchLocations(const Path & path) const
    {
        std::vector<std::pair<int, int> > result;

        for (auto & e: elements) {
            auto locs = e->matchLocations(path);
            result.insert(result.end(), locs.begin(), locs.end());
        }

        // De-duplicate
        std::sort(result.begin(), result.end());
        result.erase(std::unique(result.begin(), result.end()), result.end());

        return result;
    }
};

PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<Selector>);



} // namespace Css
} // namespace MLDB

// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* xml_helpers.h                                                   -*- C++ -*-
   Jeremy Barnes, 12 May 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.
   
   Helper functions to deal with XML.   
*/

#include <string>
#include "mldb/ext/tinyxml2/tinyxml2.h"
#include <boost/lexical_cast.hpp>
#include "mldb/arch/exception.h"
#include "mldb/jml/utils/string_functions.h"


namespace MLDB {

const tinyxml2::XMLNode * extractNode(const tinyxml2::XMLNode * element,
                                      const std::string & path);

bool pathExists(const tinyxml2::XMLNode * element, const std::string & path);

template<typename T>
T extract(const tinyxml2::XMLNode * element, const std::string & path)
{
    using namespace std;

    if (!element)
        throw MLDB::Exception("can't extract from missing element");
    //tinyxml2::XMLHandle handle(element);

    const auto p = extractNode(element, path);
    auto text = tinyxml2::XMLHandle(const_cast<tinyxml2::XMLNode *>(p)).FirstChild().ToText();

    if (!text) {
        return boost::lexical_cast<T>("");
    }
    return boost::lexical_cast<T>(text->Value());
}

template<typename T>
T extractDef(const tinyxml2::XMLNode * element, const std::string & path,
             const T & ifMissing)
{
    using namespace std;

    if (!element) return ifMissing;

    vector<string> splitPath = ML::split(path, '/');
    const tinyxml2::XMLNode * p = element;
    for (unsigned i = 0;  i < splitPath.size();  ++i) {
        p = p->FirstChildElement(splitPath[i].c_str());
        if (!p)
            return ifMissing;
    }

    auto text = tinyxml2::XMLHandle(const_cast<tinyxml2::XMLNode *>(p)).FirstChild().ToText();

    if (!text) return ifMissing;

    return boost::lexical_cast<T>(text->Value());
}

template<typename T>
T extract(const std::unique_ptr<tinyxml2::XMLDocument> & doc,
          const std::string & path)
{
    return extract<T>(doc.get(), path);
}

template<typename T>
T extractDef(const std::unique_ptr<tinyxml2::XMLDocument> & doc,
             const std::string & path, const T & def)
{
    return extractDef<T>(doc.get(), path, def);
}

template<typename T>
T extract(const tinyxml2::XMLDocument & doc,
          const std::string & path)
{
    const tinyxml2::XMLNode * element = &doc;
    return extract<T>(element, path);
}

template<typename T>
T extractDef(const tinyxml2::XMLDocument & doc,
             const std::string & path, const T & def)
{
    const tinyxml2::XMLNode * element = &doc;
    return extractDef<T>(element, path, def);
}

inline std::string
xmlDocumentAsString(const tinyxml2::XMLDocument & xmlDocument)
{
    tinyxml2::XMLPrinter printer;

    const_cast<tinyxml2::XMLDocument &>(xmlDocument).Print(&printer);

    return std::string(printer.CStr());
}

} // namespace MLDB


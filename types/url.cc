// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


#include "url.h"

#include "mldb/ext/googleurl/src/gurl.h"
#include "mldb/ext/googleurl/src/url_util.h"
#include "mldb/arch/exception.h"
#include "mldb/types/string.h"
#include "value_description.h"

#include <iostream>

using namespace std;

namespace Datacratic {


namespace {

struct Init {
    Init()
    {
        url_util::Initialize();
    }
} init;

}

Url::
Url()
    : url(new GURL())
{
}

Url::
Url(const std::string & s_)
    : original(s_)
{
    init(original);
}

Url::
Url(const char * s_)
    : original(s_)
{
    init(original);
}

Url::
Url(const Utf8String & s_)
    : original(s_.rawString())
{
    init(original);
}

Url::
Url(const Utf32String & s_)
        : original(s_.utf8String())
{
    init(original);
}

void
Url::init(std::string s)
{
    // URLs like file://filename.txt which reference a file in the CWD come out
    // as file://filename.txt/ because GURL requires everything to have a path.
    // We fix it by inserting "./" into the path.  Still is an unhandled corner
    // case for where the filename itself has a slash in it, but I'm not sure
    // that can be addressed via a URL anyway since there's no way to escape
    // separators.

    if (s.find("file://") == 0
        && s.find('/', 7) == string::npos) {
        // file URI with no path, just a filename
        // insert a ./ path
        s.insert(7, "./");
    }

    if (s == "") {
        url.reset(new GURL(s));
        return;
    }

    if (s.find("://") == string::npos) {
        throw ML::Exception("Attempt to create a URL without a scheme: if you mean http:// or file:// then add it explicitly: " + s);
        //s = "http://" + s;
    }
    url.reset(new GURL(s));

    if (url->possibly_invalid_spec().empty()) {
        //cerr << "bad parse 1" << endl;
        url.reset(new GURL("http://" + s));
        if (url->possibly_invalid_spec().empty()) {
            //cerr << "bad parse 2" << endl;
            url.reset(new GURL("http://" + s + "/"));
        }
    }
}

Url::
~Url()
{
}

std::string
Url::
toString() const
{
    if (valid())
        return canonical();
    return original;
}

Utf8String
Url::
toUtf8String() const
{
    if (valid())
        return Utf8String(canonical());
    return Utf8String(original);
}

const char *
Url::
c_str() const
{
    if (valid())
        return url->spec().c_str();
    return original.c_str();
}

bool
Url::
valid() const
{
    return url->is_valid();
}

bool
Url::
empty() const
{
    return url->is_empty();
}

std::string
Url::
canonical() const
{
    if (!valid()) return "";
    return url->spec();
}

std::string
Url::
scheme() const
{
    return url->scheme();
}

std::string
Url::
username() const
{
    return url->username();
}

std::string
Url::
password() const
{
    return url->password();
}

std::string
Url::
host() const
{
    return url->host();
}

bool
Url::
hostIsIpAddress() const
{
    return url->HostIsIPAddress();
}

bool
Url::
domainMatches(const std::string & str) const
{
    return url->DomainIs(str.c_str(), str.length());
}

int
Url::
port() const
{
    return url->IntPort();
}

std::string
Url::
path() const
{
    if (url->scheme() == "file") {
        return string(original, 7);  // truncate "file://"
        if (url->path() != "/")
            return url->host() + url->path();
        else return url->host();
    }
    else return url->path();
}

std::string
Url::
query() const
{
    return url->query();
}

uint64_t
Url::
urlHash()
{
    throw ML::Exception("urlHash");
}

uint64_t
Url::
hostHash()
{
    throw ML::Exception("hostHash");
}

#if 0
void
Url::
serialize(ML::DB::Store_Writer & store) const
{
    unsigned char version = 0;
    store << version << original;
}

void
Url::
reconstitute(ML::DB::Store_Reader & store)
{
    unsigned char version;
    store >> version;
    if (version != 0)
        store >> original;
    *this = Url(original);
}
#endif

Utf8String
Url::
decodeUri(Utf8String in)
{
    Utf8String out;
    char high;
    char low;
    char buffer[5]; // utf-8 has at most 4 bytes + \0
    for (Utf8String::iterator it = in.find('%'); it != in.end();
            it = in.find('%'))
    {
        if (it != in.begin()) {
            // copy prior part to out
            out += Utf8String(in.begin(), it);
        }

        int bufferIndex = 0;
        int remaining = 1;
        while (it != in.end() && *it == '%' && remaining) {

            ++it; // over high
            if (it == in.end() || !isxdigit(*it)) {
                throw ML::Exception("Invalid uri"); // Todo better msg?
            }
            high = *it;
            high -= high <= '9' ? '0' : (high <= 'F' ? 'A' : 'a') - 10;

            ++it; // over low
            if (it == in.end() || !isxdigit(*it)) {
                throw ML::Exception("Invalid uri"); // Todo better msg?
            }
            low = *it;
            low -= low <= '9' ? '0' : (low <= 'F' ? 'A' : 'a') - 10;

            buffer[bufferIndex] = 16 * high + low;
            if (bufferIndex == 0) {
                char c = buffer[0] << 1;
                while (c < 0) {
                    c = c << 1;
                    ++remaining;
                }
            }
            --remaining;
            ++bufferIndex;

            if (it != in.end()) {
                ++it; // past low
            }
        }

        in.erase(in.begin(), it);
        it = in.begin();
        buffer[bufferIndex] = '\0';
        out += Utf8String(buffer);
    }
    if (in.begin() != in.end()) {
        out += Utf8String(in.begin(), in.end());
    }
    return out;
}


/*****************************************************************************/
/* VALUE DESCRIPTION                                                         */
/*****************************************************************************/

std::string & getUrlDocumentationUri()
{
    static std::string result;
    return result;
}

struct UrlDescription
    : public ValueDescriptionI<Url, ValueKind::ATOM, UrlDescription> {


    UrlDescription();

    virtual void parseJsonTyped(Url * val,
                                JsonParsingContext & context) const;
    virtual void printJsonTyped(const Url * val,
                                JsonPrintingContext & context) const;
    virtual bool isDefaultTyped(const Url * val) const;
};

extern template class ValueDescriptionT<Datacratic::Url>;
extern template class ValueDescriptionI<Datacratic::Url, ValueKind::ATOM, UrlDescription>;

DEFINE_VALUE_DESCRIPTION(Url, UrlDescription);

UrlDescription::
UrlDescription()
{
    this->documentationUri = getUrlDocumentationUri();
}

void
UrlDescription::
parseJsonTyped(Url * val,
                            JsonParsingContext & context) const
{
    *val = Url(context.expectStringUtf8());
}

void
UrlDescription::
printJsonTyped(const Url * val,
                            JsonPrintingContext & context) const
{
    // Write it back exactly the same way it came in
    context.writeStringUtf8(Utf8String(val->original));
}

bool
UrlDescription::
isDefaultTyped(const Url * val) const
{
    return val->empty();
}

void setUrlDocumentationUri(const std::string & newUri)
{
    getUrlDocumentationUri() = newUri;
    ((UrlDescription *)(getDefaultDescriptionSharedT<Url>().get()))
        ->documentationUri = "/doc/builtin/Url.md";
}

template class ValueDescriptionI<Datacratic::Url, ValueKind::ATOM, UrlDescription>;

} // namespace Datacratic

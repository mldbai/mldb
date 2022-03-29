// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* any.cc
   Jeremy Barnes, 18 June 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Implementation of the Any class.
*/

#include "any.h"
#include "any_impl.h"
#include "mldb/types/structure_description.h"
#include "mldb/base/parse_context.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* ANY                                                                       */
/*****************************************************************************/

namespace {

std::shared_ptr<void>
initAnyFromJson(JsonParsingContext & context,
                const ValueDescription * desc)
{
    ExcAssert(desc);

    void * ptr = nullptr;
    if (desc->align < alignof(void *)) {
        ptr = malloc(desc->width);
        if (!ptr)
            throw MLDB::Exception(errno, "malloc");
    }
    else {
        int res = posix_memalign(&ptr, desc->align, desc->width);
        if (res != 0)
            throw MLDB::Exception(res, "posix_memalign");
    }

    try {
        desc->initializeDefault(ptr);
    } MLDB_CATCH_ALL {
        free(ptr);
        throw;
    }

    auto destruct = [desc] (void * ptr)
    {
        try {
            desc->destruct(ptr);
        } MLDB_CATCH_ALL {
            free(ptr);
            throw;
        }
        free(ptr);
    };

    std::shared_ptr<void> result(ptr, std::move(destruct));
    desc->parseJson(result.get(), context);
    return result;
}

} // file scope

/** Construct directly from Json, with a known type */
Any::
Any(const Json::Value & val,
    const ValueDescription * desc)
    : type_(desc->type),
      desc_(desc)
{
    StructuredJsonParsingContext context(val);
    obj_ = initAnyFromJson(context, desc);
}

Any::
Any(const std::string & jsonValString,
    const ValueDescription * desc)
    : type_(desc->type),
      desc_(desc)
{
    std::istringstream stream(jsonValString);
    StreamingJsonParsingContext context(jsonValString, stream);
    obj_ = initAnyFromJson(context, desc);
}

/** Get it as JSON */
Json::Value
Any::
asJson() const
{
    Json::Value result;
    if (!desc_) return result;
    StructuredJsonPrintingContext context(result);
    desc_->printJson(obj_.get(), context);
    return result;
}

/** Get it as stringified JSON */
std::string
Any::
asJsonStr() const
{
    if (!desc_)
        return "null";
    std::ostringstream stream;
    StreamJsonPrintingContext context(stream);
    desc_->printJson(obj_.get(), context);
    return stream.str();
}

void
Any::
setJson(const Json::Value & val)
{
    obj_.reset(new Json::Value(val));
    desc_ = nullptr;
    type_ = &typeid(Json::Value);
}

void
Any::
setJson(Json::Value && val)
{
    obj_.reset(new Json::Value(std::move(val)));
    desc_ = nullptr;
    type_ = &typeid(Json::Value);
}


Any
Any::
getField(const std::string & fieldName) const
{
    if (empty())
        return Any();
    if (is<Json::Value>()) {
        // Extract JSON directly
        const Json::Value & val = as<Json::Value>();
        Any result;
        if (val.isObject()) {
            result.type_ = type_;
            result.obj_ = shared_ptr<void>(obj_, (void *)(&val.atStr(fieldName)));
            result.desc_ = desc_;
        }
        return result;
    }
    else {
        // Use value description to get field
        ExcAssert(desc_);
        const ValueDescription::FieldDescription * field
            = desc_->hasField(obj_.get(), fieldName);
        Any result;
        if (field) {
            result.type_ = field->description->type;
            result.obj_ = shared_ptr<void>(obj_, (void *)(field->getFieldPtr(obj_.get())));
            result.desc_ = field->description.get();
        }
        return result;
    }
}

static TypedAnyDescription payloadDesc;

Any
Any::
jsonDecodeStrTyped(const std::string & json)
{
    std::istringstream stream(json);
    StreamingJsonParsingContext context(json, json.c_str(),
                                        json.c_str() + json.size());
    Any ev;
    payloadDesc.parseJsonTyped(&ev, context);

    return ev;
}

Any
Any::
jsonDecodeTyped(const Json::Value & json)
{
    StructuredJsonParsingContext context(json);
    Any ev;
    payloadDesc.parseJsonTyped(&ev, context);

    return ev;
}

std::string
Any::
jsonEncodeStrTyped(const Any & val)
{
    std::ostringstream stream;
    StreamJsonPrintingContext context(stream);
    payloadDesc.printJson(&val, context);
    return stream.str();
}

Json::Value
Any::
jsonEncodeTyped(const Any & val)
{
    Json::Value result;
    StructuredJsonPrintingContext context(result);
    payloadDesc.printJson(&val, context);
    return result;
}

void
Any::
throwNoValueDescription() const
{
    throw MLDB::Exception("Any had no type attached");
}

std::span<const std::byte>
Any::
asBytes() const
{
    return { (const std::byte *)obj_.get(), desc().width };
}

bool operator==(const Any & lhs, const Any & rhs)
{
    if (!lhs.desc_ || !rhs.desc_) {
        // we have no way to interpret the value - the best we can do is compare pointers
        return lhs.obj_ == rhs.obj_;
    }
    else if (typeid(lhs.desc_) != typeid(rhs.desc_)) {
        return false;
    }
    else if (lhs.desc_->hasEqualityComparison()) {
        return lhs.desc_->compareEquality(lhs.obj_.get(), rhs.obj_.get());
    }
    else {
        return lhs.asJsonStr() == rhs.asJsonStr();
    }
}

struct AnyRep {
    AnyRep()
        : repVersion(1)
    {
    }

    std::string typeName;
    int repVersion;
    std::string valueDescriptionType;
    std::string valueDescriptionVersion;
    std::string payload;  // JSON-encoded string
};

DEFINE_STRUCTURE_DESCRIPTION(AnyRep);

AnyRepDescription::
AnyRepDescription()
{
    addField("rv", &AnyRep::repVersion,
             "Version of representation");
    addField("tn", &AnyRep::typeName,
             "Type of object in payload");
    addField("vdt",
             &AnyRep::valueDescriptionType,
             "Type of value description that encoded the payload");
    addField("vdv",
             &AnyRep::valueDescriptionVersion,
             "Version of value description that encoded the payload");
    addField("p", &AnyRep::payload,
             "Payload of watch event (actual event that happened)");
}

void
TypedAnyDescription::
parseJsonTyped(Any * val,
               JsonParsingContext & context) const
{
    static AnyRepDescription repDesc;

    if (context.isNull()) {
        context.expectNull();
        *val = Any();
        return;
    }

    AnyRep rep;
    repDesc.parseJson(&rep, context);
        
    // Get the default description for the type
    auto desc = ValueDescription::get(rep.typeName);

    // Parse the payload
    StreamingJsonParsingContext pcontext(rep.payload,
                                         rep.payload.c_str(),
                                         rep.payload.size());
    auto obj = initAnyFromJson(pcontext, desc.get());

    val->obj_ = std::move(obj);
    val->desc_ = desc.get();
    val->type_ = desc->type;
}

void
TypedAnyDescription::
printJsonTyped(const Any * val,
                            JsonPrintingContext & context) const
{
    static AnyRepDescription desc;

    if (val->empty()) {
        context.writeNull();
        return;
    }

    AnyRep rep;
    rep.typeName = val->type().name();
    auto & vdesc = val->desc();
    rep.valueDescriptionType = typeid(vdesc).name();

    std::ostringstream stream;
    StreamJsonPrintingContext pcontext(stream);
        
    val->desc().printJson(val->obj_.get(), pcontext);
    rep.payload = stream.str();

    desc.printJson(&rep, context);
}

/** Alternative value description for Any that only prints the JSON,
    not the type information.
    
    When reconstituting, no type information is kept (only the
    Json::Value).  This means that it will need to be converted to
    the underlying type.
*/
void
BareAnyDescription::
parseJsonTyped(Any * val,
               JsonParsingContext & context) const
{
    Json::Value jsonVal = context.expectJson();
    val->setJson(jsonVal);
}

void
BareAnyDescription::
printJsonTyped(const Any * val,
               JsonPrintingContext & context) const
{
    if (val->empty()) {
        context.writeNull();
        return;
    }
    if (val->type() == typeid(Json::Value)) {
        auto j = val->as<Json::Value>();
        context.writeJson(j);
        return;
    }

    val->desc().printJson(val->obj_.get(), context);
}

bool
BareAnyDescription::
isDefaultTyped(const Any * val) const
{
    return val->empty();
}

DEFINE_VALUE_DESCRIPTION(Any, BareAnyDescription);

std::shared_ptr<ValueDescriptionT<Any> >
getBareAnyDescription()
{
    return std::make_shared<BareAnyDescription>();
}

} // namespace MLDB

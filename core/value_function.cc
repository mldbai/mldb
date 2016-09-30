/** value_function.cc                                             -*- C++ -*-
    Jeremy Barnes, 14 April 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

*/

#include "value_function.h"
#include "mldb/types/value_description.h"
#include "mldb/types/meta_value_description.h"
#include <unordered_map>


namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* VALUE FUNCTION                                                            */
/*****************************************************************************/

namespace {

struct FieldInfo {
    std::shared_ptr<ExpressionValueInfo> info;
    ValueDescription::FieldDescription desc;
    ValueFunction::FromInput fromInput;
    ValueFunction::ToOutput toOutput;
    PathElement fieldName;
};

std::tuple<std::shared_ptr<ExpressionValueInfo>,
           ValueFunction::FromInput,
           ValueFunction::ToOutput>
toValueInfo(std::shared_ptr<const ValueDescription> desc)
{
    ExcAssert(desc);
    typedef ValueFunction::FromInput FromInput;
    typedef ValueFunction::ToOutput ToOutput;

    switch (desc->kind) {
    case ValueKind::STRUCTURE: {
        std::vector<FieldInfo> fields;
        std::vector<KnownColumn> knownColumns;
        std::unordered_map<PathElement, int> columnToIndex;

        auto onField = [&] (const ValueDescription::FieldDescription & field)
            {
                FieldInfo info;
                info.fieldName = field.fieldName;
                info.desc = field;
                std::tie(info.info, info.fromInput, info.toOutput)
                    = toValueInfo(field.description);

                if (!info.info)
                    throw HttpReturnException
                            (500, "No info for field " + info.fieldName.toUtf8String());

                knownColumns.emplace_back(PathElement(field.fieldName),
                                          info.info,
                                          COLUMN_IS_SPARSE /* todo: optional? */
                                          /*, field.fieldNum */);
                if (!columnToIndex.emplace(info.fieldName, fields.size())
                    .second) {
                    throw HttpReturnException(500, "Structure has field twice");
                }
                fields.emplace_back(std::move(info));
            };
        
        // Since it's a structure, we don't need an actual instance of the
        // object to know what its fields are
        desc->forEachField(nullptr /* no object */, onField);
        
        auto info = std::make_shared<RowValueInfo>(std::move(knownColumns),
                                                   SCHEMA_CLOSED);
        
        // Function used to convert from an ExpressionValue to a binary
        // representation.  We go through each field in the ExpressionValue
        // and set the corresponding value.  Note that a default constructed
        // object is expected.
        auto fromInput = [=] (void * obj, const ExpressionValue & input)
            {
                auto onColumn = [&] (const PathElement & columnName,
                                     const ExpressionValue & val)
                {
                    auto it = columnToIndex.find(columnName);
                    if (it == columnToIndex.end()) {
                        throw HttpReturnException
                            (400, "Unknown field '" + columnName.toUtf8String()
                             + "' extracting type " + desc->typeName);
                    }

                    const FieldInfo & f = fields[it->second];

                    // Run the conversion recursively
                    f.fromInput(f.desc.getFieldPtr(obj),
                                input.getColumn(f.fieldName));

                    return true;
                };

                input.forEachColumn(onColumn);
            };

        // Function used to convert back from the binary value to the
        // ExpressionValue representation.
        auto toOutput = [=] (const void * obj) -> ExpressionValue
            {
                StructValue result;
                result.reserve(fields.size());
                
                for (auto & f: fields) {
                    result.emplace_back(f.fieldName,
                                        f.toOutput(f.desc.getFieldPtr(obj)));
                }

                return std::move(result);
            };

        return std::make_tuple(info, fromInput, toOutput);
    }

    case ValueKind::ATOM: {
        // First, check for ExpressionValue.  In that case, it's a trivial
        // transformation.
        
        if (*desc->type == typeid(ExpressionValue)) {
            auto info = extractExpressionValueInfo(desc);

            FromInput fromInput = [] (void * obj, const ExpressionValue & input)
                {
                    *static_cast<ExpressionValue *>(obj) = input;
                };
            
            ToOutput toOutput = [] (const void * obj) -> ExpressionValue
                {
                    return *static_cast<const ExpressionValue *>(obj);
                };
            
            return std::make_tuple(info, fromInput, toOutput);
        }
        else if (*desc->type == typeid(CellValue)) {
            auto info = std::make_shared<AtomValueInfo>();
            FromInput fromInput = [] (void * obj, const ExpressionValue & input)
                {
                    *static_cast<CellValue *>(obj) = input.getAtom();
                };
            
            ToOutput toOutput = [] (const void * obj) -> ExpressionValue
                {
                    return ExpressionValue(*static_cast<const CellValue *>(obj),
                                           Date::notADate());
                };
            
            return std::make_tuple(info, fromInput, toOutput);
        }
        break;
    }

    case ValueKind::STRING: {
        if (*desc->type == typeid(Utf8String)) {
            auto info = std::make_shared<Utf8StringValueInfo>();
            FromInput fromInput = [] (void * obj, const ExpressionValue & input)
                {
                    *static_cast<Utf8String *>(obj) = input.toUtf8String();
                };
            
            ToOutput toOutput = [] (const void * obj) -> ExpressionValue
                {
                    return ExpressionValue(*static_cast<const Utf8String *>(obj),
                                           Date::notADate());
                };
            return std::make_tuple(info, fromInput, toOutput);
        }
        else if (*desc->type == typeid(std::string)) {
            auto info = std::make_shared<StringValueInfo>();
            FromInput fromInput = [] (void * obj, const ExpressionValue & input)
                {
                    *static_cast<std::string *>(obj) = input.toString();
                };
            
            ToOutput toOutput = [] (const void * obj) -> ExpressionValue
                {
                    return ExpressionValue(*static_cast<const std::string *>(obj),
                                           Date::notADate());
                };
            return std::make_tuple(info, fromInput, toOutput);
        }
        else {
            auto info = std::make_shared<Utf8StringValueInfo>();
            FromInput fromInput = [desc] (void * obj, const ExpressionValue & input)
                {
                    Json::Value val(input.toUtf8String());
                    StructuredJsonParsingContext context(val);
                    desc->parseJson(obj, context);
                };
            
            ToOutput toOutput = [desc] (const void * obj) -> ExpressionValue
                {
                    Json::Value val;
                    StructuredJsonPrintingContext context(val);
                    desc->printJson(obj, context);
                    return ExpressionValue(Utf8String(val.toString()),
                                           Date::notADate());
                };
            return std::make_tuple(info, fromInput, toOutput);
            
        }
    }

    case ValueKind::INTEGER:
    case ValueKind::FLOAT:
    case ValueKind::BOOLEAN:
    case ValueKind::ENUM:
    case ValueKind::OPTIONAL:
    case ValueKind::LINK:
    case ValueKind::ARRAY:
    case ValueKind::TUPLE:
    case ValueKind::VARIANT:
    case ValueKind::MAP:
    case ValueKind::ANY: {
        // Go through JSON
        auto info = std::make_shared<AnyValueInfo>();

        FromInput fromInput = [desc] (void * obj, const ExpressionValue & input)
            {
                Json::Value val = input.extractJson();
                StructuredJsonParsingContext context(val);
                desc->parseJson(obj, context);
            };
            
        ToOutput toOutput = [desc] (const void * obj) -> ExpressionValue
            {
                Json::Value val;
                StructuredJsonPrintingContext context(val);
                desc->printJson(obj, context);
                return ExpressionValue(val, Date::notADate());
            };
        return std::make_tuple(info, fromInput, toOutput);
    }
    }

    throw HttpReturnException(500, "Can't convert value info of this kind: "
                              + jsonEncodeStr(desc->kind) + " "
                              + desc->typeName);
}

} // file scope

ValueFunction::
ValueFunction(MldbServer * server,
              std::shared_ptr<const ValueDescription> inputDescription,
              std::shared_ptr<const ValueDescription> outputDescription)
    : Function(server),
      inputDescription(std::move(inputDescription)),
      outputDescription(std::move(outputDescription))
{
    std::tie(inputInfo, fromInput, std::ignore)
        = toValueInfo(this->inputDescription);
    std::tie(outputInfo, std::ignore, toOutput)
        = toValueInfo(this->outputDescription);
}
    
Any
ValueFunction::
getStatus() const
{
    return Any();
}

FunctionInfo
ValueFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    result.input = ExpressionValueInfo::toRow(this->inputInfo);
    result.output = ExpressionValueInfo::toRow(this->outputInfo);
    return result;
}

} // namespace MLDB
} // namespace Datacratic

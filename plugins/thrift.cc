/** thrift.cc
    Jeremy Barnes, 30 May 2017

    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#include "thrift.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/basic_value_descriptions.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* THRIFT                                                                    */
/*****************************************************************************/

uint64_t thriftCompactDeserializeUnsigned(std::istream & stream)
{
    int c;

    uint64_t result = 0;

    int numValues = 0;
    do {
        c = stream.get();
        if (c == EOF) {
            throw HttpReturnException(400, "Error deserializing Thrift integer");
        }
        result = result | (uint64_t((c & 127)) << (numValues * 7));
        ++numValues;
    }
    while (c & 128);

    return result;
}

int64_t thriftCompactDeserializeSigned(std::istream & stream)
{
    return zigZagToLong(thriftCompactDeserializeUnsigned(stream));
}

float thriftCompactDeserializeFloat(std::istream & stream)
{
    static constexpr size_t FLOAT_BYTES = 4;

    char b[FLOAT_BYTES];
    stream.read(b, FLOAT_BYTES);

    if (!stream) {
        throw HttpReturnException(400, "Error reading Thrift float");
    }

    float_le val;
    std::memcpy(&val, b, FLOAT_BYTES);
    
    return val;
}

double thriftCompactDeserializeDouble(std::istream & stream)
{
    static constexpr size_t DOUBLE_BYTES = 8;

    char b[DOUBLE_BYTES];
    stream.read(b, DOUBLE_BYTES);

    if (!stream) {
        throw HttpReturnException(400, "Error reading Thrift double");
    }

    double_le val;
    std::memcpy(&val, b, DOUBLE_BYTES);
    
    return val;
}

bool thriftCompactDeserializeBoolean(std::istream & stream)
{
    static constexpr size_t BOOLEAN_BYTES = 1;

    char b[BOOLEAN_BYTES];
    stream.read(b, BOOLEAN_BYTES);

    if (!stream) {
        throw HttpReturnException(400, "Error reading Thrift boolean");
    }

    return b[0];
}

std::string thriftCompactDeserializeString(std::istream & stream)
{
    uint64_t length = thriftCompactDeserializeUnsigned(stream);

    std::string result(length, '\0');

    stream.read(&result[0], length);

    if (!stream) {
        throw HttpReturnException(400, "Error reading Thrift string");
    }

    return result;
}

DEFINE_ENUM_DESCRIPTION(ThriftStructureFieldType);

ThriftStructureFieldTypeDescription::
ThriftStructureFieldTypeDescription()
{
    addValue("BOOLEAN_TRUE", ThriftStructureFieldType::BOOLEAN_TRUE);
    addValue("BOOLEAN_FALSE", ThriftStructureFieldType::BOOLEAN_FALSE);
    addValue("BYTE", ThriftStructureFieldType::BYTE);
    addValue("I16", ThriftStructureFieldType::I16);
    addValue("I32", ThriftStructureFieldType::I32);
    addValue("I64", ThriftStructureFieldType::I64);
    addValue("DOUBLE", ThriftStructureFieldType::DOUBLE);
    addValue("BINARY", ThriftStructureFieldType::BINARY);
    addValue("LIST", ThriftStructureFieldType::LIST);
    addValue("SET", ThriftStructureFieldType::SET);
    addValue("MAP", ThriftStructureFieldType::MAP);
    addValue("STRUCT", ThriftStructureFieldType::STRUCT);
}

DEFINE_ENUM_DESCRIPTION(ThriftArrayFieldType)

ThriftArrayFieldTypeDescription::
ThriftArrayFieldTypeDescription()
{
    addValue("BOOL", ThriftArrayFieldType::BOOL);
    addValue("BYTE", ThriftArrayFieldType::BYTE);
    addValue("DOUBLE", ThriftArrayFieldType::DOUBLE);
    addValue("I16", ThriftArrayFieldType::I16);
    addValue("I32", ThriftArrayFieldType::I32);
    addValue("I64", ThriftArrayFieldType::I64);
    addValue("STRING", ThriftArrayFieldType::STRING);
    addValue("STRUCT", ThriftArrayFieldType::STRUCT);
    addValue("MAP", ThriftArrayFieldType::MAP);
    addValue("SET", ThriftArrayFieldType::SET);
    addValue("LIST", ThriftArrayFieldType::LIST);
}

ThriftDeserializer
createThriftDeserializer(std::shared_ptr<const ValueDescription> desc,
                         ThriftSerializationProtocol protocol)
{
    switch (desc->kind) {
    case ValueKind::INTEGER:
        return [=] (void * obj, std::istream & stream)
            {
                int64_t val = thriftCompactDeserializeSigned(stream);
                Json::Value j = val;
                StructuredJsonParsingContext context(j);
                desc->parseJson(obj, context);
            };

    case ValueKind::FLOAT:
        if (*desc->type == typeid(float)) {
            return [=] (void * obj, std::istream & stream)
                {
                    float val = thriftCompactDeserializeFloat(stream);
                    Json::Value j = val;
                    StructuredJsonParsingContext context(j);
                    desc->parseJson(obj, context);
                };
        }
        else if (*desc->type == typeid(double)) {
            return [=] (void * obj, std::istream & stream)
                {
                    float val = thriftCompactDeserializeDouble(stream);
                    Json::Value j = val;
                    StructuredJsonParsingContext context(j);
                    desc->parseJson(obj, context);
                };
        }

    case ValueKind::BOOLEAN:
        return [=] (void * obj, std::istream & stream)
            {
                bool val = thriftCompactDeserializeBoolean(stream);
                Json::Value j = val;
                StructuredJsonParsingContext context(j);
                desc->parseJson(obj, context);
            };
        
    case ValueKind::STRING:
        return [=] (void * obj, std::istream & stream)
            {
                std::string val = thriftCompactDeserializeString(stream);
                Json::Value j = val;
                StructuredJsonParsingContext context(j);
                desc->parseJson(obj, context);
            };

    case ValueKind::ENUM:
        return [=] (void * obj, std::istream & stream)
            {
                int64_t val = thriftCompactDeserializeSigned(stream);
                Json::Value j = val;
                StructuredJsonParsingContext context(j);
                desc->parseJson(obj, context);
            };

    case ValueKind::STRUCTURE: {
        // Create a deserializer for each field in the structure
        std::vector<ThriftDeserializer> fieldDeserializers;
        //(desc->getFieldCount(nullptr));
        
        auto onField = [&] (const ValueDescription::FieldDescription & field)
            {
                fieldDeserializers.emplace_back
                    (createThriftDeserializer(field.description, protocol));
                //cerr << "getting field deserializer for "
                //     << field.fieldName << " at index "
                //     << fieldDeserializers.size() << endl;
            };
        desc->forEachField(nullptr, onField);

        return [=] (void * obj, std::istream & stream)
            {
                desc->setDefault(obj);
                int fieldId = 0;

                for (;;) {
                    int c = stream.get();
                    if (c == EOF) {
                        throw HttpReturnException
                            (400, "Invalid Thrift structure");
                    }
                    
                    if (c == 0)
                        break;  // finished

                    ThriftStructureFieldType type
                        = static_cast<ThriftStructureFieldType>(c & 15);
                    unsigned delta = c >> 4;

                    //cerr << "delta = " << delta << " type = "
                    //     << jsonEncodeStr(type)
                    //     << endl;
                    
                    fieldId += delta;
                    if (delta == 0) {
                        fieldId += thriftCompactDeserializeSigned(stream);
                    }

                    if (fieldId > fieldDeserializers.size()) {
                        // TODO: skip field (but this needs to be
                        // deserialized anyhow)

                        cerr << "fieldId = " << fieldId << " of structure "
                             << desc->typeName << endl;
                        
                    }

                    const ValueDescription::FieldDescription * fieldDesc
                        = nullptr;
                    try {
                        fieldDesc = &desc->getFieldByNumber(fieldId - 1);
                    } catch (const std::exception & exc) {
                        rethrowHttpException
                            (-1, "Reading field " + std::to_string(fieldId)
                             + " of structure " + desc->typeName + ": "
                             + getExceptionString());
                    }
                    void * fieldPtr = fieldDesc->getFieldPtr(obj);
                    Json::Value j;

                    switch (type) {
                    case ThriftStructureFieldType::BOOLEAN_TRUE:
                    case ThriftStructureFieldType::BOOLEAN_FALSE:
                        j = type == ThriftStructureFieldType::BOOLEAN_TRUE;
                        break;

                    case ThriftStructureFieldType::BYTE:
                        if ((j = stream.get()) == EOF) {
                            throw HttpReturnException
                                (400, "Invalid Thrift byte");
                        }
                        break;

                    case ThriftStructureFieldType::I16:
                    case ThriftStructureFieldType::I32:
                    case ThriftStructureFieldType::I64:
                    case ThriftStructureFieldType::DOUBLE:
                    case ThriftStructureFieldType::BINARY:
                    case ThriftStructureFieldType::LIST:
                    case ThriftStructureFieldType::SET:
                    case ThriftStructureFieldType::MAP:
                    case ThriftStructureFieldType::STRUCT: {
                        fieldDeserializers.at(fieldId - 1)(fieldPtr, stream);
#if 0
                        Json::Value v;
                        StructuredJsonPrintingContext context(v);
                        fieldDesc->description->printJson(fieldPtr, context);
                        cerr << "setting field " << fieldDesc->fieldName
                             << " to " << jsonEncodeStr(v) << endl;
#endif
                        continue;
                    }
                    default:
                        throw HttpReturnException(600, "Not implemented yet");
                    }

                    StructuredJsonParsingContext context(j);
                    cerr << "setting field " << fieldDesc->fieldName
                         << " to " << jsonEncodeStr(j) << endl;
                    fieldDesc->description
                        ->parseJson(fieldPtr, context);
                }
            };
    }

    case ValueKind::ARRAY: {
        auto valueDeserializer = createThriftDeserializer(desc->containedPtr(),
                                                          protocol);
        
        return [=] (void * obj, std::istream & stream)
            {
                int c = stream.get();
                if (c == EOF) {
                    throw HttpReturnException
                        (400, "Invalid Thrift array");
                }
                    
                //ThriftArrayFieldType type = (ThriftArrayFieldType)(c & 15);
                unsigned numElements = c >> 4;

                if (numElements == 15) {
                    //cerr << "re-deserializing numElements" << endl;
                    numElements = thriftCompactDeserializeUnsigned(stream);
                }

                //cerr << "array has " << numElements
                //     << " elements of type " << jsonEncodeStr(type) << endl;

                desc->setArrayLength(obj, numElements);

                for (size_t i = 0;  i < numElements;  ++i) {
                    valueDeserializer(desc->getArrayElement(obj, i), stream);
                }
            };
    }

    case ValueKind::OPTIONAL: {
        auto valueDeserializer = createThriftDeserializer(desc->containedPtr(),
                                                          protocol);
        return [=] (void * obj, std::istream & stream)
            {
                void * contained
                    = desc->optionalMakeValue(obj);
                valueDeserializer(contained, stream);
            };
    }

    case ValueKind::ATOM:
    case ValueKind::LINK:
    case ValueKind::TUPLE:
    case ValueKind::VARIANT:
    case ValueKind::MAP:
    case ValueKind::ANY:
        break;
    }
    throw HttpReturnException
        (500, "Can't deserialize this type with Thrift: " + desc->typeName);
}

} // namespace MLDB


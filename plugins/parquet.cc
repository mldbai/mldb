/** parquet.h                                                       -*- C++ -*-
    Jeremy Barnes, 30 May 2017

    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

    Definitions to allow us to understand the Thrift serialization protocol.

    Not a complete implementation; being filled in as required.
*/

#include "parquet.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/optional_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/sql/expression_value.h"
#include "mldb/arch/bit_range_ops.h"
#include <iostream>

#define DEFINE_AND_FILLIN_ENUM_DESCRIPTION(T) \
    DEFINE_ENUM_DESCRIPTION(T); \
    T##Description::T##Description()

#define DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(T) \
    DEFINE_STRUCTURE_DESCRIPTION(T); \
    T##Description::T##Description()

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

void throwParquetException(const std::string & error)
{
    throw HttpReturnException(400, error);
}

DEFINE_AND_FILLIN_ENUM_DESCRIPTION(ParquetType)
{
    addValue("NONE", PQT_NONE);
    addValue("BOOLEAN", PQT_BOOLEAN);
    addValue("INT32", PQT_INT32);
    addValue("INT64", PQT_INT64);
    addValue("INT96", PQT_INT96);
    addValue("FLOAT", PQT_FLOAT);
    addValue("DOUBLE", PQT_DOUBLE);
    addValue("BYTE_ARRAY", PQT_BYTE_ARRAY);
    addValue("FIXED_LEN_BYTE_ARRAY", PQT_FIXED_LEN_BYTE_ARRAY);
}

DEFINE_AND_FILLIN_ENUM_DESCRIPTION(ParquetConvertedType)
{
    addValue("NONE", ParquetConvertedType::NONE);
    addValue("UTF8", ParquetConvertedType::UTF8);
    addValue("MAP", ParquetConvertedType::MAP);
    addValue("MAP_KEY_VALUE", ParquetConvertedType::MAP_KEY_VALUE);
    addValue("LIST", ParquetConvertedType::LIST);
    addValue("ENUM", ParquetConvertedType::ENUM);
    addValue("DECIMAL", ParquetConvertedType::DECIMAL);
    addValue("DATE", ParquetConvertedType::DATE);
    addValue("TIME_MILLIS", ParquetConvertedType::TIME_MILLIS);
    addValue("TIME_MICROS", ParquetConvertedType::TIME_MICROS);
    addValue("TIMESTAMP_MILLIS", ParquetConvertedType::TIMESTAMP_MILLIS);
    addValue("TIMESTAMP_MICROS", ParquetConvertedType::TIMESTAMP_MICROS);
    addValue("UINT_8", ParquetConvertedType::UINT_8);
    addValue("UINT_16", ParquetConvertedType::UINT_16);
    addValue("UINT_32", ParquetConvertedType::UINT_32);
    addValue("UINT_64", ParquetConvertedType::UINT_64);
    addValue("INT_8", ParquetConvertedType::INT_8);
    addValue("INT_16", ParquetConvertedType::INT_16);
    addValue("INT_32", ParquetConvertedType::INT_32);
    addValue("INT_64", ParquetConvertedType::INT_64);
    addValue("JSON", ParquetConvertedType::JSON);
    addValue("BSON", ParquetConvertedType::BSON);
    addValue("INTERVAL", ParquetConvertedType::INTERVAL);
    addValue("NA", ParquetConvertedType::NA);
}

DEFINE_AND_FILLIN_ENUM_DESCRIPTION(ParquetFieldRepetitionType)
{
    addValue("REQUIRED", PQT_REQUIRED);
    addValue("OPTIONAL", PQT_OPTIONAL);
    addValue("REPEATED", PQT_REPEATED);
}

DEFINE_AND_FILLIN_ENUM_DESCRIPTION(ParquetEncoding)
{
    addValue("PLAIN", PQT_PLAIN);
    addValue("GROUP_VAR_INT", PQT_GROUP_VAR_INT);
    addValue("PLAIN_DICTIONARY", PQT_PLAIN_DICTIONARY);
    addValue("RLE", PQT_RLE);
    addValue("BIT_PACKED", PQT_BIT_PACKED);
    addValue("DELTA_BINARY_PACKED", PQT_DELTA_BINARY_PACKED);
    addValue("DELTA_LENGTH_BYTE_ARRAY", PQT_DELTA_LENGTH_BYTE_ARRAY);
    addValue("DELTA_BYTE_ARRAY", PQT_DELTA_BYTE_ARRAY);
    addValue("RLE_DICTIONARY", PQT_RLE_DICTIONARY);
}

DEFINE_AND_FILLIN_ENUM_DESCRIPTION(ParquetCompressionCodec)
{
    addValue("UNCOMPRESSED", PQT_UNCOMPRESSED);
    addValue("SNAPPY", PQT_SNAPPY);
    addValue("GZIP", PQT_GZIP);
    addValue("LZO", PQT_LZO);
    addValue("BROTLI", PQT_BROTLI);
}

DEFINE_AND_FILLIN_ENUM_DESCRIPTION(ParquetPageType)
{
    addValue("DATA_PAGE", PQT_DATA_PAGE);
    addValue("INDEX_PAGE", PQT_INDEX_PAGE);
    addValue("DICTIONARY_PAGE", PQT_DICTIONARY_PAGE);
    addValue("DATA_PAGE_V2", PQT_DATA_PAGE_V2);
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetSchemaElement)
{
    addField("type", &ParquetSchemaElement::type, "");
    addAuto("type_length", &ParquetSchemaElement::type_length, "");
    addField("repetition_type", &ParquetSchemaElement::repetition_type, "");
    addField("name", &ParquetSchemaElement::name, "");
    addAuto("num_children", &ParquetSchemaElement::num_children, "");
    addField("converted_type", &ParquetSchemaElement::converted_type, "");
    addAuto("scale", &ParquetSchemaElement::scale, "");
    addAuto("precision", &ParquetSchemaElement::precision, "");
    addAuto("field_id", &ParquetSchemaElement::field_id, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetKeyValue)
{
    addField("key", &ParquetKeyValue::key, "");
    addField("value", &ParquetKeyValue::value, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetStatistics)
{
    addField("max", &ParquetStatistics::max, "");
    addField("min", &ParquetStatistics::min, "");
    addAuto("null_count", &ParquetStatistics::null_count, "");
    addAuto("distict_count", &ParquetStatistics::distinct_count, "");
    addField("max_value", &ParquetStatistics::max_value, "");
    addField("min_value", &ParquetStatistics::min_value, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetPageEncodingStats)
{
    addField("page_type", &ParquetPageEncodingStats::page_type, "");
    addField("encoding", &ParquetPageEncodingStats::encoding, "");
    addField("count", &ParquetPageEncodingStats::count, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetColumnMetaData)
{
    addField("type", &ParquetColumnMetaData::type, "");
    addField("encodings", &ParquetColumnMetaData::encodings, "");
    addField("path_in_schema", &ParquetColumnMetaData::path_in_schema, "");
    addField("codec", &ParquetColumnMetaData::codec, "");
    addField("num_values", &ParquetColumnMetaData::num_values, "");
    addField("total_uncompressed_size",
             &ParquetColumnMetaData::total_uncompressed_size, "");
    addField("total_compressed_size",
             &ParquetColumnMetaData::total_compressed_size, "");
    addField("key_value_metadata",
             &ParquetColumnMetaData::key_value_metadata, "");
    addField("data_page_offset",
             &ParquetColumnMetaData::data_page_offset, "");
    addField("index_page_offset",
             &ParquetColumnMetaData::index_page_offset, "");
    addField("dictionary_page_offset",
             &ParquetColumnMetaData::dictionary_page_offset, "");
    addField("statistics",
             &ParquetColumnMetaData::statistics, "");
    addField("encoding_stats",
             &ParquetColumnMetaData::encoding_stats, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetColumnChunk)
{
    addField("file_path", &ParquetColumnChunk::file_path, "");
    addField("file_offset", &ParquetColumnChunk::file_offset, "");
    addField("meta_data", &ParquetColumnChunk::meta_data, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetRowGroup)
{
    addField("columns", &ParquetRowGroup::columns, "");
    addField("total_byte_size", &ParquetRowGroup::total_byte_size, "");
    addField("num_rows", &ParquetRowGroup::num_rows, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetFileMetaData)
{
    addField("version", &ParquetFileMetaData::version, "");
    addField("schema", &ParquetFileMetaData::schema, "");
    addField("num_rows", &ParquetFileMetaData::num_rows, "");
    addField("row_groups", &ParquetFileMetaData::row_groups, "");
    addField("key_value_metadata",
             &ParquetFileMetaData::key_value_metadata, "");
    addField("created_by", &ParquetFileMetaData::created_by, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetDataPageHeader)
{
    addField("num_values", &ParquetDataPageHeader::num_values, "");
    addField("encoding", &ParquetDataPageHeader::encoding, "");
    addField("definition_level_encoding",
             &ParquetDataPageHeader::definition_level_encoding, "");
    addField("repetition_level_encoding",
             &ParquetDataPageHeader::repetition_level_encoding, "");
    addField("statistics",
             &ParquetDataPageHeader::statistics, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetDataPageHeaderV2)
{
    addField("num_values", &ParquetDataPageHeaderV2::num_values, "");
    addField("num_nulls", &ParquetDataPageHeaderV2::num_nulls, "");
    addField("num_rows", &ParquetDataPageHeaderV2::num_rows, "");
    addField("encoding", &ParquetDataPageHeaderV2::encoding, "");
    addField("definition_levels_byte_length",
             &ParquetDataPageHeaderV2::definition_levels_byte_length, "");
    addField("repetition_levels_byte_length",
             &ParquetDataPageHeaderV2::repetition_levels_byte_length, "");
    addField("is_compressed",
             &ParquetDataPageHeaderV2::is_compressed, "");
    addField("statistics",
             &ParquetDataPageHeaderV2::statistics, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetIndexPageHeader)
{
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetDictionaryPageHeader)
{
    addField("num_values", &ParquetDictionaryPageHeader::num_values, "");
    addField("encoding", &ParquetDictionaryPageHeader::encoding, "");
    addField("is_sorted", &ParquetDictionaryPageHeader::is_sorted, "");
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetPageHeader)
{
    addField("type", &ParquetPageHeader::type, "");
    addField("uncompressed_page_size",
             &ParquetPageHeader::uncompressed_page_size, "");
    addField("compressed_page_size",
             &ParquetPageHeader::compressed_page_size, "");
    addField("crc",
             &ParquetPageHeader::crc, "");
    addField("data_page_header",
             &ParquetPageHeader::data_page_header, "");
    addField("index_page_header",
             &ParquetPageHeader::index_page_header, "");
    addField("dictionary_page_header",
             &ParquetPageHeader::dictionary_page_header, "");
    addField("data_page_header_v2",
             &ParquetPageHeader::data_page_header_v2, "");
}


/*****************************************************************************/
/* PARQUET STRUCTURED SCHEMA ELEMENT                                         */
/*****************************************************************************/

std::vector<ParquetStructuredSchemaElement>
ParquetStructuredSchemaElement::
process(std::vector<ParquetSchemaElement> & root,
        size_t & first, size_t last,
        int definition, int repetition)
{
    if (first == last) 
        return {};

    std::vector<ParquetStructuredSchemaElement> result;
    while (first < last) {
        size_t numChildren = root[first].num_children;
        ParquetStructuredSchemaElement element
            (std::move(root[first]));
        element.definition_level
            = definition + (element.repetition_type == PQT_OPTIONAL);
        element.repetition_level
            = repetition + (element.repetition_type == PQT_REPEATED);
            
        first += 1;
        element.children = process(root, first, first + numChildren,
                                   element.definition_level,
                                   element.repetition_level);
        result.emplace_back(std::move(element));
    }
    return result;
}

ParquetStructuredSchemaElement
ParquetStructuredSchemaElement::
process(std::vector<ParquetSchemaElement> root)
{
    size_t el = 0;
    auto results = process(root, el, root.size(),
                           0 /* definition */,
                           0 /* repetition */);
    if (results.size() != 1)
        throw HttpReturnException(400, "Invalid Parquet schema: unable to structure columns");
    return std::move(results[0]);
}

const ParquetStructuredSchemaElement &
ParquetStructuredSchemaElement::
lookup(const std::vector<std::string> & path,
       size_t prefix) const
{
    if (prefix == path.size())
        return *this;

    for (const auto & child: children) {
        if (child.name == path[prefix])
            return child.lookup(path, prefix + 1);
    }

    cerr << jsonEncode(*this) << endl;
    cerr << jsonEncodeStr(path) << endl;

    throw HttpReturnException(400, "Couldn't find Parquet schema element "
                              + jsonEncodeStr(path));
}

std::shared_ptr<ExpressionValueInfo>
ParquetStructuredSchemaElement::
getInfo() const
{
    if (num_children > 0 &&
        // structure is implicitly defined by having children and not
        // being a MAP type.
        (!converted_type || *converted_type != ParquetConvertedType::MAP)) {
        std::vector<KnownColumn> columns;
        for (auto & el: children) {
            columns.emplace_back(PathElement(el.name), el.getInfo(),
                                 el.repetition_type == PQT_REQUIRED
                                 ? COLUMN_IS_DENSE : COLUMN_IS_SPARSE);
        }
            
        return std::make_shared<RowValueInfo>(columns);
    }

    if (!converted_type) {
        switch (type) {
        case PQT_BOOLEAN:

        case PQT_INT32:
        case PQT_INT64:
            return std::make_shared<IntegerValueInfo>();

        case PQT_INT96:
            return std::make_shared<Float64ValueInfo>();

        case PQT_FLOAT:
            return std::make_shared<Float32ValueInfo>();
        case PQT_DOUBLE:
            return std::make_shared<Float64ValueInfo>();

        case PQT_BYTE_ARRAY:
        case PQT_FIXED_LEN_BYTE_ARRAY:
            return std::make_shared<BlobValueInfo>();

        case PQT_NONE:
            return std::make_shared<EmptyValueInfo>();
        }
            
        throw HttpReturnException(400, "Unknown Parquet converted type "
                                  + jsonEncodeStr(*converted_type));
        //cerr << "non-converted type" << endl;
        //return std::make_shared<AtomValueInfo>();
    }

    switch (*converted_type) {
    case ParquetConvertedType::NONE:
    case ParquetConvertedType::NA:
        return std::make_shared<EmptyValueInfo>();

    case ParquetConvertedType::UTF8:
        return std::make_shared<Utf8StringValueInfo>();

    case ParquetConvertedType::MAP: {
        // Maps should have a single "map" subelement with a
        // "key" and a "value" subelement.
        const ParquetStructuredSchemaElement & key
            = lookup({"map", "key"});
        const ParquetStructuredSchemaElement & value
            = lookup({"map", "value"});

        auto keyInfo = key.getInfo();
        auto valueInfo = value.getInfo();

        return makeMapValueInfo(keyInfo, valueInfo);
    }
            

    case ParquetConvertedType::MAP_KEY_VALUE:
        // This should be caught at a higher level with map
        // if it gets here it's a logic error
        break;

    case ParquetConvertedType::LIST: {
        //const ParquetStructuredSchemaElement & value
        //    = lookup({"list", "value"});
        //return makeArrayValueInfo(valueInfo);
        return std::make_shared<UnknownRowValueInfo>();
    }

    case ParquetConvertedType::ENUM:
        return std::make_shared<Utf8StringValueInfo>();

    case ParquetConvertedType::DECIMAL:
        return std::make_shared<Float64ValueInfo>();

    case ParquetConvertedType::DATE:
    case ParquetConvertedType::TIMESTAMP_MILLIS:
    case ParquetConvertedType::TIMESTAMP_MICROS:
        return std::make_shared<TimestampValueInfo>();

    case ParquetConvertedType::TIME_MILLIS:
    case ParquetConvertedType::TIME_MICROS:
    case ParquetConvertedType::INTERVAL:
        return std::make_shared<TimeIntervalValueInfo>();

    case ParquetConvertedType::UINT_8:
    case ParquetConvertedType::UINT_16:
    case ParquetConvertedType::UINT_32:
    case ParquetConvertedType::INT_8:
    case ParquetConvertedType::INT_16:
    case ParquetConvertedType::INT_32:
    case ParquetConvertedType::INT_64:
        return std::make_shared<IntegerValueInfo>();

    case ParquetConvertedType::UINT_64:
        return std::make_shared<Uint64ValueInfo>();

    case ParquetConvertedType::JSON:
    case ParquetConvertedType::BSON:
        return std::make_shared<AnyValueInfo>();
    }

    throw HttpReturnException(400, "Unknown Parquet converted type "
                              + jsonEncodeStr(*converted_type));
}

size_t
ParquetStructuredSchemaElement::
maxDepth() const
{
    size_t result = 0;
    for (auto & ch: children) {
        result = std::max(result, 1 + ch.maxDepth());
    }
    return result;
}

int
ParquetStructuredSchemaElement::
maxRepetitionLevel() const
{
    int result = repetition_level;
    for (auto & ch: children) {
        result = std::max(result, ch.maxRepetitionLevel());
    }
    return result;
}

int
ParquetStructuredSchemaElement::
maxDefinitionLevel() const
{
    int result = definition_level;
    for (auto & ch: children) {
        result = std::max(result, ch.maxDefinitionLevel());
    }
    return result;
}

DEFINE_AND_FILLIN_STRUCTURE_DESCRIPTION(ParquetStructuredSchemaElement)
{
    addParent<ParquetSchemaElement>();
    addField("children", &ParquetStructuredSchemaElement::children, "");
    addField("repetition_level",
             &ParquetStructuredSchemaElement::repetition_level, "");
    addField("definition_level",
             &ParquetStructuredSchemaElement::definition_level, "");
}

CellValue
readParquetPlain(const char * & p, const char * e,
                 ParquetType type,
                 const ParquetStructuredSchemaElement & schema)
{
    size_t fixedLength = schema.type_length;

    switch (type) {
    case PQT_BOOLEAN:
        throw HttpReturnException(500, "Cannot deserialize plain boolean");
    case PQT_INT32:
        return readParquetPlainT<int32_t>(p, e);
    case PQT_INT64:
        return readParquetPlainT<int64_t>(p, e);
    case PQT_INT96: {
        int32_t top = readParquetPlainT<int32_t>(p, e);
        uint32_t mid = readParquetPlainT<uint32_t>(p, e);
        uint32_t bottom = readParquetPlainT<uint32_t>(p, e);

        // TODO: negative
#if 0
        cerr << "top = " << MLDB::format("%08x", (unsigned)top)
             << endl;
        cerr << "mid = " << MLDB::format("%08x", (unsigned)mid)
             << endl;
        cerr << "bottom = " << MLDB::format("%08x", (unsigned)bottom)
             << endl;
#endif
        if (top == 0)
            return uint64_t(mid) << 32 | bottom;
        return pow(2.0, 64) * top + pow(2.0, 32) * mid + bottom;
    }
    case PQT_FLOAT:
        return readParquetPlainT<float>(p, e);
    case PQT_DOUBLE:
        return readParquetPlainT<double>(p, e);
    case PQT_BYTE_ARRAY:
        fixedLength = readParquetPlainT<int32_t>(p, e);
        // fall through
    case PQT_FIXED_LEN_BYTE_ARRAY:
        if (p + fixedLength > e)
            throw HttpReturnException(400, "Error deserializing Parquet plain");
        p += fixedLength;

        if (schema.converted_type) {
            switch (*schema.converted_type) {

            case ParquetConvertedType::UTF8:
                return CellValue(p - fixedLength, fixedLength);

            case ParquetConvertedType::DECIMAL:
                throw HttpReturnException
                    (500, "Cannot deserialize decimal Parquet type");

            case ParquetConvertedType::DATE:
            case ParquetConvertedType::TIMESTAMP_MILLIS:
            case ParquetConvertedType::TIMESTAMP_MICROS:
                throw HttpReturnException
                    (500, "Cannot deserialize timestamp Parquet type");

            case ParquetConvertedType::TIME_MILLIS:
            case ParquetConvertedType::TIME_MICROS:
            case ParquetConvertedType::INTERVAL:
                throw HttpReturnException
                    (500, "Cannot deserialize interval Parquet type");

            case ParquetConvertedType::UINT_8:
                return readParquetPlainAllT<uint8_t>(p - fixedLength, p);

            case ParquetConvertedType::UINT_16:
                return readParquetPlainAllT<uint16_t>(p - fixedLength, p);

            case ParquetConvertedType::UINT_32:
                return readParquetPlainAllT<uint32_t>(p - fixedLength, p);

            case ParquetConvertedType::UINT_64:
                return readParquetPlainAllT<uint64_t>(p - fixedLength, p);

            case ParquetConvertedType::INT_8:
                return readParquetPlainAllT<int8_t>(p - fixedLength, p);

            case ParquetConvertedType::INT_16:
                return readParquetPlainAllT<int16_t>(p - fixedLength, p);

            case ParquetConvertedType::INT_32:
                return readParquetPlainAllT<int32_t>(p - fixedLength, p);

            case ParquetConvertedType::INT_64:
                return readParquetPlainAllT<int64_t>(p - fixedLength, p);

            case ParquetConvertedType::NONE:
            case ParquetConvertedType::JSON:
            case ParquetConvertedType::NA:
            case ParquetConvertedType::MAP:
            case ParquetConvertedType::MAP_KEY_VALUE:
            case ParquetConvertedType::LIST:
            case ParquetConvertedType::ENUM:
            case ParquetConvertedType::BSON:
                throw HttpReturnException
                    (500, "Cannot deserialize complex Parquet type "
                     + jsonEncodeStr(type));

            }
        }   

        return CellValue::blob(p - fixedLength, fixedLength);

    case PQT_NONE:
        return CellValue();
    }
    
    throw HttpReturnException(500, "Cannot deserialize plain Parquet type");
}


/*****************************************************************************/
/* ARRAY DECODERS                                                            */
/*****************************************************************************/

struct ParquetPlainArrayReader: public ParquetIntegerArrayReader {

    ParquetPlainArrayReader(const char * & p,
                            const char * e,
                            size_t numEntries)
        : p(p), e(e), numEntries(numEntries)
    {
        throw HttpReturnException(500, "Not implemented");
    }

    virtual size_t generate(size_t n, uint32_t * out) override
    {
        throw HttpReturnException(500, "Not implemented");
    }

    const char * p;
    const char * e;
    size_t numEntries;
};

struct ParquetZeroArrayReader: public ParquetIntegerArrayReader {

    ParquetZeroArrayReader(const char * & p,
                           const char * e,
                           size_t numEntries)
        : p(p), e(e), numEntries(numEntries)
    {
        // We don't bother putting in length bytes
    }

    virtual size_t generate(size_t n, uint32_t * out) override
    {
        std::fill(out, out + n, 0);
        return n;
    }

    const char * p;
    const char * e;
    size_t numEntries;
};

struct ParquetBitPackedArrayReader: public ParquetIntegerArrayReader {

    ParquetBitPackedArrayReader(const char * & p,
                                const char * e,
                                size_t numEntries,
                                int numBits)
        : p(p), e(e), numEntries(numEntries), numBits(numBits)
    {
        p += (numEntries * numBits + 7) / 8;
    }

    virtual size_t generate(size_t n, uint32_t * out) override
    {
        for (size_t i = 0;  i < n;  ++i) {
            *out++ = generateOne();
        }
        return n;
    }

    uint32_t generateOne()
    {
        uint32_t current = 0;
        uint32_t bitsDone = 0;

        while (bitsDone < numBits) {
            int bitsToDo = std::min<int>(8 - bitsDoneInCurrent,
                                         bitsDone - numBits);
            uint8_t newBits = *p;
            newBits = newBits >> (8 - bitsDone - bitsToDo);
            newBits = newBits & ((1 << bitsToDo) - 1);
            current <<= bitsToDo;
            current |= newBits;

            bitsDone += bitsToDo;
            bitsDoneInCurrent += bitsToDo;
            if (bitsDoneInCurrent == 8) {
                p += 1;
                bitsDoneInCurrent = 0;
            }
        }
        
        return current;
    }

    const char * p;
    const char * e;
    size_t numEntries;
    int numBits;

    /// The number of bits we've done in the current one
    int bitsDoneInCurrent = 0;
};

struct ParquetRleArrayReader: public ParquetIntegerArrayReader {

    ParquetRleArrayReader(const char * & p,
                          const char * e,
                          size_t numEntries,
                          int numBits,
                          bool readNumBytes = true)
        : p(p), numEntries(numEntries), numBits(numBits)
    {
        if (readNumBytes) {
            numBytesEncodedData = readParquetPlainT<uint32_t>(p, e);
        }
        else {
            numBytesEncodedData = e - p;
        }
        cerr << "RLE array with " << numBytesEncodedData << " bytes" << endl;
        this->p = p;
        p += numBytesEncodedData;
        this->e = p;
        deserializeSome();
    }

    /*
      rle-bit-packed-hybrid: <length> <encoded-data>
      length := length of the <encoded-data> in bytes stored as 4 bytes little endian
      encoded-data := <run>*
      run := <bit-packed-run> | <rle-run>  
      bit-packed-run := <bit-packed-header> <bit-packed-values>  
      bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)  
      // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8  
      bit-pack-count := (number of values in this run) / 8  
      bit-packed-values := *see 1 below*  
      rle-run := <rle-header> <repeated-value>  
      rle-header := varint-encode( (number of times repeated) << 1)  
      repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
    */

    uint64_t deserializeVle()
    {
        uint64_t result = 0;

        unsigned c;

        int numValues = 0;
        do {
            if (p == e) {
                throw HttpReturnException
                    (400, "Error deserializing VLE Parquet RLE integer");
            }
            c = *(const unsigned char *)p++;
            cerr << "got char " << c << endl;
            result = result | (uint64_t((c & 127)) << (numValues * 7));
            ++numValues;
        }
        while (c & 128);
        
        return result;
    }
    
    void deserializeSome()
    {
        uint64_t header = deserializeVle();
        int indicator = header & 1;
        header >>= 1;
        if (indicator == 0) {
            deserializeRle(header);
        }
        else {
            deserializeBitPacked(header);
        }
    }

    void deserializeBitPacked(uint64_t numValues)
    {
        cerr << numValues << " bit packed values with " << numBits << " bits"
             << endl;
        numValues *= 8;
        ML::Bit_Extractor<uint32_t> vals((const uint32_t *)p);
        for (size_t i = 0;  i < numValues;  ++i) {
            addValue(vals.extract<uint32_t>(numBits));
        }
        p += numValues * numBits / 8;
    }

    void deserializeRle(uint64_t numRepeats)
    {
        size_t numBytes = (numBits + 7) / 8;
        uint32_t val = 0;
        for (size_t i = 0;  i < numBytes;  ++i) {
            val = val | ((*(const unsigned char *)p++) << (8 * i));
        }
        cerr << numRepeats << " RLE values of " << val << endl;
        for (size_t i = 0;  i < numRepeats;  ++i) {
            addValue(val);
        }
    }

    void addValue(uint32_t val)
    {
        values.push_back(val);
    }
    
    virtual size_t generate(size_t n, uint32_t * out) override
    {
        while (p < e && (values.size() - consumed) < n) {
            deserializeSome();
        }

        n = std::min(n, values.size() - consumed);
        std::copy(values.data() + consumed,
                  values.data() + consumed + n,
                  out);
        consumed += n;

        if (consumed == values.size()) {
            values.clear();
            consumed = 0;
        }
        
        return n;
    }

    const char * p;
    const char * e;
    size_t numEntries;
    int numBits;
    size_t numBytesEncodedData;

    std::vector<uint32_t> values;
    size_t consumed = 0;
};

std::unique_ptr<ParquetIntegerArrayReader>
createParquetRleDictionaryReader(const char * & p,
                                 const char * e,
                                 size_t numEntries,
                                 int numBits)
{
    return std::unique_ptr<ParquetIntegerArrayReader>
        (new ParquetRleArrayReader(p, e, numEntries, numBits,
                                   false /* read length from buffer */));
}


/*****************************************************************************/
/* PARQUET INTEGER ARRAY                                                     */
/*****************************************************************************/

ParquetIntegerArray::
ParquetIntegerArray(const char * & p, const char * e,
                    size_t numEntries,
                    ParquetEncoding encoding,
                    int numBits)
{
    if (numBits == 0) {
        reader.reset(new ParquetZeroArrayReader(p, e, numEntries));
    }
    else {
        switch (encoding) {
        case PQT_PLAIN:
            reader.reset
                (new ParquetPlainArrayReader(p, e, numEntries));
            break;
        case PQT_RLE:
            cerr << "rle with " << e - p << " bytes, " << numEntries
                 << " entries and " << numBits << " bits/entry" << endl;
            reader.reset
                (new ParquetRleArrayReader(p, e, numEntries, numBits));
            break;
        case PQT_BIT_PACKED:
            reader.reset
                (new ParquetBitPackedArrayReader(p, e, numEntries, numBits));
            break;
        case PQT_DELTA_BINARY_PACKED:
            //reader.reset
            //    (new ParquetDeltaBinaryPackedArrayReader(p, e, numEntries, numBits));
            //break;
        case PQT_DELTA_LENGTH_BYTE_ARRAY:
            //reader.reset
            //    (new ParquetDeltaLengthByteArrayReader(p, e, numEntries, numBits));
            //break;
        case PQT_DELTA_BYTE_ARRAY:
            //reader.reset
            //    (new ParquetDeltaStringsArrayReader(p, e, numEntries, numBits));
            //
            //break;

            // These are not applicable for integers
        case PQT_GROUP_VAR_INT:
        case PQT_PLAIN_DICTIONARY:
        case PQT_RLE_DICTIONARY:
            break;
        }
    }

    if (!reader) {
        throw HttpReturnException(400, "Unknown Parquet encoding "
                                  + jsonEncodeStr(encoding));
    }
}

} // namespace MLDB

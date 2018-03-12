/** parquet.h                                                       -*- C++ -*-
    Jeremy Barnes, 30 May 2017

    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

    Definitions to allow us to understand the Thrift serialization protocol.

    Not a complete implementation; being filled in as required.
*/

#pragma once

#include <vector>
#include <cstring>
#include <functional>

#include "mldb/types/value_description_fwd.h"
#include "mldb/arch/endian.h"
#include "mldb/compiler/compiler.h"
#include "mldb/types/optional.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

struct ExpressionValueInfo;
struct CellValue;

enum ParquetType {
    PQT_BOOLEAN,
    PQT_INT32,
    PQT_INT64,
    PQT_INT96,
    PQT_FLOAT,
    PQT_DOUBLE,
    PQT_BYTE_ARRAY,
    PQT_FIXED_LEN_BYTE_ARRAY,
    PQT_NONE = -1
};

DECLARE_ENUM_DESCRIPTION(ParquetType);

enum struct ParquetConvertedType: int32_t {
    NONE = -1,
    UTF8 = 0,
    MAP = 1,
    MAP_KEY_VALUE = 2,
    LIST = 3,
    ENUM = 4,
    DECIMAL = 5,
    DATE = 6,
    TIME_MILLIS = 7,
    TIME_MICROS = 8,
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
    UINT_8 = 11,
    UINT_16 = 12,
    UINT_32 = 13,
    UINT_64 = 14,
    INT_8 = 15,
    INT_16 = 16,
    INT_32 = 17,
    INT_64 = 18,
    JSON = 19,
    BSON = 20,
    INTERVAL = 21,
    NA = 25
};

DECLARE_ENUM_DESCRIPTION(ParquetConvertedType);

enum ParquetFieldRepetitionType {
    PQT_REQUIRED,
    PQT_OPTIONAL,
    PQT_REPEATED
};

DECLARE_ENUM_DESCRIPTION(ParquetFieldRepetitionType);

enum ParquetEncoding {
    PQT_PLAIN = 0,
    PQT_GROUP_VAR_INT = 1,
    PQT_PLAIN_DICTIONARY = 2,
    PQT_RLE = 3,
    PQT_BIT_PACKED = 4,
    PQT_DELTA_BINARY_PACKED = 5,
    PQT_DELTA_LENGTH_BYTE_ARRAY = 6,
    PQT_DELTA_BYTE_ARRAY = 7,
    PQT_RLE_DICTIONARY = 8
};

DECLARE_ENUM_DESCRIPTION(ParquetEncoding);

enum ParquetCompressionCodec {
    PQT_UNCOMPRESSED,
    PQT_SNAPPY,
    PQT_GZIP,
    PQT_LZO,
    PQT_BROTLI
};

DECLARE_ENUM_DESCRIPTION(ParquetCompressionCodec);

enum ParquetPageType {
    PQT_DATA_PAGE = 0,
    PQT_INDEX_PAGE = 1,
    PQT_DICTIONARY_PAGE = 2,
    PQT_DATA_PAGE_V2 = 3
};

DECLARE_ENUM_DESCRIPTION(ParquetPageType);

struct ParquetSchemaElement {
    ParquetType type;
    int32_t type_length = 0;
    ParquetFieldRepetitionType repetition_type;
    std::string name;
    int32_t num_children = 0;
    Optional<ParquetConvertedType> converted_type;
    int32_t scale = 0;
    int32_t precision = 0;
    int32_t field_id = -1;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetSchemaElement);

struct ParquetKeyValue {
    std::string key;
    std::string value;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetKeyValue);

struct ParquetStatistics {
    Optional<std::string> max;
    Optional<std::string> min;
    int64_t null_count = -1;
    int64_t distinct_count = -1;
    Optional<std::string> max_value;
    Optional<std::string> min_value;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetStatistics);

struct ParquetPageEncodingStats {
    ParquetPageType page_type;
    ParquetEncoding encoding;
    int32_t count = 0;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetPageEncodingStats);

struct ParquetColumnMetaData {
    ParquetType type;
    std::vector<ParquetEncoding> encodings;
    std::vector<std::string> path_in_schema;
    ParquetCompressionCodec codec;
    int64_t num_values = 0;
    int64_t total_uncompressed_size = 0;
    int64_t total_compressed_size = 0;
    std::vector<ParquetKeyValue> key_value_metadata;
    int64_t data_page_offset = -1;
    int64_t index_page_offset = -1;
    int64_t dictionary_page_offset = -1;
    Optional<ParquetStatistics> statistics;
    std::vector<ParquetPageEncodingStats> encoding_stats;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetColumnMetaData);

struct ParquetColumnChunk {
    std::string file_path;
    int64_t file_offset = -1;
    ParquetColumnMetaData meta_data;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetColumnChunk);

struct ParquetRowGroup {
    std::vector<ParquetColumnChunk> columns;
    int64_t total_byte_size;
    int64_t num_rows;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetRowGroup);

//struct ParquetColumnOrder {
//};

struct ParquetFileMetaData {
    int32_t version;
    std::vector<ParquetSchemaElement> schema;
    int64_t num_rows;
    std::vector<ParquetRowGroup> row_groups;
    std::vector<ParquetKeyValue> key_value_metadata;
    std::string created_by;
    //std::vector<ParquetColumnOrder> column_orders;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetFileMetaData);

struct ParquetDataPageHeader {
    int32_t num_values;
    ParquetEncoding encoding;
    ParquetEncoding definition_level_encoding;
    ParquetEncoding repetition_level_encoding;
    Optional<ParquetStatistics> statistics;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetDataPageHeader);

struct ParquetDataPageHeaderV2 {
    int32_t num_values;
    int32_t num_nulls;
    int32_t num_rows;
    ParquetEncoding encoding;
    int32_t definition_levels_byte_length;
    int32_t repetition_levels_byte_length;
    bool is_compressed = true;
    Optional<ParquetStatistics> statistics;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetDataPageHeaderV2);

struct ParquetIndexPageHeader {
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetIndexPageHeader);

struct ParquetDictionaryPageHeader {
    int32_t num_values;
    ParquetEncoding encoding;
    bool is_sorted = false;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetDictionaryPageHeader);

struct ParquetPageHeader {
    ParquetPageType type;
    int32_t uncompressed_page_size;
    int32_t compressed_page_size;
    Optional<int32_t> crc;
    Optional<ParquetDataPageHeader> data_page_header;
    Optional<ParquetIndexPageHeader> index_page_header;
    Optional<ParquetDictionaryPageHeader> dictionary_page_header;
    Optional<ParquetDataPageHeaderV2> data_page_header_v2;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetPageHeader);

struct ParquetStructuredSchemaElement: public ParquetSchemaElement {
    ParquetStructuredSchemaElement() = default;
    ParquetStructuredSchemaElement(ParquetSchemaElement el)
        : ParquetSchemaElement(std::move(el))
    {
    }

    std::vector<ParquetStructuredSchemaElement> children;
    int definition_level = -1;
    int repetition_level = -1;

    /** Process an unstructured schema element and get a structured
        version.
    */
    static std::vector<ParquetStructuredSchemaElement>
    process(std::vector<ParquetSchemaElement> & root,
            size_t & first, size_t last,
            int definition, int repetition);

    static ParquetStructuredSchemaElement
    process(std::vector<ParquetSchemaElement> root);

    const ParquetStructuredSchemaElement &
    lookup(const std::vector<std::string> & path,
           size_t prefix = 0) const;

    std::shared_ptr<ExpressionValueInfo>
    getInfo() const;

    size_t maxDepth() const;

    int maxRepetitionLevel() const;

    int maxDefinitionLevel() const;
};

DECLARE_STRUCTURE_DESCRIPTION(ParquetStructuredSchemaElement);

void throwParquetException(const std::string & error) MLDB_NORETURN;

template<typename T>
T readParquetPlainT(const char * & p, const char * e)
{
    LittleEndian<T> result;
    if (p + sizeof(result) > e)
        throwParquetException("End of stream reading Parquet");
    std::memcpy(&result, p, sizeof(result));
    p += sizeof(result);
    return result;
}

template<typename T>
T readParquetPlainAllT(const char * p, const char * e)
{
    LittleEndian<T> result;
    if (p + sizeof(result) != e)
        throwParquetException("Wrong binary size reading Parquet");
    std::memcpy(&result, p, sizeof(result));
    return result;
}

CellValue
readParquetPlain(const char * & p, const char * e,
                 ParquetType type,
                 const ParquetStructuredSchemaElement & schema);

struct ParquetTrailer {
    int32_le metadataSize;
    char signature[4] = {0,0,0,0};
};


/*****************************************************************************/
/* ARRAY READERS                                                             */
/*****************************************************************************/

struct ParquetIntegerArrayReader {
    virtual ~ParquetIntegerArrayReader()
    {
    }

    virtual size_t generate(size_t n, uint32_t * out) = 0;
};

// Creates a ParquetRleArrayReader without exposing the interface
std::unique_ptr<ParquetIntegerArrayReader>
createParquetRleDictionaryReader(const char * & p,
                                 const char * e,
                                 size_t numEntries,
                                 int numBits);

struct ParquetIntegerArray {
    ParquetIntegerArray(const char * & p, const char * e,
                        size_t numEntries,
                        ParquetEncoding encoding,
                        int numBits);

    std::vector<uint32_t> generate(size_t n)
    {
        std::vector<uint32_t> result(n);
        size_t numRead = reader->generate(n, &result[0]);
        ExcAssertEqual(numRead, n);
        return result;
    }

    uint32_t current() const;

    std::shared_ptr<ParquetIntegerArrayReader> reader;

    ParquetEncoding encoding;
    const char * p;
    size_t numEntries;
    size_t numBits;
};

} // namespace MLDB

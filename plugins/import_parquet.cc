/** import_parquet.cc
    Jeremy Barnes, 30 May 2017
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#include "import_parquet.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/optional_description.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/jml/utils/hex_dump.h"
#include "mldb/base/thread_pool.h"
#include "mldb/vfs/compressor.h"
#include "mldb/arch/endian.h"
#include "mldb/server/dataset_context.h"
#include "mldb/arch/bitops.h"
#include "thrift.h"
#include "parquet.h"

using namespace std;


namespace MLDB {

// https://github.com/apache/parquet-cpp/blob/master/src/parquet/parquet.thrift

DEFINE_STRUCTURE_DESCRIPTION(ImportParquetConfig);

ImportParquetConfigDescription::ImportParquetConfigDescription()
{
    addField("dataFileUrl", &ImportParquetConfig::dataFileUrl,
             "URL of the parquet data to import");
    addAuto("outputDataset", &ImportParquetConfig::outputDataset,
            "Dataset to record the data into.");
    addAuto("limit", &ImportParquetConfig::limit,
            "Maximum number of records to process.");
    addAuto("offset", &ImportParquetConfig::offset,
            "Skip the first n records.");
    addAuto("select", &ImportParquetConfig::select,
            "Which columns to import.  Default is to import all.");
    addAuto("where", &ImportParquetConfig::where,
            "Which lines to use to create rows.");
    addAuto("named", &ImportParquetConfig::named,
            "Row name expression for output dataset. Note that each row "
            "must have a unique name.");
    addAuto("timestamp", &ImportParquetConfig::timestamp,
            "Expression for row timestamp.");

    addParent<ProcedureConfig>();
}

ImportParquetProcedure::
ImportParquetProcedure(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->config = config.params.convert<ImportParquetConfig>();
}

Any
ImportParquetProcedure::
getStatus() const
{
    return Any();
}

struct ParquetImporter {

    ParquetImporter(ParquetFileMetaData md,
                    Recorder & recorder,
                    const SelectExpression & select,
                    std::shared_ptr<const SqlExpression> where,
                    std::shared_ptr<const SqlExpression> named,
                    std::shared_ptr<const SqlExpression> timestamp,
                    size_t offset,
                    ssize_t limit)
        : md(std::move(md)),
          recorder(recorder),
          outerScope(recorder.server),
          selectScope(outerScope),
          offset(offset), limit(limit)
        
    {
        /* The schema in the metadata is flattened since Thrift doesn't support
           nesting.  Here we reconstitute a structured version of the schema
           that is easier to work with.
        */
        this->schema
            = ParquetStructuredSchemaElement::process(this->md.schema);

        cerr << jsonEncode(this->schema) << endl;

#if 0
        std::vector<KnownColumn> columns;
        for (auto & el: this->schema) {
            columns.emplace_back(PathElement(el.name), el.getInfo(),
                                 el.repetition_type == PQT_REQUIRED
                                 ? COLUMN_IS_DENSE : COLUMN_IS_SPARSE);
        }
        
        auto info = std::make_shared<RowValueInfo>(columns);
#else
        auto info = schema.getInfo();
#endif
   
        cerr << "info is " << jsonEncode(info) << endl;

        selectScope.init(info);

        // Set up so that we can bind the custom functions
        selectScope.customFunctions = bindCustomFunction;

        selectBound = select.bind(selectScope);
        isIdentitySelect = select.isIdentitySelect(selectScope);
        whereBound = where->bind(selectScope);
        isWhereTrue = where->isConstantTrue();
        namedBound = named->bind(selectScope);
        isNamedRowNumber = named->surface == "rowNumber()";
        timestampBound = timestamp->bind(selectScope);
        isFileTimestamp = timestamp->surface == "fileTimestamp()";
    }

    ParquetFileMetaData md;
    ParquetStructuredSchemaElement schema;
    Recorder & recorder;
    SqlExpressionMldbScope outerScope;
    SqlExpressionExtractScope selectScope;

    BoundSqlExpression selectBound;
    bool isIdentitySelect = false;
    BoundSqlExpression whereBound;
    bool isWhereTrue = false;
    BoundSqlExpression namedBound;
    bool isNamedRowNumber = false;
    BoundSqlExpression timestampBound;
    bool isFileTimestamp = false;

    size_t offset = 0;
    ssize_t limit = -1;

    std::vector<KnownColumn> columns;

    struct ParquetRowScopeExtra {
        uint64_t rowNumber;
        ParquetImporter * importer;
        Date fileTimestamp;
    };

    static BoundFunction
    bindCustomFunction(const Utf8String & functionName,
                       const std::vector<BoundSqlExpression> & args)
    {
        if (functionName == "rowNumber") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                    {
                        auto & row
                            = scope.as<SqlExpressionExtractScope::RowScope>();
                        auto extra = reinterpret_cast<const ParquetRowScopeExtra *>
                            (row.context);
                        return ExpressionValue(extra->rowNumber,
                                               extra->fileTimestamp);
                    },
                    std::make_shared<IntegerValueInfo>()
                };
        }
        else if (functionName == "fileTimestamp") {
            return {[=] (const std::vector<ExpressionValue> & args,
                         const SqlRowScope & scope)
                    {
                        auto & row
                            = scope.as<SqlExpressionExtractScope::RowScope>();
                        auto extra = reinterpret_cast<const ParquetRowScopeExtra *>
                            (row.context);
                        return ExpressionValue(extra->fileTimestamp,
                                               extra->fileTimestamp);
                    },
                    std::make_shared<TimestampValueInfo>()
                };
        }
        return BoundFunction();
    }

    void read(Url filename,
              filter_istream & stream)
    {
        size_t i = 0;
        for (auto & rg: md.row_groups) {
            cerr << "reading group " << i << " of " << md.row_groups.size() << endl;
            readRowGroup(filename, stream, rg);
        }
    }

    const ParquetStructuredSchemaElement &
    getSchema(const std::vector<std::string> & path) const
    {
        return schema.lookup(path, 0);
    }

    std::vector<ExpressionValue>
    readColumnChunk(Url filename,
                    filter_istream & outerStream,
                    const ParquetColumnChunk & column)
    {
        cerr << "reading column "
             << jsonEncodeStr(column.meta_data.path_in_schema) << endl;
        cerr << jsonEncode(column) << endl;

        const auto & schema
            = getSchema(column.meta_data.path_in_schema);

        cerr << jsonEncode(schema) << endl;

        filter_istream stream(filename);
        if (column.meta_data.dictionary_page_offset != -1) {
            stream.seekg(std::min(column.meta_data.data_page_offset,
                                  column.meta_data.dictionary_page_offset),
                         std::ios_base::beg);
        }
        else {
            stream.seekg(column.meta_data.data_page_offset, std::ios_base::beg);
        }

        auto deserializer = createThriftDeserializerT<ParquetPageHeader>();

        size_t rowsDone = 0;

        uint64_t bytesDone = 0;

        //while (stream.tellg() - column.meta_data.data_page_offset
        //       < column.meta_data.total_compressed_size) {

        struct DataElement {
            DataElement(uint16_t def = 0, uint16_t rep = 0,
                        CellValue val = CellValue())
                : def(def), rep(rep), val(std::move(val))
            {
            }

            uint16_t def;
            uint16_t rep;
            CellValue val;
        };

        std::vector<DataElement> elements;

        std::vector<CellValue> dictionary;

        while (rowsDone < column.meta_data.num_values) {

            //cerr << "done " << bytesDone << " of "
            //     << column.meta_data.total_uncompressed_size
            //     << " bytes" << endl;

            cerr << "done " << rowsDone << " of "
                 << column.meta_data.num_values << endl;

            auto before = stream.tellg();

            ParquetPageHeader header = deserializer(stream);
        
            cerr << "page header is " << jsonEncode(header) << endl;

            auto headerBytes = stream.tellg() - before;

            cerr << "page header has " << headerBytes
                 << " bytes" << endl;
            bytesDone += headerBytes;

            std::string compression;
            switch (column.meta_data.codec) {
            case PQT_UNCOMPRESSED:
                compression = "none";
                break;
            case PQT_GZIP:
                compression = "gzip";
                break;
            case PQT_SNAPPY:
                compression = "snappy";
                break;
            default:
                throw HttpReturnException
                    (500, "Can't deal with Parquet compression type "
                     + jsonEncodeStr(column.meta_data.codec));
            }
        
            std::string compressed(header.compressed_page_size, '\0');

            stream.read(&compressed[0], header.compressed_page_size);

            if (!stream)
                throw HttpReturnException(600, "Error reading compressed Parquet column");

            std::shared_ptr<Decompressor> decompressor
                (Decompressor::create(compression));
                
            std::string decompressed;
            decompressed.reserve(header.uncompressed_page_size);
            
            auto onData = [&] (const char * data, size_t len)
                {
                    decompressed.append(data, len);
                    return len;
                };

            decompressor->decompress(compressed.data(),
                                     header.compressed_page_size,
                                     onData);
            
            ML::hex_dump(decompressed.data(), decompressed.length());

            // Now we have our data.  Time to decode it
            switch (header.type) {
            case PQT_INDEX_PAGE:
                break;
                // we skip index pages since we don't know what
                // to do with them
            case PQT_DICTIONARY_PAGE: {
                ExcAssert(header.dictionary_page_header);
                
                switch (header.dictionary_page_header->encoding) {
                case PQT_PLAIN_DICTIONARY: {
                    ExcAssert(dictionary.empty());
                    dictionary.reserve(header.dictionary_page_header->num_values);
                    const char * p = decompressed.data();
                    const char * e = p + decompressed.size();
                    for (size_t i = 0;  i < header.dictionary_page_header->num_values;  ++i) {
                        dictionary.emplace_back(readParquetPlain(p, e, column.meta_data.type, schema));
                        if (i < 10) {
                            cerr << jsonEncodeStr(dictionary.back()) << endl;
                        }
                    }
                    break;
                }
                    
                default:
                    throw HttpReturnException
                        (400, "Can't real with PQT dictionary page encoding "
                         + jsonEncodeStr
                         (header.dictionary_page_header->encoding));
                }
                break;
            }

            case PQT_DATA_PAGE: {
                ExcAssert(header.data_page_header);
                
                cerr << "page has " << header.data_page_header->num_values
                     << " values" << endl;
                rowsDone += header.data_page_header->num_values;
                
                const char * p0 = decompressed.data();
                const char * p = decompressed.data();
                const char * e = p + decompressed.size();


                cerr << "max rep level " << this->schema.maxRepetitionLevel()
                     << endl;

                cerr << "max def level " << this->schema.maxDefinitionLevel()
                     << endl;

                int repetitionLevelBits
                    = ML::highest_bit(this->schema.maxRepetitionLevel(), -1) + 1;
                    //= ML::highest_bit(schema.repetition_level, -1) + 1;
                
                ParquetIntegerArray repetitions
                    (p, e, header.data_page_header->num_values,
                     header.data_page_header->repetition_level_encoding,
                     repetitionLevelBits);

                cerr << "repetitions had " << p - p0 << " bytes" << endl;

                int definitionLevelBits
                    = ML::highest_bit(this->schema.maxDefinitionLevel(), -1) + 1;

                cerr << "definitionLevelBits = "
                     << definitionLevelBits << endl;
                
                cerr << "reading definition level with "
                     << definitionLevelBits << " bits" << endl;
                ParquetIntegerArray definitions
                    (p, e, header.data_page_header->num_values,
                     header.data_page_header->definition_level_encoding,
                     definitionLevelBits);
                
                cerr << "definitions had " << p - p0 << " bytes" << endl;

                std::vector<CellValue> vals;
                vals.reserve(header.data_page_header->num_values);

                switch (header.data_page_header->encoding) {
                case PQT_PLAIN_DICTIONARY: {
                    //cerr << "doing data for plain_dictionary" << endl;
                    //ML::hex_dump(p, e - p);

                    if (p == e) {
                        throw HttpReturnException
                            (400, "Expected dictionary width reading Parquet "
                             "array");
                    }
                    uint32_t bitWidth = *p++;
                    cerr << "plain dict: bit width is " << bitWidth << endl;

                    auto reader
                        = createParquetRleDictionaryReader
                        (p, e, header.data_page_header->num_values,
                         bitWidth);

                    static constexpr int NUM_TO_DECODE = 1024;

                    while (vals.size()
                           < header.data_page_header->num_values) {

                        cerr << "generated " << vals.size()
                             << " of " << header.data_page_header->num_values
                             << endl;

                        size_t numToDo
                            = std::min<size_t>
                            (NUM_TO_DECODE,
                             header.data_page_header->num_values
                             - vals.size());
                        
                        uint32_t gen[NUM_TO_DECODE];

                        size_t numGenerated
                            = reader->generate(numToDo, gen);
                        ExcAssertEqual(numGenerated, numToDo);

                        cerr << "generated " << vals.size()
                             << " of " << header.data_page_header->num_values
                             << endl;
                        cerr << "generated " << numGenerated << endl;
                        cerr << "dictionary has " << dictionary.size() << endl;

                        for (size_t i = 0;  i < numGenerated;  ++i) {
                            try {
                                CellValue val
                                    = dictionary.empty()
                                    ? gen[i]
                                    : dictionary.at(gen[i]);
                                vals.emplace_back(std::move(val));
                            } catch (const std::exception & exc) {
                                cerr << "generating " << i << " of "
                                     << numGenerated << " with value "
                                     << gen[i] << endl;
                                throw;
                            }
                        }
                    }

                    break;
                }
                case PQT_PLAIN: {
                    if (column.meta_data.type == PQT_BOOLEAN) {
                        for (size_t i = 0;  i < header.data_page_header->num_values;  ++i) {
                            int bitNum = i % 8;
                            vals.emplace_back((*p & (1<<bitNum)) != 0);
                            if (bitNum == 7)
                                ++p;
                        }
                    }
                    else {
                        const char * p = decompressed.data();
                        const char * e = p + decompressed.size();
                        for (size_t i = 0;  i < header.data_page_header->num_values;  ++i) {
                            vals.emplace_back(readParquetPlain(p, e, column.meta_data.type, schema));
                        }
                    }
                    break;
                }

                case PQT_RLE:
                case PQT_BIT_PACKED:
                case PQT_DELTA_BINARY_PACKED:
                case PQT_DELTA_LENGTH_BYTE_ARRAY:
                case PQT_DELTA_BYTE_ARRAY:
                case PQT_RLE_DICTIONARY:
                default:
                    throw HttpReturnException
                        (500, "Can't handle Parquet data page encoding "
                         + jsonEncodeStr(header.data_page_header->encoding));
                    break;
                }

                std::vector<uint32_t> reps = repetitions.generate(vals.size());
                std::vector<uint32_t> defs = definitions.generate(vals.size());

                //int64_t rowNumber = -1;
                
                for (size_t i = 0;  i < vals.size();  ++i) {
                    cerr << "  " << i << " " << defs[i] << " " << reps[i]
                         << " " << jsonEncodeStr(vals[i]) << endl;
                    elements.emplace_back(defs[i], reps[i], std::move(vals[i]));
                }
                break;
            }

            case PQT_DATA_PAGE_V2:
                break;
            }
            

            bytesDone += header.uncompressed_page_size;
        }        

        std::vector<ExpressionValue> result;
        return result;
    }

    void readRowGroup(Url filename,
                      filter_istream & outerStream,
                      const ParquetRowGroup & rowGroup)
    {
        std::vector<std::vector<ExpressionValue> > columns;
        columns.reserve(rowGroup.columns.size());
        for (auto & c: rowGroup.columns) {
            columns.emplace_back(readColumnChunk(filename, outerStream, c));
        }

        // Now record each one

#if 0
        std::vector<ParquetColumnChunk> columns;
        int64_t total_byte_size;
        int64_t num_rows;
#endif
    }
};

RunOutput
ImportParquetProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(config, run);

    std::shared_ptr<Dataset> dataset
        = createDataset(server, runProcConf.outputDataset, onProgress,
                        true /*overwrite*/);

    filter_istream stream(config.dataFileUrl);

    char h[5] = {0,0,0,0};

    stream.read(h, 4);

    if (h[0] != 'P' || h[1] != 'A' || h[2] != 'R' || h[3] != '1') {
        cerr << "header " << h << endl;
        throw HttpReturnException(400, "Parquet file header is wrong");
    }

    size_t sz = stream.info().size;

    static constexpr size_t TRAILER_LENGTH = sizeof(ParquetTrailer); // 8

    stream.seekg(sz - TRAILER_LENGTH, ios_base::beg);

    // The last 8 bytes of the file are the ParquetTrailer
    char ch[TRAILER_LENGTH + 1] = {0};
    ParquetTrailer trailer;
    stream.read(ch, TRAILER_LENGTH);
    memcpy(&trailer, ch, TRAILER_LENGTH);
    
    if (trailer.signature[0] != 'P' || trailer.signature[1] != 'A'
        || trailer.signature[2] != 'R' || trailer.signature[3] != '1') {
        throw HttpReturnException(400, "Parquet file header is wrong");
    }

    cerr << "md size is " << trailer.metadataSize << endl;
    cerr << trailer.signature[0] << trailer.signature[1]
         << trailer.signature[2] << trailer.signature[3] << endl;


    // Now seek to the beginning of the metadata block
    stream.seekg(sz - TRAILER_LENGTH - trailer.metadataSize);

    auto deserializer = createThriftDeserializerT<ParquetFileMetaData>();

    ParquetFileMetaData md = deserializer(stream);

    cerr << "md = " << jsonEncode(md) << endl;

    DatasetRecorder recorder(dataset.get());

    ParquetImporter importer(md,
                             recorder,
                             config.select,
                             config.where,
                             config.named,
                             config.timestamp,
                             config.offset,
                             config.limit);

    importer.read(config.dataFileUrl, stream);

    Date before = Date::now();
    
    // Read blocks for parallel processing
    int64_t objectNumber = 0;
    size_t numBlocks = 0;
    int64_t totalBytes = 0;
    //size_t chunkNumber = 0;

    dataset->commit();
    //coordinator.commit();

    Date after = Date::now();
    double elapsed = after.secondsSince(before);

    cerr << endl << endl;
    cerr << "read " << numBlocks << " blocks with " << objectNumber
         << " objects in " << elapsed << "seconds with "
         << 1.0 * totalBytes / objectNumber
         << " bytes/object at " << objectNumber / elapsed
         << " objects/second and "
         << totalBytes / elapsed / 1000000.0 << " MB/second"
         << endl;



#if 0
    ImportParquetProcedureWorkInstance instance(logger);

    instance.loadParquet(config, dataset, server, onProgress);
#endif

    Json::Value status;
#if 0
    status["numLineErrors"] = instance.numLineErrors;
    status["rowCount"] = instance.rowCount;
#endif

    dataset->commit();

    return Any(status);
}

namespace {

RegisterProcedureType<ImportParquetProcedure, ImportParquetConfig>
regImportParquet(builtinPackage(),
      "Import from a parquet file, line by line.",
      "procedures/import_parquet_procedure.md.html");

} // file scope

} //MLDB


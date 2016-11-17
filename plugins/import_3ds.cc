/* import_3ds.cc
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Procedure that reads 3ds files into an indexed dataset.
*/


#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/types/optional.h"
#include "mldb/types/url.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/plugins/for_each_line.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/endian.h"
#include "mldb/http/http_exception.h"
#include "mldb/server/per_thread_accumulator.h"

using namespace std;

namespace MLDB {


struct Import3dsConfig : public ProcedureConfig  {
    static constexpr const char * name = "import.3ds";

    Import3dsConfig()
    {
        vertexDataset.withType("tabular");
        faceDataset.withType("tabular");
    }

    Url dataFileUrl;
    PolyConfigT<Dataset> vertexDataset;
    PolyConfigT<Dataset> faceDataset;

};

DECLARE_STRUCTURE_DESCRIPTION(Import3dsConfig);

DEFINE_STRUCTURE_DESCRIPTION(Import3dsConfig);

Import3dsConfigDescription::
Import3dsConfigDescription()
{
    addParent<ProcedureConfig>();

    addField("dataFileUrl", &Import3dsConfig::dataFileUrl,
             "");
    addField("vertexDataset", &Import3dsConfig::vertexDataset,
             "Dataset to record the data into.",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("faceDataset", &Import3dsConfig::faceDataset,
             "Dataset to record the data into.",
             PolyConfigT<Dataset>().withType("tabular"));
}


/*****************************************************************************/
/* IMPORT 3DS PROCEDURE                                                      */
/*****************************************************************************/

struct Import3dsProcedure: public Procedure {

    Import3dsProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress) 
    : Procedure(owner)
    {
        this->config = config.params.convert<Import3dsConfig>();
    }

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const
    {
        return Any();
    }

    Import3dsConfig config;

private:

};

//#pragma pack(push, 1)
struct ChunkHeader {
    uint16_t chunkid;
    uint32_t chunkSize;
} MLDB_PACKED;
//#pragma pack(pop)

RunOutput 
Import3dsProcedure::
run(const ProcedureRunConfig & run, const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(config, run);

    string filename = Url::decodeUri(config.dataFileUrl.toUtf8String()).rawString();

    std::shared_ptr<Dataset> vertexDataset
            = createDataset(server, config.vertexDataset, onProgress,
                            true /*overwrite*/);

    std::shared_ptr<Dataset> faceDataset
            = createDataset(server, config.faceDataset, onProgress,
                            true /*overwrite*/);

    // Ask for a memory mappable stream if possible
    MLDB::filter_istream stream(filename, { { "mapped", "true" } });

    ChunkHeader mainChunkHeader;
    stream.read((char *)&mainChunkHeader, sizeof(mainChunkHeader));

    cerr << std::hex << mainChunkHeader.chunkid << "," << std::dec << mainChunkHeader.chunkSize << endl;

   /* for (int i = 0; i < 16; ++i) {
        uint16_t test;
        stream.read((char*)&test, 2);
        cerr << std::hex << test << ",";
    }*/

    size_t vertexCount = 0;
    auto recordVertex = [&] (float x, float y, float z) {
           // void recordRow(const RowPath & rowName,
             //      const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);
        std::vector<std::tuple<ColumnPath, CellValue, Date> > vals;
        vals.emplace_back(ColumnPath("x"), CellValue(x), Date::now());
        vals.emplace_back(ColumnPath("y"), CellValue(y), Date::now());
        vals.emplace_back(ColumnPath("z"), CellValue(z), Date::now());

        vertexDataset->recordRow(RowPath(vertexCount), vals);
        ++vertexCount;
    };

    size_t faceCount = 0;
    auto recordFace = [&] (int x, int y, int z) {
           // void recordRow(const RowPath & rowName,
             //      const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);
        std::vector<std::tuple<ColumnPath, CellValue, Date> > vals;
        vals.emplace_back(ColumnPath("x"), CellValue(x), Date::now());
        vals.emplace_back(ColumnPath("y"), CellValue(y), Date::now());
        vals.emplace_back(ColumnPath("z"), CellValue(z), Date::now());

        faceDataset->recordRow(RowPath(faceCount), vals);
        ++faceCount;
    };

    auto readObject = [&] (int offset, int size, MLDB::filter_istream& stream) {

        size_t nameCount = 0;
        char letter = 0;
        do
        {
           stream.read(&letter, 1);
           cerr << letter;
           nameCount++;
           offset++;
        }
        while ((letter!=0) && (nameCount<12));

        cerr << endl;

        size_t byteCount = 6;
        ChunkHeader subchunk;
        stream.read((char *)&subchunk, sizeof(subchunk));
        cerr << std::hex << subchunk.chunkid << "," << std::dec << subchunk.chunkSize << endl;

        if (subchunk.chunkid == 0x4100) {
            size_t subCount = 6;
            cerr << "Reading Triangular Mesh" << endl;
            size_t vertexOffset = vertexCount;
            uint16_t verticesRead = 0;
            uint16_t maxVertexIndice = 0;
            do {
                ChunkHeader meshchunk;
                stream.read((char *)&meshchunk, sizeof(meshchunk));
                cerr << std::hex << meshchunk.chunkid << "," << std::dec << meshchunk.chunkSize << endl;
                if (meshchunk.chunkid == 0x4110) {
                    //vertex list
                    uint16_t numVertices = 0;
                    stream.read((char *)&numVertices, 2);
                    cerr << "num vertices: " << numVertices << endl;

                    for (int v = 0; v < numVertices; ++v) {
                        float vertices[3];
                        stream.read((char *)&vertices, 12);

                        recordVertex(vertices[0], vertices[1], vertices[2]);
                    }

                    verticesRead = numVertices;
                }
                else if (meshchunk.chunkid == 0x4120) {
                    //face list
                    uint16_t numFace = 0;
                    stream.read((char *)&numFace, 2);
                    cerr << "num faces: " << numFace << endl;

                    for (int v = 0; v < numFace; ++v) {
                        uint16_t vertices[4];
                        stream.read((char *)&vertices, 8);

                        //cerr << "vertexOffset: " << vertexOffset << endl;
                        //cerr << "grdsf: " << vertices[0] << "," << vertices[1] << "," << vertices[2] << "," << vertices[3] << endl;

                        recordFace(vertices[0]+vertexOffset, vertices[1]+vertexOffset, vertices[2]+vertexOffset);
                        maxVertexIndice = std::max(maxVertexIndice, vertices[0]);
                        maxVertexIndice = std::max(maxVertexIndice, vertices[1]);
                        maxVertexIndice = std::max(maxVertexIndice, vertices[2]);
                    }
                }

                ExcAssert(maxVertexIndice == verticesRead -1);

                subCount += meshchunk.chunkSize;
                stream.seekg(offset + byteCount + subCount);
            } while (subCount < subchunk.chunkSize);
            
        }

        byteCount += subchunk.chunkSize;
    };

    size_t byteCount = 6;
    do {
        ChunkHeader chunk;
        stream.read((char *)&chunk, sizeof(chunk));
        cerr << std::hex << chunk.chunkid << "," << std::dec << chunk.chunkSize << endl;

        if (chunk.chunkid == 0x0002) {
            uint16_t version = 0;
            stream.read((char*)&version, 2);
            cerr << "3ds version: " << version << endl;
        }
        else if (chunk.chunkid == 0x3d3d) {
            cerr << "reading 3d data" << endl;
            size_t subbytecount = 6;
            do {
                ChunkHeader subchunk;
                stream.read((char *)&subchunk, sizeof(subchunk));
                cerr << std::hex << subchunk.chunkid << "," << std::dec << subchunk.chunkSize << endl;
                if (subchunk.chunkid == 0x2100) {
                    cerr << "skipping ambient data" << endl;
                }
                else if (subchunk.chunkid == 0xAFFF) {
                    cerr << "skipping material data" << endl;
                }
                else if (subchunk.chunkid == 0x4000) {
                    cerr << "reading object data" << endl;
                    readObject(byteCount + subbytecount, subchunk.chunkSize, stream);
                }
                subbytecount += subchunk.chunkSize;
                stream.seekg(byteCount + subbytecount);
            } while (subbytecount < chunk.chunkSize);            
        }
        else if (chunk.chunkid == 0xb000) {
            cerr << "skipping keyframe data" << endl;
        }
        else {
            cerr << "ignoring unknown data id " << std::hex << mainChunkHeader.chunkid << endl;
        }

        stream.seekg(byteCount + chunk.chunkSize);
        byteCount += chunk.chunkSize;

    } while (byteCount < mainChunkHeader.chunkSize);

    vertexDataset->commit();
    faceDataset->commit();

    RunOutput result;
    return result;
}

RegisterProcedureType<Import3dsProcedure, Import3dsConfig>
regImportLas(builtinPackage(),
             "",
             "");

}
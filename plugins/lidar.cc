#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/types/url.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "mldb/base/parallel.h"
#include "mldb/base/thread_pool.h"
#include "mldb/server/column_scope.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/server/per_thread_accumulator.h"
#include "mldb/plugins/for_each_line.h"
#include "mldb/arch/timers.h"
#include <endian.h>
#include <iostream>


using namespace std;


namespace Datacratic {
namespace MLDB {

struct TraceRayConfig {
    double cameraX;
    double cameraY;
    double cameraZ;

    int width;
    int height;
    
};

struct TraceRays {
};

struct PixelData {
};

double distance(double pointX, double pointY, double pointZ,
                double cameraX, double cameraY, double cameraZ);


// Goal
// - render part of a scene onto a pixel grid, with the camera at an
//   infinite distance
// - using a z-buffer

// Inputs
// - camera
//   - point (z, y, z)
//   - angle (0-360 degrees)
//   - elevation angle (-90 to 90 degrees)
// - image
//   - width (pixels)
//   - height (pixels)
//   - width (...)


// Trace ray function
// - we have a point at (x,y,z) with color (r,g,b)
// - we have a plane p with a surface normal N
// - figure out where on the plane a ray cast from the point in direction
//   -N would intersect the plane (x and y coordinates)


// Equation of a line in 2-space (which breaks down for vertical lines)
// y = kx + l
// but more generally (which works for vertical lines)
// a(x - x0) + b(y - y0) = 0


// Equation of an infinite plane
//
// ax + by + cz = d
//
// this has the surface normal vector (a, b, c)

// Intersection point of a ray cast from point (x,y,z) with the plane
// First we need to define a coordinate system on the plane.  The most
// sensible is that the origin of the plane is where the origin projects
// on to the plane.  We then are left with the rotation of the coordinate
// system.  This can be handled by defining an up direction.

// https://en.wikipedia.org/wiki/Line%E2%80%93plane_intersection

// Distance of a point (x,y,z) from the intersection point of the plane
// that's simply the projection onto the unit normal vector

// Inputs
// - unit surface normal on the plane n
// - point on the plane through which it passes p
// - 


void tracePixel();

struct CreateImageConfig: public ProcedureConfig {
    InputQuery trainingData;
    Url imageUrl;
    ssize_t limit;
    static constexpr const char * name = "lidar.image";
};

DECLARE_STRUCTURE_DESCRIPTION(CreateImageConfig);

DEFINE_STRUCTURE_DESCRIPTION(CreateImageConfig);

CreateImageConfigDescription::
CreateImageConfigDescription()
{
    addParent<ProcedureConfig>();

    addField("trainingData", &CreateImageConfig::trainingData,
             "");
    addField("imageUrl", &CreateImageConfig::imageUrl,
             "");
    addField("limit", &CreateImageConfig::limit,
             "");
}

struct ZRenderOptions {
    size_t w;
    size_t h;
};

DECLARE_STRUCTURE_DESCRIPTION(ZRenderOptions);
DEFINE_STRUCTURE_DESCRIPTION(ZRenderOptions);

ZRenderOptionsDescription::
ZRenderOptionsDescription()
{
}

BoundTableExpression
zrender(const SqlBindingScope & context,
        const std::vector<BoundTableExpression> & args,
        const BoundSqlExpression & optionsBound,
        const Utf8String& alias)
{
    BoundTableExpression result;
    result.asName = alias;
    
    //auto options
    //    = jsonDecode<ZRenderOptions>(optionsBound.constantValue().extractJson());
    
    //auto info = std::make_shared<EmbeddingValueInfo>({ options.w, options.h, 4 },
    //                                                 ST_UINT8);
    
    // We return an embedding that's actually the image
    result.table.getRowInfo = [=] () -> std::shared_ptr<RowValueInfo>
        {
            //return info;
            return nullptr;
        };

    // No special functions for me
    result.table.getFunction = [=]
        (SqlBindingScope & scope,
         const Utf8String & tableName,
         const Utf8String & functionName,
         const std::vector<std::shared_ptr<ExpressionValueInfo> > & args)
        -> BoundFunction
        {
            return BoundFunction();
        };
    
    result.table.runQuery = [=]
        (const SqlBindingScope & context,
         const SelectExpression & select,
         const WhenExpression & when,
         const SqlExpression & where,
         const OrderByExpression & orderBy,
         ssize_t offset,
         ssize_t limit) -> BasicRowGenerator
        {
            return BasicRowGenerator();
        };

    // Not a join so no aliases
    result.table.getChildAliases = [=] () -> std::vector<Utf8String>
        {
            return {};
        };

    return result;
}

struct CreateImageProcedure: public Procedure {

    CreateImageProcedure(MldbServer * owner,
                         PolyConfig config,
                         const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        this->svdConfig = config.params.convert<CreateImageConfig>();
        //this->config.reset(new ProcedureConfig(std::move(config)));
    }

    CreateImageConfig svdConfig;

    virtual Any getStatus() const
    {
        return Any();
    }

    virtual RunOutput
    run(const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(svdConfig, run);

        //auto onProgress2 = [&] (const Json::Value & progress)
        //    {
        //        Json::Value value;
        //        value["dataset"] = progress;
        //        return onProgress(value);
        //    };

        if (!runProcConf.imageUrl.empty()) {
            checkWritability(runProcConf.imageUrl.toString(), "imageUrl");
        }

        union ZEntry {
            ZEntry()
                : z(INFINITY), r(0), g(0), b(0), unused(0)
            {
            }

            std::atomic<uint64_t> abits;
            uint64_t bits;
            struct {
                float z;
                uint8_t r;
                uint8_t g;
                uint8_t b;
                uint8_t unused;
            };
        };

        size_t px = 4096;
        size_t py = 2048;

        std::unique_ptr<ZEntry []> pixels(new ZEntry[px * py]);

        uint64_t inside = 0, outside = 0;

        double minX = INFINITY, maxX = -INFINITY,
            minY = INFINITY, maxY = -INFINITY,
            minZ = INFINITY, maxZ = -INFINITY;

        // Surface normal of the camera
        //ML::distribution<double> surfaceNormal{1, 1, 0};
        //surfaceNormal.normalize();

        // Theta is the isometric angle
        // Theta = 0 means look entirely from the top down
        // Theta = pi means look entirely along the y axis
        // In between means isometric projection
        float th = M_PI / 4;

        // Rotation matrix for isometric projection
        float rotation[3][3] = {
            { 1, 0, 0},
            { 0, cos(th), sin(th), },
            { 0, sin(th), cos(th)  } };

        // Rotate the given coordinate according to the rotation matrix
        auto rotate = [&] (int i, float x, float y, float z) -> float
            {
                return
                  rotation[i][0] * x
                + rotation[i][1] * y
                + rotation[i][2] * z;
            };

        // Record a value in pixel coordinates
        auto recordValue = [&] (double x_, double y_, double z_,
                                uint8_t r, uint8_t g, uint8_t b)
            {
                double x = rotate(0, x_, y_, z_);
                double y = rotate(1, x_, y_, z_);
                double z = rotate(2, x_, y_, z_);

#if 0
                minX = std::min(minX, x);
                maxX = std::max(maxX, x);
                minY = std::min(minY, y);
                maxY = std::max(maxY, y);
                minZ = std::min(minZ, z);
                maxZ = std::max(maxZ, z);
#endif

                //cerr << "x = " << x << " y = " << y << " z = " << z << endl;
                //cerr << "r = " << r << " g = " << g << " b = " << b << endl;
                
                int64_t xi = x, yi = y;

                if (xi < 0 || xi >= px || yi < 0 || yi >= py /*|| z < 0*/) {
                    ++outside;
                    return;
                }
                ++inside;

                //cerr << "storing at " << x << "," << y << endl;

                ZEntry & entry = pixels.get()[px * yi + xi];
                for (;;) {
                    ZEntry loaded;
                    loaded.bits = entry.abits.load();
                    
                    if (loaded.z <= z)
                        return;
            
                    //cerr << "storing z = " << z << endl;
        
                    ZEntry stored;
                    stored.z = z;
                    stored.r = r;
                    stored.g = g;
                    stored.b = b;

                    if (entry.abits.compare_exchange_weak(loaded.bits, stored.bits))
                        return;
                }
            };

#if 0
        // Unit vector for direction of the normal of the plane
        double px = 1, py = 0, pz = 0;

        // Position of a point through which the plane passes
        double ox = 0, oy = 0, oz = 0;
#endif

        SqlExpressionMldbScope scope(server);

        // run the query
        cerr << "running query" << endl;

        auto doRow = [&] (double x, double y, double z,
                          uint8_t r, uint8_t g, uint8_t b)
            {
#if 0
                double x, y, z;
                uint8_t r, g, b;

                //const MatrixNamedRow & row = rows[i];
                MatrixNamedRow row;

                ExcAssertEqual(row.columns.size(), 6);

                b = std::get<1>(row.columns[0]).toInt();
                g = std::get<1>(row.columns[1]).toInt();
                r = std::get<1>(row.columns[2]).toInt();
                x = std::get<1>(row.columns[3]).toDouble();
                y = std::get<1>(row.columns[4]).toDouble();
                z = std::get<1>(row.columns[5]).toDouble();
#endif

                recordValue(x * 0.001 + 2000, y * 0.001, z * 0.01 + 500, r, g, b);
            };
        
        std::function<bool (Path &, ExpressionValue &)>
            onResult = [&] (Path & rowName, ExpressionValue & expr) -> bool
            {
                static const PathElement coordKey("coord");
                static const PathElement colorKey("color");

                ExpressionValue coordVal = expr.getColumn(coordKey);
                ExpressionValue colorVal = expr.getColumn(colorKey);

                double coord[3];
                uint8_t color[3];

                coordVal.convertEmbedding(coord, 3, ST_FLOAT64);
                colorVal.convertEmbedding(color, 3, ST_UINT8);

                doRow(coord[0], coord[1], coord[2],
                      color[0], color[1], color[2]);
                
                return true;
            };

        vector<PathElement> columnNames = { "x", "y", "z", "r", "g", "b" };
        
        BoundTableExpression table
            = runProcConf.trainingData.stm->from->bind(scope);

        ML::Timer timer;

        ColumnScope colScope(server, table.dataset);
        vector<BoundSqlExpression> bound;
        for (auto & c: columnNames) {
            auto expr = std::make_shared<ReadColumnExpression>(c);
            bound.emplace_back(expr->bind(colScope));
        }
        
        

        auto onVal = [&] (size_t rowNum, double * vals)
            {
#if 1
                recordValue(vals[0] * 0.001 + 1000,
                            vals[1] * 0.001 + 1000,
                            vals[2] * 0.001,
                            vals[3], vals[4], vals[5]);
#else
                recordValue(vals[0] * 0.001 + 1000,
                            (vals[2] * -0.001) + 500,
                            vals[1] * 0.001 + 500,
                            vals[3], vals[4], vals[5]);
#endif
                return true;
            };


        colScope.runIncrementalDouble(bound, onVal);
        
#if 0
        auto onColumn = [&] (int columnNum)
            {
                auto column = table.dataset->getColumnIndex()->getColumn(columnNames[columnNum]);
            };

        parallelMap(0, 1 /*columnNames.size()*/, onColumn);
#endif

        cerr << "got all columns in " << timer.elapsed() << endl;

        //queryFromStatement(onResult, *runProcConf.trainingData.stm, scope);

        //parallelMap(0, rows.size(), doRow);

        cerr << "got " << inside << " inside and " << outside << " outside points"
             << endl;
        cerr << "x: from " << minX << " to " << maxX << endl;
        cerr << "y: from " << minY << " to " << maxY << endl;
        cerr << "z: from " << minZ << " to " << maxZ << endl;

        cerr << "done running query" << endl;

        std::unique_ptr<uint8_t []> r(new uint8_t[px * py]);
        std::unique_ptr<uint8_t []> g(new uint8_t[px * py]);
        std::unique_ptr<uint8_t []> b(new uint8_t[px * py]);
        std::unique_ptr<uint8_t []> a(new uint8_t[px * py]);

        uint64_t activePixels = 0;
        uint64_t inactivePixels = 0;

        size_t i = 0;
        for (size_t y = 0;  y < py;  ++y) {
            for (size_t x = 0;  x < px;  ++x, ++i) {
                if (pixels[i].z < INFINITY) {
                    ++activePixels;
                    r[i] = pixels[i].r;
                    g[i] = pixels[i].g;
                    b[i] = pixels[i].b;
                    a[i] = 255;
                }
                else {
                    ++inactivePixels;
                    r[i] = 0;
                    g[i] = 0;
                    b[i] = 0;
                    a[i] = 0;
                }
            }
        }
        
        std::shared_ptr<uint8_t> rgba(new uint8_t[px * py * 4],
                                      [] (uint8_t * p) { delete[] p; });
        i = 0;
        for (size_t y = 0;  y < py;  ++y) {
            for (size_t x = 0;  x < px;  ++x, ++i) {
                rgba.get()[i * 4 + 0] = r[i];
                rgba.get()[i * 4 + 1] = g[i];
                rgba.get()[i * 4 + 2] = b[i];
                rgba.get()[i * 4 + 3] = a[i];
            }
        }
        
        auto tensor = ExpressionValue::embedding
            (Date::notADate(), rgba, ST_UINT8, { py, px, 4 });

        ExternalFunction fn = lookupFunction("tf_EncodePng");
        
        SqlExpressionParamScope pscope(scope);

        BoundSqlExpression param1 = SqlExpression::parse("$1")->bind(pscope);
        param1.info.reset(new EmbeddingValueInfo({(ssize_t)px, (ssize_t)py, 4}, ST_UINT8));
        BoundSqlExpression param2 = SqlExpression::parse("{}")->bind(pscope);
        BoundFunction boundFn = fn("tf_EncodePng", { param1, param2 }, pscope);

        BoundParameters params;

        auto rowScope = pscope.getRowScope(params);
        
        vector<ExpressionValue> args { tensor, param2.constantValue() };

        auto res = boundFn(args, rowScope);

        cerr << "got result" << endl;

        {
            filter_ostream stream("lidar.png");
            
            stream.write((const char *)res.getAtom().blobData(),
                         res.getAtom().blobLength());
            stream.close();
        }

        // First argument is the tensor, a 

        //var expr = SqlExpression::parse("tf_EncodePng($vals)");

        cerr << "got " << activePixels << " active and " << inactivePixels
             << " inactive pixels" << endl;

        RunOutput result;
        return result;
    }
};

RegisterProcedureType<CreateImageProcedure, CreateImageConfig>
regSvd(builtinPackage(),
       "Create an image out of a projected value",
       "");


/*****************************************************************************/
/* LAS FILE IMPORTER                                                         */
/*****************************************************************************/

struct ImportLasConfig: public ProcedureConfig {
    static constexpr const char * name = "lidar.import.las";
    ImportLasConfig()
        : limit(-1)
    {
        outputDataset.withType("tabular");
    }

    /// The URL of the LAS file we're importing
    Url dataFileUrl;

    /// The output dataset.  Rows will be dumped into here via insertRows.
    PolyConfigT<Dataset> outputDataset;

    /// Limit to the number of points to import
    int64_t limit;
};

DECLARE_STRUCTURE_DESCRIPTION(ImportLasConfig);

DEFINE_STRUCTURE_DESCRIPTION(ImportLasConfig);

ImportLasConfigDescription::
ImportLasConfigDescription()
{
    addParent<ProcedureConfig>();

    addField("dataFileUrl", &ImportLasConfig::dataFileUrl,
             "");
    addField("outputDataset", &ImportLasConfig::outputDataset,
             "Dataset to record the data into.",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("limit", &ImportLasConfig::limit,
             "Limit to the number of fields to import",
             (int64_t)-1);
}

inline uint8_t host_to_be(uint8_t v)
{
    return v;
}

inline uint8_t be_to_host(uint8_t v)
{
    return v;
}

inline uint16_t host_to_be(uint16_t v)
{
    return htobe16(v);
}

inline uint16_t be_to_host(uint16_t v)
{
    return be16toh(v);
}

inline uint32_t host_to_be(uint32_t v)
{
    return htobe32(v);
}

inline uint32_t be_to_host(uint32_t v)
{
    return be32toh(v);
}

inline uint64_t host_to_be(uint64_t v)
{
    return htobe64(v);
}

inline uint64_t be_to_host(uint64_t v)
{
    return be64toh(v);
}

inline uint8_t host_to_le(uint8_t v)
{
    return v;
}

inline int8_t host_to_le(int8_t v)
{
    return v;
}

inline uint8_t le_to_host(uint8_t v)
{
    return v;
}

inline int8_t le_to_host(int8_t v)
{
    return v;
}

inline uint16_t host_to_le(uint16_t v)
{
    return htole16(v);
}

inline uint16_t le_to_host(uint16_t v)
{
    return le16toh(v);
}

inline int16_t host_to_le(int16_t v)
{
    return htole16(v);
}

inline int16_t le_to_host(int16_t v)
{
    return le16toh(v);
}

inline uint32_t host_to_le(uint32_t v)
{
    return htole32(v);
}

inline uint32_t le_to_host(uint32_t v)
{
    return le32toh(v);
}

inline int32_t host_to_le(int32_t v)
{
    return htole32(v);
}

inline int32_t le_to_host(int32_t v)
{
    return le32toh(v);
}

inline uint64_t host_to_le(uint64_t v)
{
    return htole64(v);
}

inline uint64_t le_to_host(uint64_t v)
{
    return le64toh(v);
}

inline int64_t host_to_le(int64_t v)
{
    return htole64(v);
}

inline int64_t le_to_host(int64_t v)
{
    return le64toh(v);
}

template<typename Base>
struct BigEndian {
    Base val;

    operator Base () const
    {
        return be_to_host(val);
    }

    BigEndian & operator = (Base val)
    {
        this->val = host_to_be(val);
        return *this;
    }
};

template<typename Base>
struct LittleEndian {
    Base val;

    operator Base () const
    {
        return le_to_host(val);
    }

    LittleEndian & operator = (Base val)
    {
        this->val = host_to_le(val);
        return *this;
    }
};

typedef LittleEndian<uint16_t> uint16_le;
typedef LittleEndian<int16_t> int16_le;
typedef LittleEndian<uint32_t> uint32_le;
typedef LittleEndian<int32_t> int32_le;
typedef LittleEndian<uint64_t> uint64_le;
typedef LittleEndian<int64_t> int64_le;

struct LasHeader {
    char signature[4];  // File Signature (“LASF”) char[4] 4 bytes *
    uint16_t sourceId;     // File Source ID unsigned short 2 bytes *
    uint16_t encoding;     // Global Encoding unsigned short 2 bytes *
    char guid[16];         // Project ID - GUID data 1 unsigned long 4 bytes
    //                        Project ID - GUID data 2 unsigned short 2 byte
    //                        Project ID - GUID data 3 unsigned short 2 byte
    //                        Project ID - GUID data 4 unsigned char[8] 8 bytes
    uint8_t versionMajor;  // Version Major unsigned char 1 byte *
    uint8_t versionMinor;  // Version Minor unsigned char 1 byte *
    char systemIdentifier[32]; // System Identifier char[32] 32 bytes *
    char softwareIdentifier[32];  // Generating Software char[32] 32 bytes *
    uint16_le fileCreationDayOfYear;  // File Creation Day of Year unsigned short 2 bytes *
    uint16_le fileCreationYear;       // File Creation Year unsigned short 2 bytes *
    uint16_le headerSize;             // Header Size unsigned short 2 bytes *
    uint32_le pointDataOffset;        // Offset to point data unsigned long 4 bytes *
    uint32_le numVariableLengthRecords; // Number of Variable Length Records unsigned long 4 bytes *
    uint8_t pointDataFormat;           // Point Data Record Format unsigned char 1 byte *
    uint16_le pointDataRecordLength;   // Point Data Record Length unsigned short 2 bytes *
    uint32_le legacyNumberOfPoints;    // Legacy Number of point records unsigned long 4 bytes *
    uint32_le legacyNumberOfPointsByReturn[5];  // Legacy Number of points by return unsigned long [5] 20 bytes *
    double xScaleFactor;  // X scale factor double 8 bytes *
    double yScaleFactor;  // Y scale factor double 8 bytes *
    double zScaleFactor;  // Z scale factor double 8 bytes *
    double xOffset;       // X offset double 8 bytes *
    double yOffset;       // Y offset double 8 bytes *
    double zOffset;       // Z offset double 8 bytes *
    double maxX;          // Max X double 8 bytes *
    double minX;          // Min X double 8 bytes *
    double maxY;          // Max Y double 8 bytes *
    double minY;          // Min Y double 8 bytes *
    double maxZ;          // Max Z double 8 bytes *
    double minZ;          // Min Z double 8 bytes *
    uint64_le packetDataoffset;  // Start of Waveform Data Packet Record Unsigned long long 8 bytes *
    uint64_le extendedVariableLengthOffset;  // Start of first Extended Variable Length Record unsigned long long 8 bytes *
    uint32_le numExtendedVariableLengthRecords;  // Number of Extended Variable Length Records unsigned long 4 bytes *
    uint64_le numPointRecords;           // Number of point records unsigned long long 8 bytes *
    uint64_le numPointRecordsByReturn[15];  // Number of points by return unsigned long long [15] 120 bytes     

    uint64_t numberOfPoints() const
    {
        if (legacyNumberOfPoints)
            return legacyNumberOfPoints;
        else return numPointRecords;
    }
} JML_PACKED;

struct LasPointRecordV0 {
    int32_le x;  // X long 4 bytes *
    int32_le y;  // Y long 4 bytes *
    int32_le z;  // Z long 4 bytes *
    uint16_le intensity;  // Intensity unsigned short 2 bytes
    uint8_t   returnNumber:3;  // Return Number 3 bits (bits 0 – 2) 3 bits *
    uint8_t   numberOfReturns: 3;  // Number of Returns (given pulse) 3 bits (bits 3 – 5) 3 bits *
    uint8_t scanDirection:1;  // Scan Direction Flag 1 bit (bit 6) 1 bit *
    uint8_t edgeOfFlight:1;   // Edge of Flight Line 1 bit (bit 7) 1 bit *
    uint8_t classification;   // Classification unsigned char 1 byte *
    uint8_t scanAngle;      // Scan Angle Rank (-90 to +90) – Left side char 1 byte *
    uint8_t userData;       // User Data unsigned char 1 byte
    uint16_le pointSourceId;  // Point Source ID unsigned short 2 bytes *
} JML_PACKED;

struct LasPointRecordV3: public LasPointRecordV0 {
    double gpsTime;
    uint16_le r, g, b;
} JML_PACKED;

static_assert(sizeof(LasPointRecordV0) == 20, "LasPointRecordV0 size is wrong");
static_assert(sizeof(LasPointRecordV3) == 34, "LasPointRecordV3 size is wrong");

struct ImportLasProcedure: public Procedure {

    ImportLasProcedure(MldbServer * owner,
                       PolyConfig config,
                       const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        this->config = config.params.convert<ImportLasConfig>();
    }

    ImportLasConfig config;

    virtual Any getStatus() const
    {
        return Any();
    }

    virtual RunOutput
    run(const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);

        string filename = Url::decodeUri(config.dataFileUrl.toUtf8String()).rawString();

        // Ask for a memory mappable stream if possible
        Datacratic::filter_istream stream(filename, { { "mapped", "true" } });

        LasHeader header;
        stream.read((char *)&header, sizeof(header));

        cerr << "version "
             << (int)header.versionMajor << "."
             << (int)header.versionMinor << endl;
        cerr << header.systemIdentifier << " " << header.softwareIdentifier << endl;
        cerr << "header.legacyNumberOfPoints = " << header.legacyNumberOfPoints << endl;
        cerr << "header.legacyNumberOfPointsByReturn[0] = " << header.legacyNumberOfPointsByReturn[0] << endl;
        cerr << "header.numPointRecords = " << header.numPointRecords << endl;
        cerr << "contains " << header.numberOfPoints() << " points at "
             << header.pointDataRecordLength << " bytes each" << endl;
        cerr << "in format " << (int)header.pointDataFormat << endl;

        cerr << "fileCreationYear " << header.fileCreationYear << endl;
        cerr << "fileCreationDayOfYear " << header.fileCreationDayOfYear << endl;
        cerr << "scale " << header.xScaleFactor << " " << header.yScaleFactor
             << " " << header.zScaleFactor << endl;
        //auto onProgress2 = [&] (const Json::Value & progress)
        //    {
        //        Json::Value value;
        //        value["dataset"] = progress;
        //        return onProgress(value);
        //    };

        std::shared_ptr<Dataset> dataset
            = createDataset(server, config.outputDataset, onProgress,
                            true /*overwrite*/);

        Dataset::MultiChunkRecorder recorder
            = dataset->getChunkRecorder();

        vector<ColumnName> inputColumnNames;
        for (Utf8String n: { "x", "y", "z", "r", "g", "b" }) {
            inputColumnNames.push_back(PathElement(n));
        }

        size_t blockSize = 65536;

        ML::Timer timer;

        std::atomic<int64_t> recordsDone(0);

        auto doTiming = [&] ()
            {
                double wall = timer.elapsed_wall();
                cerr << "done " << recordsDone << " in " << wall
                     << "s at " << recordsDone / wall * 0.000001 << "M records/second on "
                     << timer.elapsed_cpu() / timer.elapsed_wall() << " CPUs" << endl;
            };

        auto onChunk = [&] (const char * chunkStart,
                            size_t chunkLength,
                            int64_t chunkNumber)
            {
                auto threadRecorder = recorder.newChunk(chunkNumber);
                auto specializedRecorder
                    = threadRecorder->specializeRecordTabular(inputColumnNames);

                size_t firstElement = chunkNumber * blockSize;
                if (config.limit != -1 && firstElement > config.limit)
                    return true;
                
                size_t lastElement = std::min(firstElement + blockSize,
                                              header.numberOfPoints());
                bool lastChunk = false;
                if (config.limit != -1) {
                    lastElement = std::min<size_t>(lastElement, config.limit);
                    lastChunk = (lastElement == config.limit);
                }
                size_t numPoints = lastElement - firstElement;

                std::vector<CellValue> values(inputColumnNames.size());

                for (size_t i = 0;  i < numPoints;  ++i) {
                    const auto * record
                        = reinterpret_cast<const LasPointRecordV3 *>
                        (chunkStart + i * header.pointDataRecordLength);

                    values[0] = (int32_t)record->x;
                    values[1] = (int32_t)record->y;
                    values[2] = (int32_t)record->z;
                    values[3] = (uint32_t)record->r;
                    values[4] = (uint32_t)record->g;
                    values[5] = (uint32_t)record->b;
                    
                    Date ts;
#if 0
                    if (header.encoding & 1) {
                        cerr << "standard GPS time" << endl;
                    }
                    else {
                        cerr << "GPS week time" << endl;
                        static const Date origin(1980, 1, 6);
                        ts = origin.plusSeconds(record->gpsTime * 0.000001);
                    }

                    //Date ts = Date::fromSecondsSinceEpoch(record->gpsTime * 0.001);
                    if (chunkNumber == 0) {
                        cerr << "record->gpsTime = " << CellValue(record->gpsTime) << " now " << CellValue(Date::now().secondsSinceEpoch()) << endl;
                        cerr << "ts = " << ts << endl;
                    }
#endif

                    RowName rowName(i + firstElement);

                    specializedRecorder(std::move(rowName),
                                        ts, values.data(),
                                        values.size(), {});
                }

                threadRecorder->finishedChunk();
                
                recordsDone += numPoints;

                doTiming();

                return true;  //!lastChunk;
            };
        
        // Skip to the actual data before we import it
        stream.seekg(header.pointDataOffset, std::ios::beg);
        
        forEachChunk(stream, onChunk, blockSize * header.pointDataRecordLength,
                     -1 /* max chunks */, numCpus() /* max parallelism */);
        
        recorder.commit();

        dataset->commit();
        
        doTiming();
        
#if 0
        auto onLine = [&] (const char * line,
                           size_t length,
                           int chunkNum,
                           int64_t lineNum)
        {
            int64_t actualLineNum = lineNum + lineOffset;
#if 1
            uint64_t linesDone = totalLinesProcessed.fetch_add(1);

            if (linesDone && linesDone % 1000000 == 0) {
                double wall = timer.elapsed_wall();
                cerr << "done " << linesDone << " in " << wall
                     << "s at " << linesDone / wall * 0.000001 << "M lines/second on "
                     << timer.elapsed_cpu() / timer.elapsed_wall() << " CPUs" << endl;
            }
#endif

            // Values that come in from the CSV file
            // TODO: clang doesn't like a variable length array
            // here.  Find another way to allocate it on the
            // stack.
            vector<CellValue> values(inputColumnNames.size());

            const char * lineStart = line;

            const size_t numInputColumn = inputColumnNames.size();

            const char * errorMsg
                    = parseFixedWidthCsvRow(line, length, &values[0],
                                            numInputColumn,
                                            separator, quote, encoding,
                                            replaceInvalidCharactersWith,
                                            isTextLine,
                                            hasQuoteChar);

            if (errorMsg) {
                if(config.allowMultiLines) {
                    // check if we hit an error meaning we probably
                    // have a multiline error
                    if(errorMsg == unclosedQuoteError ||
                       errorMsg == notEnoughColsError) {
                        return false;
                    }
                }

                return handleError(errorMsg, actualLineNum,
                                   line - lineStart + 1,
                                   string(line, length));
            }

            auto row = scope.bindRow(&values[0], ts, actualLineNum,
                                         0 /* todo: chunk ofs */);

            // If it doesn't match the where, don't add it
            if (!isWhereTrue) {
                ExpressionValue storage;
                if (!whereBound(row, storage, GET_ALL).isTrue())
                    return true;
            }

            // Get the timestamp for the row
            Date rowTs = ts;
            ExpressionValue tsStorage;
            rowTs = timestampBound(row, tsStorage, GET_ALL)
                    .coerceToTimestamp().toTimestamp();

            ExpressionValue nameStorage;
            RowName rowName(namedBound(row, nameStorage, GET_ALL)
                                .toUtf8String());

            //ExcAssert(!(isIdentitySelect && outputColumnNamesUnknown));

            auto & threadAccum = accum.get();

            if (isIdentitySelect) {
                // If it's a select *, we don't really need to run the
                // select clause.  We simply go for it.
                threadAccum.specializedRecorder(std::move(rowName),
                                                rowTs, values.data(),
                                                values.size(), {});
            }
            else {
                // TODO: optimization for
                // SELECT * excluding (...)

                ExpressionValue selectStorage;
                const ExpressionValue & selectOutput
                        = selectBound(row, selectStorage, GET_ALL);

                if (&selectOutput == &selectStorage) {
                    // We can destructively work with it
                    threadAccum.threadRecorder
                        ->recordRowExprDestructive(std::move(rowName),
                                                   std::move(selectStorage));
                    }
                    else {
                        // We don't own the output; we will need to copy
                        // it.
                        threadAccum.threadRecorder
                            ->recordRowExpr(std::move(rowName),
                                            selectOutput);
                }
            }

            return true;
        };
#endif



        RunOutput result;
        return result;
    }
};

RegisterProcedureType<ImportLasProcedure, ImportLasConfig>
regImportLas(builtinPackage(),
             "Create an image out of a projected value",
             "");

} // namespace MLDB
} // namespace Datacratic

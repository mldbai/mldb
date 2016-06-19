#include "mldb/core/procedure.h"
#include "mldb/types/url.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/analytics.h"
#include "mldb/types/any_impl.h"
#include "mldb/base/parallel.h"
#include "mldb/vfs/filter_streams.h"
#include <iostream>
#include <png.h>

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
    
    auto options
        = jsonDecode<ZRenderOptions>(optionsBound.constantValue().extractJson());
    
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

        auto onProgress2 = [&] (const Json::Value & progress)
            {
                Json::Value value;
                value["dataset"] = progress;
                return onProgress(value);
            };

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

        size_t px = 2048;
        size_t py = 2048;

        std::unique_ptr<ZEntry []> pixels(new ZEntry[px * py]);

        uint64_t inside = 0, outside = 0;

        double minX = INFINITY, maxX = -INFINITY,
            minY = INFINITY, maxY = -INFINITY,
            minZ = INFINITY, maxZ = -INFINITY;

        // Record a value in pixel coordinates
        auto recordValue = [&] (double x, double y, double z,
                                uint8_t r, uint8_t g, uint8_t b)
            {
                minX = std::min(minX, x);
                maxX = std::max(maxX, x);
                minY = std::min(minY, y);
                maxY = std::max(maxY, y);
                minZ = std::min(minZ, z);
                maxZ = std::max(maxZ, z);

                //cerr << "x = " << x << " y = " << y << " z = " << z << endl;

                size_t xi = x, yi = y;

                if (xi < 0 || xi > px || yi < 0 || yi > py /*|| z < 0*/) {
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

        auto doRow = [&] (double x, double y, double z, uint8_t r, uint8_t g, uint8_t b)
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

        queryFromStatement(onResult, *runProcConf.trainingData.stm, scope);

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

} // namespace MLDB
} // namespace Datacratic

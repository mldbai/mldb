/** builtin_geo_functions.cc
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Builtin geometric functions for SQL.
*/

#include "mldb/sql/builtin_functions.h"
#include "mldb/ext/s2geometry/src/s2/s2latlng.h"
#include "mldb/ext/s2geometry/src/s2/s2polygon.h"
#include "mldb/ext/s2geometry/src/s2/s2loop.h"
#include "mldb/ext/s2geometry/src/s2/s2builder.h"
#include "mldb/ext/s2geometry/src/s2/s2builderutil_s2polygon_layer.h"
#include "mldb/types/basic_value_descriptions.h"


using namespace std;



namespace MLDB {
namespace Builtins {

/*****************************************************************************/
/* GEOGRAPHICAL FUNCTIONS                                                    */
/*****************************************************************************/

static constexpr double EARTH_EQUATORIAL_RADIUS_METERS = 6378137.0;
static constexpr double EARTH_POLAR_RADIUS_METERS      = 6356752.3;

// https://en.wikipedia.org/w/index.php?title=Earth_radius&action=edit&section=16
static constexpr double EARTH_MEAN_RADIUS_METERS       = 6371008.8;

BoundFunction geo_distance(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 4, __FUNCTION__);

    auto outputInfo
        = std::make_shared<Float64ValueInfo>();

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 4);

                Date ts = calcTs(args[0], args[1], args[2], args[3]);

                if (args[0].empty() || args[1].empty()
                    || args[2].empty() || args[3].empty())
                    return ExpressionValue::null(ts);

                double lat1 = args[0].getAtom().toDouble();
                double lon1 = args[1].getAtom().toDouble();
                double lat2 = args[2].getAtom().toDouble();
                double lon2 = args[3].getAtom().toDouble();

                S2LatLng point1 = S2LatLng::FromDegrees(lat1, lon1).Normalized();
                S2LatLng point2 = S2LatLng::FromDegrees(lat2, lon2).Normalized();

                double dist = point1.GetDistance(point2).radians()
                    * EARTH_MEAN_RADIUS_METERS;

                return ExpressionValue(dist, ts);
            },
            outputInfo
            };
}

static RegisterBuiltin registerGeoDistance(geo_distance, "geo_distance");

BoundFunction st_contains(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 3, __FUNCTION__);

    auto outputInfo
        = std::make_shared<BooleanValueInfo>();
    
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
    {
      //
        checkArgsSize(args.size(), 3);

        auto getCol = [] (const ExpressionValue & eVal,
                          const PathElement & columnName,
                          ExpressionValue & storage)
            -> const ExpressionValue &
            {
                const ExpressionValue * col = eVal.tryGetColumn(columnName,
                                                                storage);
                if (!col) {
                    throw MLDB::Exception("Cound not find required column '"+
                                          columnName.toUtf8String().rawString()+"'");
                }
                return *col;
            };
        
        if(!args[0].isRow()) {
            throw MLDB::Exception("argument 1 must be a row representing a GeoJson geometry");
        }

        //cerr << "parsing geometry " << args[0].extractJson() << endl;
        
        // GeoJson should have a type key telling us we are dealing
        // with a polygon
        ExpressionValue typeStorage;
        const ExpressionValue & typeCol = getCol(args[0], "type", typeStorage);

        string geomType = typeCol.getAtom().toString();
        if(geomType != "Polygon" && geomType != "MultiPolygon")
            throw MLDB::Exception("unknown polygon type: " + geomType);

        //cerr << "geo type is " << geomType << endl;

        ExpressionValue coordsStorage;
        const ExpressionValue & coordsCol = getCol(args[0], "coordinates",
                                                   coordsStorage);

        // Avoid allocations by keeping it here
        vector<S2Point> points;

        auto parsePolygon = [&points]
            (S2Builder & polyBuilder,
             const ExpressionValue& coords)
        {
            //cerr << "parsePolygon " << coords.extractJson() << endl;

            auto onCol = [&] (const PathElement & el,
                              const ExpressionValue & coordi) -> bool
            {
                size_t numPt = coordi.rowLength();
                points.clear();
                points.reserve(numPt);

                std::function<bool (const PathElement & columnName,
                                    const ExpressionValue & val)>
                onPoint = [&] (const PathElement & columnName,
                               const ExpressionValue & val) -> bool
                {
                    auto extractDouble = [&] (int el) -> double
                    {
                        ExpressionValue storage;
                        const ExpressionValue * v
                        = val.tryGetColumn(el, storage);
                        if (!v)
                            throw HttpReturnException
                                (400, "GeoJSON points should be [lat,long]; got "
                                 + jsonEncodeStr(val.extractJson()));
                        return v->getAtom().toDouble();
                    };
                    
                    double lat1 = extractDouble(1);
                    double lon1 = extractDouble(0);
                    
                    points.emplace_back(S2LatLng::FromDegrees(lat1, lon1)
                                        .Normalized().ToPoint());
                        
                    // Don't add a degenerate point
                    if (points.size() > 1
                        && points.back() == points[points.size() - 2])
                        points.pop_back();

                    return true;
                };

                coordi.forEachColumn(onPoint);
                
                // Loop is implicitly closed, so if it's explicitly closed
                // remove the last value
                if (!points.empty() && points.front() == points.back())
                    points.pop_back();

                //cerr << "loop has " << points.size() << " points" << endl;

                // It's not possible to define a polygon with less than three
                // edges; this will cause an exception if we let it pass but
                // occurs in the wild.
                if (points.size() < 3)
                    return true;
                
                S2Loop loop(points);

                S2Error error;
                if (loop.FindValidationError(&error)) {
                    // Note that there may be some points filtered out
                    cerr << "error in loop: " << error.text() << endl;
                    cerr << "points.size() = " << points.size() << endl;
                    cerr << coords.extractJson() << endl;

                    //throw HttpReturnException
                    //    (400, "Error interpreting GeoJson polygon: S2: "
                    //     + error.text(),
                    //     "coordinates", coords.extractJson(),
                    //     "numDistinctPointsExcludingDuplicates",
                    //     points.size());                    
                }
                
                // https://tools.ietf.org/html/rfc7946#section-3.1.6
                // Anything apart from the first is interior, ie a hole
                // in the first.  This makes it depth one.
                if(el.toIndex()>0) 
                    loop.set_depth(1);

                //cerr << "valid = " << loop.IsValid() << endl;
                //cerr << "normalized = " << loop.IsNormalized() << endl;
                //cerr << "area = " << loop.GetArea() << endl;

                // Many in the wild geo-JSON files don't respect exct orders
                loop.Normalize();
                
                polyBuilder.AddLoop(std::move(loop));

                return true;
            };

            coords.forEachColumn(onCol);
        };

        S2Builder::Options options;
        S2Builder polyBuilder(options);
        S2Polygon poly;
        polyBuilder.StartLayer
            (absl::make_unique<s2builderutil::S2PolygonLayer>(&poly));

        if(geomType == "Polygon") {
            parsePolygon(polyBuilder, coordsCol);
        }
        else if(geomType == "MultiPolygon") {

            std::function<bool (const PathElement & columnName,
                                const ExpressionValue & val)>
            onPolygon = [&] (const PathElement & columnName,
                             const ExpressionValue & val) -> bool
            {
                S2Builder multiPolyBuilder(options);                
                S2Polygon poly;
                multiPolyBuilder.StartLayer
                    (absl::make_unique<s2builderutil::S2PolygonLayer>(&poly));
                parsePolygon(multiPolyBuilder, val);

                S2Error error;
                if (!multiPolyBuilder.Build(&error)) {
                    throw MLDB::Exception("unable to assemble polygon: "
                                          + error.text());
                }

                polyBuilder.AddPolygon(std::move(poly));

                return true;
            };

            coordsCol.forEachColumn(onPolygon);

            
        }
        else {
            ExcAssert(false); //tested above
        }
        
        S2Error error;
        if (!polyBuilder.Build(&error)) {
            throw MLDB::Exception("unable to assemble polygon: "
                                  + error.text());
        }
        
        double lat1 = args[1].getAtom().toDouble();
        double lon1 = args[2].getAtom().toDouble();

        S2LatLng point1 = S2LatLng::FromDegrees(lat1, lon1).Normalized();

        bool contains = poly.Contains(point1.ToPoint());

        return ExpressionValue(
                contains,
                Date());

    },
    outputInfo
    };
}

static RegisterBuiltin registerST_Contains(st_contains, "ST_Contains_Point");

} // namespace Builtins
} // namespace MLDB


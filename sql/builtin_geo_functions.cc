/** builtin_geo_functions.cc
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Builtin geometric functions for SQL.
*/

#include "mldb/sql/builtin_functions.h"
#include "mldb/ext/s2/s2.h"
#include "mldb/ext/s2/s2latlng.h"
#include "mldb/ext/s2/s2polygon.h"
#include "mldb/ext/s2/s2loop.h"
#include "mldb/ext/s2/s2polygonbuilder.h"
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
        checkArgsSize(args.size(), 3);

        auto getCol = [] (const ExpressionValue & eVal,
                          const PathElement & columnName)
        {
            ExpressionValue col = eVal.getColumn(columnName);
            if (col.empty()) {
                throw MLDB::Exception("Cound not find required column '"+
                        columnName.toUtf8String().rawString()+"'");
            }
            return std::move(col);
        };

        if(!args[0].isRow()) {
            throw MLDB::Exception("argument 1 must be a row representing a GeoJson geometry");
        }

        // GeoJson should have a type key telling us we are dealing
        // with a polygon
        ExpressionValue typeCol = getCol(args[0], "type");
        string geomType = typeCol.getAtom().toString();
        if(geomType != "Polygon" && geomType != "MultiPolygon")
            throw MLDB::Exception("polygon!!! " + geomType);


        ExpressionValue coordsCol = getCol(args[0], "coordinates");

        Json::Value coordinatesJs = std::move(coordsCol.extractJson());


        vector<S2Loop*> loops;
        auto parsePolygon = [&loops] (S2PolygonBuilder & polyBuilder,
                                const Json::Value & polygonJs)
        {
            // For type "Polygon", the "coordinates" member must be an 
            // array of LinearRing coordinate arrays. For Polygons with 
            // multiple rings, the first must be the exterior ring and
            // any others must be interior rings or holes.
            // http://geojson.org/geojson-spec.html#polygon
            vector<S2Point> points;
            points.reserve(polygonJs.size());

            for(int i=0; i<polygonJs.size(); i++) {
                //cout << "   getting key: " << std::to_string(i) << endl;
                const Json::Value & currPoint = polygonJs[std::to_string(i)];
                //cout << currPoint.toStyledString() << endl;

                if(currPoint.size() != 2)
                    throw MLDB::Exception("wrong zie!!");

                double lat1 = currPoint["1"].asDouble();
                double lon1 = currPoint["0"].asDouble();
                points.emplace_back(S2LatLng::FromDegrees(lat1, lon1).Normalized().ToPoint());

                //S2Loop loop(points);
                loops.push_back(new S2Loop(points));
                if(i>0) loops.back()->set_depth(1);  // TODO should this be recursive?
            }
                
            polyBuilder.AddLoop(loops.back());
        };

        S2PolygonBuilderOptions options;
        S2PolygonBuilder polyBuilder(options);

        vector<S2Polygon*> polygons;
        if(geomType != "Polygon") {
            parsePolygon(polyBuilder, coordinatesJs);
        }
        else if(geomType != "MultiPolygon") {
            // go over each polygon
            for(int i=0; i<coordinatesJs.size(); i++) {
                S2PolygonBuilder multiPolyBuilder(options);
                parsePolygon(multiPolyBuilder, coordinatesJs[std::to_string(i)]);

                polygons.push_back(new S2Polygon);//S2Polygon poly;
                S2PolygonBuilder::EdgeList unused_edges;
                if(!multiPolyBuilder.AssemblePolygon(polygons.back(), &unused_edges)) {
                    throw MLDB::Exception("unable to assemble polygon!");
                }
                polyBuilder.AddPolygon(polygons.back());
            }

        }
        else {
            throw MLDB::Exception("unknown polygon type!");
        }

        S2Polygon poly;
        S2PolygonBuilder::EdgeList unused_edges;
        if(!polyBuilder.AssemblePolygon(&poly, &unused_edges)) {
            throw MLDB::Exception("unable to assemble polygon!");
        }



        double lat1 = args[1].getAtom().toDouble();
        double lon1 = args[2].getAtom().toDouble();
        S2LatLng point1 = S2LatLng::FromDegrees(lat1, lon1).Normalized();


        return ExpressionValue(
                poly.Contains(point1.ToPoint()),
                Date());
    },
    outputInfo
    };


    // s2poylgon.Contains(S2Polygon const* b)

}

static RegisterBuiltin registerST_Contains(st_contains, "ST_Contains_Point");

} // namespace Builtins
} // namespace MLDB


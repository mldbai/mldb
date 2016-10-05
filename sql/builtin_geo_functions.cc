/** builtin_geo_functions.cc
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Builtin geometric functions for SQL.
*/

#include "mldb/sql/builtin_functions.h"
#include "mldb/ext/s2/s2.h"
#include "mldb/ext/s2/s2latlng.h"
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

static RegisterBuiltin registerFlatten(geo_distance, "geo_distance");



} // namespace Builtins
} // namespace MLDB


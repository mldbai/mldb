// This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

/* info_override_feature_space.cc               -*- C++ -*-
   Mathieu Marquis Bolduc, 22 March 2017

   Feature space that wraps a const feature place and allow feature info to be 
   overriden
*/


#include "info_override_feature_space.h"

namespace ML {


Feature_Info 
Info_Override_Feature_Space::
info(const Feature & feature) const {
    auto it = overridenInfo.find(feature);
    if (it != overridenInfo.end())
        return it->second;
    else 
        return inner->info(feature);
}

void 
Info_Override_Feature_Space::
set_info(const ML::Feature & feature, const ML::Feature_Info & info)
{
    overridenInfo[feature] = info;
}

void 
Info_Override_Feature_Space::
serialize(DB::Store_Writer & store) const {
    throw MLDB::Exception("Cannot serialize Info_Override_Feature_Space");
}
void 
Info_Override_Feature_Space::
reconstitute(DB::Store_Reader & store,
                            const std::shared_ptr<const Feature_Space> & fs) {
    throw MLDB::Exception("Cannot reconstitute Info_Override_Feature_Space");
}
    
std::string 
Info_Override_Feature_Space::
class_id() const 
{
    return "MLDB::Info_Override_Feature_Space";
}

Feature_Space * 
Info_Override_Feature_Space::
make_copy() const
{
    return new Info_Override_Feature_Space(*this);
}

}
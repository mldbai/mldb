// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* rest_entity.cc
   Jeremy Barnes, 15 September 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

*/

#include "rest_entity.h"
#include "mldb/types/structure_description.h"
#include "mldb/http/http_exception.h"

namespace Datacratic {

DEFINE_STRUCTURE_DESCRIPTION(RestEntityChildEvent);

RestEntityChildEventDescription::
RestEntityChildEventDescription()
{
}


void
RestRouteManager::
throwChildAlreadyAdded(const Utf8String & name)
{
    throw HttpReturnException(400, "Child of '" + name + "' already added");
}

} // namespace Datacratic

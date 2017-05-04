/** mutable_behavior_view.h                                 -*- C++ -*-
    Rémi Attab, 28 Apr 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    // This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/plugins/behavior/id.h"


namespace MLDB {

struct MutableBehaviorDomain;
struct PathElement;
struct Id;

namespace behaviors {

/******************************************************************************/
/* ENCODE / DECODE COLUMN                                                     */
/******************************************************************************/

// Encodes the given column name and value into a behavior Id.  Not for
// legacy systems.
Id encodeColumn(const ColumnPath& name, const CellValue& value);

// Decodes the given behviour Id into it's column name and value.  This is
// only called for legacy systems.
ColumnPath decodeLegacyColumnPath(Id id);

// Decodes a column from a behavior file.  If couldBeLegacy is true, then
// it may be a legacy behavior file which needs special processing to get
// the column name out.
std::pair<ColumnPath, CellValue>
decodeColumn(const Id & id, bool couldBeLegacy);

// Efficient conversion from Id to PathElement and from PathElement to Id
PathElement toPathElement(const Id & id);
Id toId(const PathElement & coord);
Id toId(const Path & coord);

} // namespace behaviors
} // namespace MLDB 


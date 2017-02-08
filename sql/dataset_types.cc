/* dataset_types.cc
   Jeremy Barnes, 21 January 2016
   Copyright (c) 2016 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Dataset types support.
*/

#include "dataset_types.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/tuple_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/hash_wrapper_description.h"



namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION(MatrixRow);

MatrixRowDescription::
MatrixRowDescription()
{
    addField("rowName", &MatrixRow::rowName, "Name of the row");
    addField("columns", &MatrixRow::columns, "Columns active for this row");
}

DEFINE_STRUCTURE_DESCRIPTION(MatrixNamedRow);

MatrixNamedRowDescription::
MatrixNamedRowDescription()
{
    addField("rowName", &MatrixNamedRow::rowName, "Name of the row");
    addField("columns", &MatrixNamedRow::columns, "Columns active for this row");
}

DEFINE_STRUCTURE_DESCRIPTION(MatrixEvent);

MatrixEventDescription::
MatrixEventDescription()
{
    addField("rowName", &MatrixEvent::rowName, "Name of the row");
    addField("timestamp", &MatrixEvent::timestamp, "Timestamp of event");
    addField("columns", &MatrixEvent::columns, "Columns active for this event");
}

DEFINE_STRUCTURE_DESCRIPTION(MatrixNamedEvent);

MatrixNamedEventDescription::
MatrixNamedEventDescription()
{
    addField("rowName", &MatrixNamedEvent::rowName, "Name of the row");
    addField("timestamp", &MatrixNamedEvent::timestamp, "Timestamp of event");
    addField("columns", &MatrixNamedEvent::columns, "Columns active for this event");
}

DEFINE_STRUCTURE_DESCRIPTION(MatrixColumn);

MatrixColumnDescription::
MatrixColumnDescription()
{
    addField("columnName", &MatrixColumn::columnName, "Name of the column");
    addField("rows", &MatrixColumn::rows, "Row values for this column");
}

} // namespace MLDB


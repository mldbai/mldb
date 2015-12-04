// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** dataset_types.h                                                -*- C++ -*-
    Jeremy Barnes, 16 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#pragma once

#include "cell_value.h"
#include "dataset_fwd.h"
#include "mldb/types/date.h"


namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* MATRIX ROW                                                                */
/*****************************************************************************/

struct MatrixRow {
    RowHash rowHash;
    RowName rowName;
    std::vector<std::tuple<ColumnHash, CellValue, Date> > columns;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixRow);


/*****************************************************************************/
/* MATRIX NAMED ROW                                                          */
/*****************************************************************************/

struct MatrixNamedRow {
    RowHash rowHash;
    RowName rowName;
    std::vector<std::tuple<ColumnName, CellValue, Date> > columns;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixNamedRow);


/*****************************************************************************/
/* MATRIX EVENT                                                              */
/*****************************************************************************/

struct MatrixEvent {
    RowHash rowHash;
    RowName rowName;
    Date timestamp;
    std::vector<std::tuple<ColumnHash, CellValue> > columns;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixEvent);


/*****************************************************************************/
/* MATRIX NAMED EVENT                                                        */
/*****************************************************************************/

struct MatrixNamedEvent {
    RowHash rowHash;
    RowName rowName;
    Date timestamp;
    std::vector<std::tuple<ColumnName, CellValue> > columns;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixNamedEvent);


/*****************************************************************************/
/* MATRIX COLUMN                                                             */
/*****************************************************************************/

struct MatrixColumn {
    ColumnHash columnHash;
    ColumnName columnName;
    std::vector<std::tuple<RowName, CellValue, Date> > rows;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixColumn);


} // namespace MLDB
} // namespace Datacratic

/** dataset_types.h                                                -*- C++ -*-
    Jeremy Barnes, 16 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "cell_value.h"
#include "dataset_fwd.h"
#include "mldb/types/date.h"
#include "path.h"



namespace MLDB {

/*****************************************************************************/
/* MATRIX ROW                                                                */
/*****************************************************************************/

struct MatrixRow {
    RowHash rowHash;
    RowPath rowName;
    std::vector<std::tuple<ColumnHash, CellValue, Date> > columns;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixRow);


/*****************************************************************************/
/* MATRIX NAMED ROW                                                          */
/*****************************************************************************/

struct MatrixNamedRow {
    RowHash rowHash;
    RowPath rowName;
    std::vector<std::tuple<ColumnPath, CellValue, Date> > columns;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixNamedRow);


/*****************************************************************************/
/* MATRIX EVENT                                                              */
/*****************************************************************************/

struct MatrixEvent {
    RowHash rowHash;
    RowPath rowName;
    Date timestamp;
    std::vector<std::tuple<ColumnHash, CellValue> > columns;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixEvent);


/*****************************************************************************/
/* MATRIX NAMED EVENT                                                        */
/*****************************************************************************/

struct MatrixNamedEvent {
    RowHash rowHash;
    RowPath rowName;
    Date timestamp;
    std::vector<std::tuple<ColumnPath, CellValue> > columns;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixNamedEvent);


/*****************************************************************************/
/* MATRIX COLUMN                                                             */
/*****************************************************************************/

struct MatrixColumn {
    ColumnHash columnHash;
    ColumnPath columnName;
    std::vector<std::tuple<RowPath, CellValue, Date> > rows;
};

DECLARE_STRUCTURE_DESCRIPTION(MatrixColumn);


} // namespace MLDB


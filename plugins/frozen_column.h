/** frozen_column.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Frozen (immutable), compressed column representations and methods
    to operate on them.
*/

#pragma once

#include "column_types.h"
#include <memory>


namespace MLDB {

struct TabularDatasetColumn;


/*****************************************************************************/
/* COLUMN FREEZE PARAMETERS                                                  */
/*****************************************************************************/

/** Parameters used to control the freeze operation. */
struct ColumnFreezeParameters {
};


/*****************************************************************************/
/* FROZEN COLUMN                                                             */
/*****************************************************************************/

/// Base class for a frozen column
struct FrozenColumn {
    virtual ~FrozenColumn()
    {
    }

    virtual CellValue get(uint32_t rowIndex) const = 0;

    virtual size_t size() const = 0;

    virtual size_t memusage() const = 0;

    typedef std::function<bool (size_t rowNum, const CellValue & val)> ForEachRowFn;

    virtual bool forEach(const ForEachRowFn & onRow) const = 0;

    virtual bool forEachDense(const ForEachRowFn & onRow) const = 0;

    virtual bool
    forEachDistinctValue(std::function<bool (const CellValue &)> fn)
        const = 0;

    virtual ColumnTypes getColumnTypes() const = 0;

    /** Freeze the given column into the best fitting frozen column type. */
    static std::shared_ptr<FrozenColumn>
    freeze(TabularDatasetColumn & column,
           const ColumnFreezeParameters & params);
};


} // namespace MLDB


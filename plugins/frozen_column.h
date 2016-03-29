/** frozen_column.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Frozen (immutable), compressed column representations and methods
    to operate on them.
*/

#pragma once

#include "column_types.h"
#include "mldb/sql/cell_value.h"

namespace Datacratic {
namespace MLDB {

struct TabularDatasetColumn;

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

    virtual bool forEachDistinctValue(std::function<bool (const CellValue &, size_t)> fn) const = 0;

    CellValue operator [] (size_t index) const
    {
        return this->get(index);
    }

    template<typename Fn>
    bool forEach(Fn && fn) const
    {
        // TODO: sparse columns have nulls...
        size_t sz = this->size();
        for (size_t i = 0;  i < sz;  ++i) {
            if (!fn(i, std::move(this->get(i))))
                return false;
        } 
        return true;
    }

    virtual ColumnTypes getColumnTypes() const = 0;

    static std::shared_ptr<FrozenColumn>
    freeze(TabularDatasetColumn & column);
};


} // namespace MLDB
} // namespace Datacratic

/** frozen_column.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Frozen (immutable), compressed column representations and methods
    to operate on them.
*/

#pragma once

#include "column_types.h"
#include "mldb/utils/log.h"
#include "mldb/plugins/tabular_dataset.h"
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
    FrozenColumn();
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

    std::shared_ptr<spdlog::logger> logger;
};


/*****************************************************************************/
/* FROZEN COLUMN FORMAT                                                        */
/*****************************************************************************/

/** This describes a format of frozen column.  These can be registered to
    provide cusomized compression for given formats of columns.
*/

struct FrozenColumnFormat {

    virtual ~FrozenColumnFormat();

    /** Return the name of this frozen column format. */
    virtual std::string format() const = 0;

    /** Quick test that tells us whether this particular column format can
        store the given column.  It may update cachedInfo to store information
        that is useful in the columnSize() and freeze() functions.
    */
    virtual bool isFeasible(const TabularDatasetColumn & column,
                            const ColumnFreezeParameters & params,
                            std::shared_ptr<void> & cachedInfo) const = 0;

    static constexpr const size_t CANT_STORE = -2;
    static constexpr const size_t NOT_BEST = -1;
    
    /** Calculate how much data will be required to store the given column
        in this column format.  If it's unable to, or can't beat the previousBest,
        then it should return CANT_STORE or NOT_BEST.

        It can both read and update the cachedInfo, which has been provided
        by the isFeasible method.

        If this method returns a positive value, it must be able to serialize
        the data using the freeze() method.
    */
    virtual ssize_t columnSize(const TabularDatasetColumn & column,
                               const ColumnFreezeParameters & params,
                               ssize_t previousBest,
                               std::shared_ptr<void> & cachedInfo) const = 0;
    
    /** Freeze the given column as this particular column format.  It has access
        to the cachedInfo that isFeasible and columnSize have provided.  This
        method should not fail unless there is an error in the underlying
        layers, eg a memory allocation error, in which case it should
        throw an exception.
    */
    virtual FrozenColumn *
    freeze(TabularDatasetColumn & column,
           const ColumnFreezeParameters & params,
           std::shared_ptr<void> cachedInfo) const = 0;
    
    /** Register a new column format.  Returns a handle that, once released,
        will de-register the column format.
    */
    static std::shared_ptr<void>
    registerFormat(std::shared_ptr<FrozenColumnFormat> format);
};


/*****************************************************************************/
/* REGISTER FROZEN COLUMN FORMAT TEMPLATE                                    */
/*****************************************************************************/

/** Helper class that allows a frozen column format to be registered. */

template<typename Format>
struct RegisterFrozenColumnFormatT {
    RegisterFrozenColumnFormatT()
    {
        handle = FrozenColumnFormat
            ::registerFormat(std::make_shared<Format>());
    }
    
    std::shared_ptr<void> handle;
};

} // namespace MLDB


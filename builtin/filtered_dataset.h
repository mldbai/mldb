/** filtered_dataset.h                                               -*- C++ -*-
    Guy Dumais, September 18th 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Dataset that filters some of the tuples out.  This is used namely to implement
    the SQL WHEN clause.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/types/value_description_fwd.h"


namespace MLDB {


/*****************************************************************************/
/* FILTERED DATASET                                                          */
/*****************************************************************************/

struct FilteredDataset : public Dataset {

    typedef std::function<bool(const CellValue & value, const Date & date)> TupleFilter;

    FilteredDataset(const Dataset & dataset, const TupleFilter & filter);
    virtual ~FilteredDataset();

    virtual Any getStatus() const;
    virtual void recordRowItl(const RowPath & rowName,
          const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        throw MLDB::Exception("Dataset type doesn't allow recording");
    }

    // these methods are usually not overriden by the dataset specialization - don't implement
    // void recordRow(const RowPath & rowName, const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);
    // virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);
    // virtual void recordColumn(const ColumnPath & columnName, const std::vector<std::tuple<RowPath, CellValue, Date> > & vals);
    // virtual void recordColumns(const std::vector<std::pair<ColumnPath, std::vector<std::tuple<RowPath, CellValue, Date> > > > & rows);

    // these methods have been overriden by the dataset specialized classes - implement redirect
    virtual KnownColumn getKnownColumnInfo(const ColumnPath & columnName) const;
    virtual BoundFunction overrideFunction(const Utf8String & tableName,
                                           const Utf8String & functionName,
                                           SqlBindingScope & context) const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    // TODO: if often used, this could be reasonably overridden here
    //virtual std::pair<Date, Date> getTimestampRange() const;

private:
    const Dataset &dataset;

    struct Itl;
    std::shared_ptr<Itl> itl;
};

} // namespace MLDB


// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** filtered_dataset.h                                               -*- C++ -*-
    Guy Dumais, September 18th 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Dataset that filters some of the tuples out.  This is used namely to implement
    the SQL WHEN clause.
*/

#pragma once

#include "mldb/server/dataset.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/types/value_description.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* FILTERED DATASET                                                          */
/*****************************************************************************/

struct FilteredDataset : public Dataset {

    typedef std::function<bool(const CellValue & value, const Date & date)> TupleFilter;

    FilteredDataset(const Dataset & dataset, const TupleFilter & filter);
    virtual ~FilteredDataset();

    virtual Any getStatus() const;
    virtual void recordRowItl(const RowName & rowName,
          const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
        throw ML::Exception("Dataset type doesn't allow recording");
    }

    // these methods are usually not overriden by the dataset specialization - don't implement
    // void recordRow(const RowName & rowName, const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals);
    // virtual void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows);
    // virtual void recordColumn(const ColumnName & columnName, const std::vector<std::tuple<RowName, CellValue, Date> > & vals);
    // virtual void recordColumns(const std::vector<std::pair<ColumnName, std::vector<std::tuple<RowName, CellValue, Date> > > > & rows);

    // these methods have been overriden by the dataset specialized classes - implement redirect
    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const;
    virtual BoundFunction overrideFunction(const Utf8String & functionName,
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
} // namespace Datacratic

// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** filtered_dataset.cc                          -*- C++ -*-
    Guy Dumais, September 18th, 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "mldb/server/dataset_collection.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/analytics.h"
#include "filtered_dataset.h"
#include "mldb/types/any_impl.h"


using namespace std;



namespace MLDB {

namespace {
struct BetweenFilter {
    bool notBetween;
    Date lower, upper;
    
    BetweenFilter(bool notBetween, const Date& lower, const Date& upper) :
    notBetween(notBetween), lower(lower), upper(upper) {}
    
    FilteredDataset::TupleFilter  eval() {
        // prevent the this pointer from being captured allowing syntax like BetweenFilter(...).eval()
        Date lower_ = lower;
        Date upper_ = upper;
        bool notBetween_ = notBetween;
        return [lower_, upper_, notBetween_](const CellValue & value, const Date & date) 
        {
            //cerr << "between " << lower_ << " " << upper_ << " " << date << endl;
            if (date < lower_) return notBetween_;
            if (date > upper_) return notBetween_;
            return !notBetween_;
        }; 
    }
};
                      
typedef std::function<bool(const Date & leftDate, const Date & rightDate)>  CompareOp;

struct CompareFilter {
    CompareOp op;
    Date constantDate;
    
    CompareFilter(const CompareOp & op, const Date& date) :
    op(op), constantDate(date) {}
    
    FilteredDataset::TupleFilter  eval() {
        // prevent the this pointer from being captured allowing syntax like CompareFilter(...).eval()
        CompareOp op_ = op;
        Date constantDate_ = constantDate;
        return [op_, constantDate_](const CellValue & value, const Date & date) 
        {
            //cerr << "compare " << date << " " << constantDate_ << endl;
            return op_(date, constantDate_);
        }; 
    }
};
                      
typedef std::function<bool(const bool &, const bool &)> BooleanOp;

struct BooleanFilter {
    BooleanOp op;
    FilteredDataset::TupleFilter left, right;
    
    BooleanFilter(const BooleanOp & op, const FilteredDataset::TupleFilter & left, const FilteredDataset::TupleFilter & right) :
    op(op), left(left), right(right) {}
    
    FilteredDataset::TupleFilter  eval() {
        // prevent the this pointer from being captured allowing syntax like BooleanFilter(...).eval()
        FilteredDataset::TupleFilter left_ = left;
        FilteredDataset::TupleFilter right_ = right;
        BooleanOp op_ = op;
        return [op_, left_, right_](const CellValue & value, const Date & date) 
        {
            //cerr << "boolean " << value << endl;
            return op_(left_(value, date), right_(value, date));
        }; 
    }
};
}

/*****************************************************************************/
/* FILTERED DATASET INTERNAL REPRESENTATION                                  */
/*****************************************************************************/


struct FilteredDataset::Itl
    : public MatrixView, public ColumnIndex {

    std::shared_ptr<MatrixView> matrixView;
    std::shared_ptr<ColumnIndex> columnIndex;
    const TupleFilter filter;

    Itl(std::shared_ptr<MatrixView> matrixView, std::shared_ptr<ColumnIndex> columnIndex,
        const TupleFilter& filter) : 
        matrixView(matrixView), columnIndex(columnIndex), filter(filter)
    {
    }

    ~Itl() { }

    /** MatrixView */

    virtual RowPath getRowPath(const RowHash & rowHash) const
    {
        return matrixView->getRowPath(rowHash);
    }

    virtual ColumnPath getColumnPath(ColumnHash column) const
    {
        return matrixView->getColumnPath(column);
    }

    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {    
        return matrixView->getRowPaths(start, limit);
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        return matrixView->getRowHashes(start, limit);
    }

    virtual size_t getRowCount() const
    {
        return matrixView->getRowCount();
    }

    virtual bool knownRow(const RowPath & rowName) const
    {
        return matrixView->knownRow(rowName);
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const
    {
        MatrixNamedRow matrixRow = matrixView->getRow(rowName);

        std::vector<std::tuple<ColumnPath, CellValue, Date> > columns;
        auto filterColumn = [=] (const std::tuple<ColumnPath, CellValue, Date> & tuple) {
            return filter(get<1>(tuple), get<2>(tuple));
        };
        std::copy_if(matrixRow.columns.begin(), matrixRow.columns.end(),
                     std::back_inserter(columns), filterColumn);

        return {matrixRow.rowHash, matrixRow.rowName, columns };
    }

    virtual bool knownColumn(const ColumnPath & column) const
    {
        return matrixView->knownColumn(column);
    }

    virtual std::vector<ColumnPath> getColumnPaths() const
    {
        return matrixView->getColumnPaths();
    }   

    virtual size_t getColumnCount() const
    {
        return matrixView->getColumnCount();
    }   

    virtual const ColumnStats &
    getColumnStats(const ColumnPath & ch, ColumnStats & toStoreResult) const
    {
        return columnIndex->getColumnStats(ch, toStoreResult);
    }

    virtual MatrixColumn getColumn(const ColumnPath & columnHash) const
    {
        MatrixColumn matrixColumn = columnIndex->getColumn(columnHash);
        
        std::vector<std::tuple<RowPath, CellValue, Date> > rows;
        auto filterRow = [=] (const std::tuple<RowHash, CellValue, Date> & tuple) {
            return filter(get<1>(tuple), get<2>(tuple));
        };
        std::copy_if(matrixColumn.rows.begin(), matrixColumn.rows.end(),
                     std::back_inserter(rows), filterRow);

        return {matrixColumn.columnHash, matrixColumn.columnName, rows};
    }

    virtual std::vector<std::tuple<RowPath, CellValue> >
    getColumnValues(const ColumnPath & columnName,
                    const std::function<bool (const CellValue &)> & filter) const
    {
        // TODO apply the predicate here
        return columnIndex->getColumnValues(columnName, filter);
    }

    // Duplicated methods in interfaces - Implemented as part of MatrixView
    //virtual bool knownColumn(ColumnHash column) const
    //virtual std::vector<ColumnPath> getColumnPaths() const
};


/*****************************************************************************/
/* FILTERED DATASET                                                            */
/*****************************************************************************/

FilteredDataset::
FilteredDataset(const Dataset& dataset, const FilteredDataset::TupleFilter& filter)
    : Dataset(dataset.server), dataset(dataset)
{
    itl.reset(new Itl(dataset.getMatrixView(), dataset.getColumnIndex(), filter));
}

FilteredDataset::
~FilteredDataset()
{
}

Any
FilteredDataset::
getStatus() const
{
    return Any();
}

KnownColumn
FilteredDataset::
getKnownColumnInfo(const ColumnPath & columnName) const
{
    return dataset.getKnownColumnInfo(columnName);
}

BoundFunction
FilteredDataset::
overrideFunction(const Utf8String & tableName,
                 const Utf8String & functionName,
                 SqlBindingScope & context) const
{
    return dataset.overrideFunction(tableName, functionName, context);
}

std::shared_ptr<MatrixView>
FilteredDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
FilteredDataset::
getColumnIndex() const
{
    return itl;
}

} // namespace MLDB


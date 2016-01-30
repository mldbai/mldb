// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** dataset_utils.cc                                 -*- C++ -*-
    Rémi Attab, 29 Apr 2015
    Copyright (c) 2015 Datacratic.  All rights reserved.
*/

#include "mldb/server/dataset_utils.h"
#include <algorithm>

using namespace std;
using namespace ML;

namespace Datacratic {
namespace MLDB {


/******************************************************************************/
/* MERGED MATRIX VIEW                                                         */
/******************************************************************************/

MergedMatrixView::
MergedMatrixView(std::vector< std::shared_ptr<MatrixView> > views) :
    views(std::move(views))
{}

std::vector<RowName>
MergedMatrixView::
getRowNames(ssize_t start, ssize_t limit) const
{
    std::vector<RowName> names;

    for (const auto& view : views) {
        auto rows = view->getRowNames();
        names.insert(names.end(), rows.begin(), rows.end());
    }

    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());

    if (start > names.size()) return {};
    if (limit < 0) limit = names.size();

    auto it = names.begin() + start;
    auto end = start + limit < names.size() ? it + limit : names.end();
    return std::vector<RowName>(it, end);
}

std::vector<RowHash>
MergedMatrixView::
getRowHashes(ssize_t start, ssize_t limit) const
{
    std::vector<RowHash> hashes;

    for (const auto& view : views) {
        auto rows = view->getRowHashes();
        hashes.insert(hashes.end(), rows.begin(), rows.end());
    }

    std::sort(hashes.begin(), hashes.end());
    hashes.erase(std::unique(hashes.begin(), hashes.end()), hashes.end());

    if (start > hashes.size()) return {};
    if (limit < 0) limit = hashes.size();

    auto it = hashes.begin() + start;
    auto end = start + limit <= hashes.size() ? it + limit : hashes.end();
    return std::vector<RowHash>(it, end);
}

size_t
MergedMatrixView::
getRowCount() const
{
    return getRowNames().size();
}

bool
MergedMatrixView::
knownRow(const RowName & rowName) const
{
    for (const auto& view : views) {
        if (view->knownRow(rowName)) return true;
    }

    return false;
}

MatrixNamedRow
MergedMatrixView::
getRow(const RowName & row) const
{
    MatrixNamedRow result;
    result.rowHash = result.rowName = row;

    for (const auto& view : views) {
        if (!view->knownRow(row)) continue;

        auto rows = view->getRow(row);

        result.columns.insert(
                result.columns.end(), rows.columns.begin(), rows.columns.end());
    }

    if (result.columns.empty())
        throw ML::Exception("unknown row '%s'", row.toUtf8String().rawData());

    return result;
}

std::vector<ColumnName>
MergedMatrixView::
getColumnNames() const
{
    std::vector<ColumnName> result;

    for (const auto& view : views) {
        auto names = view->getColumnNames();
        result.insert(result.end(), names.begin(), names.end());
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());

    return result;
}

size_t
MergedMatrixView::
getColumnCount() const
{
    return getColumnNames().size();
}

bool
MergedMatrixView::
knownColumn(const ColumnName & column) const
{
    for (const auto& view : views) {
        if (view->knownColumn(column)) return true;
    }

    return false;
}


/******************************************************************************/
/* MERGED COLUMN INDEXES                                                      */
/******************************************************************************/

MergedColumnIndex::
MergedColumnIndex(std::vector< std::shared_ptr<ColumnIndex> > indexes) :
    indexes(std::move(indexes))
{}

MatrixColumn
MergedColumnIndex::
getColumn(const ColumnName & column) const
{
    MatrixColumn result;
    result.columnHash = column;

    for (const auto& index : indexes) {
        if (!index->knownColumn(column)) continue;

        auto ret = index->getColumn(column);
        if (result.columnName.empty())
            result.columnName = std::move(ret.columnName);

        result.rows.insert(result.rows.end(), ret.rows.begin(), ret.rows.end());
    }

    if (result.rows.empty())
        throw ML::Exception("unknown column '%s'", column.toUtf8String().rawData());

    return result;
}

bool
MergedColumnIndex::
knownColumn(const ColumnName & column) const
{
    for (const auto& index : indexes) {
        if (index->knownColumn(column)) return true;
    }

    return false;
}

std::vector<ColumnName>
MergedColumnIndex::
getColumnNames() const
{
    std::vector<ColumnName> result;

    for (const auto& index : indexes) {
        auto names = index->getColumnNames();
        result.insert(result.end(), names.begin(), names.end());
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());

    return result;
}

} // namespace MLDB
} // namepsace Datacratic

// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** dataset_utils.cc                                 -*- C++ -*-
    Rémi Attab, 29 Apr 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include "mldb/server/dataset_utils.h"
#include "mldb/http/http_exception.h"
#include <algorithm>

using namespace std;


namespace MLDB {


/******************************************************************************/
/* MERGED MATRIX VIEW                                                         */
/******************************************************************************/

MergedMatrixView::
MergedMatrixView(std::vector< std::shared_ptr<MatrixView> > views) :
    views(std::move(views))
{}

std::vector<RowPath>
MergedMatrixView::
getRowPaths(ssize_t start, ssize_t limit) const
{
    std::vector<RowPath> names;

    for (const auto& view : views) {
        auto rows = view->getRowPaths();
        names.insert(names.end(), rows.begin(), rows.end());
    }

    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());

    if (start > names.size()) return {};
    if (limit < 0) limit = names.size();

    auto it = names.begin() + start;
    auto end = start + limit < names.size() ? it + limit : names.end();
    return std::vector<RowPath>(it, end);
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
    return getRowPaths().size();
}

bool
MergedMatrixView::
knownRow(const RowPath & rowName) const
{
    for (const auto& view : views) {
        if (view->knownRow(rowName)) return true;
    }

    return false;
}

MatrixNamedRow
MergedMatrixView::
getRow(const RowPath & row) const
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
        throw HttpReturnException(400, "unknown row '" + row.toUtf8String() + "'");

    return result;
}

std::vector<ColumnPath>
MergedMatrixView::
getColumnPaths() const
{
    std::vector<ColumnPath> result;

    for (const auto& view : views) {
        auto names = view->getColumnPaths();
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
    return getColumnPaths().size();
}

bool
MergedMatrixView::
knownColumn(const ColumnPath & column) const
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
getColumn(const ColumnPath & column) const
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
        throw HttpReturnException(400, "unknown column '" + column.toUtf8String() + "'");

    return result;
}

bool
MergedColumnIndex::
knownColumn(const ColumnPath & column) const
{
    for (const auto& index : indexes) {
        if (index->knownColumn(column)) return true;
    }

    return false;
}

std::vector<ColumnPath>
MergedColumnIndex::
getColumnPaths() const
{
    std::vector<ColumnPath> result;

    for (const auto& index : indexes) {
        auto names = index->getColumnPaths();
        result.insert(result.end(), names.begin(), names.end());
    }

    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());

    return result;
}

} // namespace MLDB


/** dataset_collection.h                                           -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Interface for datasets into MLDB.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/rest/poly_collection.h"



struct RestConnection;

namespace MLDB {


/** Run a query (by calling the given function) and format and return the
    results in HTTP based upon the given flag.

    - format: output format of results
    - createHeaders: table result formats will include a header row
    - rowNames: add a '_rowName' column
    - rowHashes: add a '_rowHash' column
*/
void runHttpQuery(std::function<std::vector<MatrixNamedRow> ()> runQuery,
                  RestConnection & connection,
                  const std::string & format,
                  bool createHeaders,
                  bool rowNames,
                  bool rowHashes,
                  bool sortColumns);
                      

/*****************************************************************************/
/* DATASET COLLECTION                                                        */
/*****************************************************************************/

struct DatasetCollection: public PolyCollection<Dataset> {
    DatasetCollection(MldbServer * server);

    static void initRoutes(RouteManager & manager);

    virtual Any getEntityStatus(const Dataset & dataset) const;

    std::vector<std::pair<CellValue, int64_t> >
    getColumnValueCounts(const Dataset * dataset,
                         const ColumnPath & columnName,
                         ssize_t offset,
                         ssize_t limit) const;

    /** Select from the database in a given format */
    virtual void
    queryStructured(const Dataset * dataset,
                    RestConnection & connection,
                    const std::string & format,
                    const Utf8String & select,
                    const Utf8String & when,
                    const Utf8String & where,
                    const Utf8String & orderBy,
                    const Utf8String & groupBy,
                    const Utf8String & having,
                    const Utf8String & rowName,
                    ssize_t offset,
                    ssize_t limit,
                    bool createHeaders,
                    bool rowNames,
                    bool rowHashes,
                    bool sortColumns) const;
};

extern template class PolyCollection<Dataset>;

} // namespace MLDB




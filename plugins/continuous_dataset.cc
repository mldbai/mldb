// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** continuous.cc
    Jeremy Barnes, 9 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Implementation of continuous dataset.
*/

#include "continuous_dataset.h"
#include "mldb/arch/rcu_protected.h"
#include "mldb/rest/rest_request_binding.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/jml/utils/worker_task.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/types/any_impl.h"
#include "mldb/arch/rcu_protected.h"
#include "mldb/watch/watch.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/server/mldb_server.h"
#include "mldb/builtin/merged_dataset.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* CONTINUOUS DATASET CONFIG                                                 */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ContinuousDatasetConfig);

ContinuousDatasetConfigDescription::
ContinuousDatasetConfigDescription()
{
    addField("metadataDataset", &ContinuousDatasetConfig::metadataDataset,
             "Dataset used to store metadata in");
    addField("createStorageDataset", &ContinuousDatasetConfig::createStorageDataset,
             "Procedure that will create a dataset for storage");
    addField("saveStorageDataset", &ContinuousDatasetConfig::saveStorageDataset,
             "Procedure that will save a storage dataset returning metadata");
    addField("commitInterval", &ContinuousDatasetConfig::commitInterval,
             "Interval between auto-commit operations");
}


/*****************************************************************************/
/* CONTINUOUS INTERNAL REPRESENTATION                                        */
/*****************************************************************************/

struct ContinuousDataset::Itl {
    Itl(MldbServer * server, const ContinuousDatasetConfig & config)
        : server(server),
          current(gcLock),
          lastCommit(Date::now().secondsSinceEpoch())
    {
        initRoutes();

        try {
            // Get the metadata dataset.  This is what stores the internal
            // metadata about which datasets are available.
            metadataDataset = obtainDataset(server, config.metadataDataset);
        } JML_CATCH_ALL {
            rethrowHttpException(-1, "Error initializing continuous dataset in "
                                 "metadata initialization: " + ML::getExceptionString(),
                                 "continuousDatasetConfig", config);
        }
        
        try {
            createStorageDataset = obtainProcedure(server, config.createStorageDataset);
        } JML_CATCH_ALL {
            rethrowHttpException(-1, "Error initializing continuous dataset in "
                                 "createStorageDataset initialization: "
                                 + ML::getExceptionString(),
                                 "continuousDatasetConfig", config);
        }
        
        try {
            saveStorageDataset = obtainProcedure(server, config.saveStorageDataset);
        } JML_CATCH_ALL {
            rethrowHttpException(-1, "Error initializing continuous dataset in "
                                 "saveStorageDataset procedure initialization: "
                                 + ML::getExceptionString(),
                                 "continuousDatasetConfig", config);
        }
        
        // Perform a first rotation, so that everything is properly set
        // up for the rotation.
        rotate(Date::positiveInfinity());

        ExcAssert(current.val);

        // Set up an interval for the commit operation
        if (config.commitInterval.number > 0) {
            timer = server->getTimer(Date::now().plusSeconds(config.commitInterval.number),
                                     config.commitInterval.number,
                                     [=] (Date date)
                                     {
                                         rotate(date);
                                     });
        }
    }

    ~Itl()
    {
    }

    MldbServer * server;

    WatchT<Date> timer;

    RestRequestRouter router;

    std::shared_ptr<Dataset> metadataDataset;
    std::shared_ptr<Procedure> createStorageDataset;
    std::shared_ptr<Procedure> saveStorageDataset;

    void initRoutes()
    {
#if 0
        addRouteSyncJsonReturn(router, "/views", {"POST"},
                               "Create a view of the current dataset that can be "
                               "queried",
                               "Status of dataset view",
                               &Itl::createView,
                               this,
                               JsonParam<Utf8String>("where", "Filter to choose which metadata databases to load"),
                               JsonParamDefault<bool>("live", "If true, the returned dataset is live and will be updated in real-time as events are recorded", false));
#endif
    }

    Any
    getStatus() const
    {
        auto myCurrent = current();
        return myCurrent->dataset->getStatus();
    }

    virtual void
    recordRowItl(const RowName & rowName,
                 const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
        auto myCurrent = current();
        myCurrent->dataset->recordRow(rowName, vals);
        myCurrent->hasData = true;
    }
    
    virtual void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
    {
        auto myCurrent = current();
        myCurrent->dataset->recordRows(rows);
        myCurrent->hasData = true;
    }

    struct Current {
        std::shared_ptr<Dataset> dataset;  ///< Dataset itself
        std::atomic<bool> hasData;         ///< Is there any data in it?
    };

    GcLock gcLock;
    RcuProtected<Current> current;

    /// Mutex for phase 1 of the rotate, which is to swap out the datasets.
    std::mutex rotateMutex;

    /// Mutex for phase 2 of the rotate, which is to save the dataset.  The
    /// mutex for phase 1 is released once this mutex is acquired.
    std::mutex saveMutex;

    /// Used live datasets to know about datasets that are created and
    /// rotated.
    WatchesT<std::shared_ptr<Dataset> > datasetWatches;

    /// Date of the last commit
    std::atomic<double> lastCommit;
    
    static std::vector<std::tuple<ColumnName, CellValue, Date> >
    extractMetadata(const Json::Value & val, ColumnName prefix = ColumnName())
    {
        std::vector<std::tuple<ColumnName, CellValue, Date> > result;
        
        switch (val.type()) {
        case Json::objectValue:
            for (auto it = val.begin(), end = val.end();  it != end;  ++it) {
                ColumnName newPrefix
                    = prefix == ColumnName()
                    ? ColumnName(it.memberName())
                    : ColumnName(prefix.toUtf8String() + "." + it.memberName());
                auto child = extractMetadata(*it, newPrefix);
                result.insert(result.end(),
                              std::make_move_iterator(child.begin()),
                              std::make_move_iterator(child.end()));
            }
            break;
        case Json::arrayValue:
            for (unsigned i = 0;  i < val.size();  ++i) {
                ColumnName newPrefix
                    (prefix.toUtf8String() + "[" + std::to_string(i) + "]");
                auto child = extractMetadata(val[i], newPrefix);
                result.insert(result.end(),
                              std::make_move_iterator(child.begin()),
                              std::make_move_iterator(child.end()));
            }
            break;
        default:
            result.emplace_back(prefix, jsonDecode<CellValue>(val), Date::now());
        }
        
        return result;
    }

    /** Rotate the dataset, atomically, and add it to the metadata store. */
    void rotate(Date commitStarted)
    {
        if (lastCommit.load() > commitStarted.secondsSinceEpoch())
            return;

        std::unique_lock<std::mutex> rotateGuard(rotateMutex);

        // If we already commited after the time rotate() was called, then
        // nothing to do.
        if (lastCommit.load() > commitStarted.secondsSinceEpoch())
            return;

        // First, create a new storage dataset to hold anything that comes
        // along while we're rotating the old

        ProcedureRunConfig runConfig;

        auto storageOutput
            = createStorageDataset->run(runConfig, nullptr /* progress */);

        cerr << "output of storage is " << jsonEncode(storageOutput) << endl;

        std::unique_ptr<Current> newCurrent(new Current());
        newCurrent->dataset
            = obtainDataset(server,
                            storageOutput.results.getField("config")
                            .convert<PolyConfig>(), nullptr);
        
        // Now, swap it in...
        auto old = current.replaceCustomCleanup(newCurrent.release());

        // In initialization, we don't have an old dataset so we get out
        // here.
        if (!old || !old->dataset)
            return;

        std::unique_lock<std::mutex> saveGuard(saveMutex);

        // If we already commited after the time rotate() was called, then
        // nothing to do.
        if (lastCommit.load() > commitStarted.secondsSinceEpoch())
            return;
        
        // Release the rotate mutex
        rotateGuard.unlock();
        
        std::shared_ptr<Dataset> savedDataset = old->dataset;

        // ... and wait for all users to stop using it.  Once we're past this
        // line, there is no possibility that old will be modified by
        // any thread, and so we can save it, etc.  There still may be external
        // users who have the shared pointer to the dataset to read from, though,
        // so we do have to make sure it stays readable.
        gcLock.visibleBarrier();

        // If there is no data in the dataset, then don't save anything
        if (!old->hasData) {
            lastCommit = commitStarted.secondsSinceEpoch();
            return;
        }
        
        // Now we can run our procedure to save the dataset, and get back
        // its metadata
        ProcedureRunConfig saveRunConfig;
        Json::Value saveParams;
        saveParams["args"]["datasetId"] = savedDataset->config_->id;
        saveRunConfig.params = saveParams;

        auto saveOutput
            = saveStorageDataset->run(saveRunConfig, nullptr /* progress */);
        
        // Take the metadata and put it in the metadata database
        
        Json::Value resultsJson = jsonEncode(saveOutput.results);
        const Json::Value & metadataJson = resultsJson["metadata"];
        const Json::Value & configJson = resultsJson["config"];

        cerr << "metadata is " << jsonEncode(saveOutput) << endl;

        RowName rowName(savedDataset->config_->id);

        std::vector<std::tuple<ColumnName, CellValue, Date> > metadata
            = extractMetadata(metadataJson, ColumnName("md"));

        std::vector<std::tuple<ColumnName, CellValue, Date> > config
            = extractMetadata(configJson, ColumnName("config"));

        metadata.insert(metadata.end(),
                        std::make_move_iterator(config.begin()),
                        std::make_move_iterator(config.end()));

        Date earliest, latest;
        std::tie(earliest, latest)
            = savedDataset->getTimestampRange();

        // TODO: the procedure should return this...
        metadata.emplace_back(ColumnName("earliest"), earliest, Date::now());
        metadata.emplace_back(ColumnName("latest"), latest, Date::now());
        
        metadataDataset->recordRow(rowName, metadata);

        datasetWatches.trigger(savedDataset);

        // We now know that everything is committed up to lastCommit.
        lastCommit = commitStarted.secondsSinceEpoch();
    }

    virtual void commit()
    {
        // Force a write-out of the dataset.  We only exit once it's
        // done.  This allows a call to commit() to be used to guarantee
        // that what was written up to now is actually in the database.
        rotate(Date::now());
    }

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const
    {
        return router.processRequest(connection, request, context);
    }

    std::shared_ptr<MatrixView>
    getMatrixView() const
    {
        auto myCurrent = current();
        return myCurrent->dataset->getMatrixView();
    }
    
    std::shared_ptr<ColumnIndex>
    getColumnIndex() const
    {
        auto myCurrent = current();
        return myCurrent->dataset->getColumnIndex();
    }

    std::pair<Date, Date>
    getTimestampRange() const
    {
        auto myCurrent = current();
        return myCurrent->dataset->getTimestampRange();
    }
    
    Date
    quantizeTimestamp(Date timestamp) const
    {
        auto myCurrent = current();
        return myCurrent->dataset->quantizeTimestamp(timestamp);
    }
};


/*****************************************************************************/
/* CONTINUOUS DATASET                                                        */
/*****************************************************************************/

ContinuousDataset::
ContinuousDataset(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    datasetConfig = config.params.convert<ContinuousDatasetConfig>();
    itl.reset(new Itl(owner, datasetConfig));
}
    
ContinuousDataset::
~ContinuousDataset()
{
}

Any
ContinuousDataset::
getStatus() const
{
    return itl->getStatus();
}

void
ContinuousDataset::
recordRowItl(const RowName & rowName,
          const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
{
    return itl->recordRowItl(rowName, vals);
}

void
ContinuousDataset::
recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows)
{
    return itl->recordRows(rows);
}

void
ContinuousDataset::
commit()
{
    return itl->commit();
}
    
std::pair<Date, Date>
ContinuousDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

Date
ContinuousDataset::
quantizeTimestamp(Date timestamp) const
{
    return itl->quantizeTimestamp(timestamp);
}

std::shared_ptr<MatrixView>
ContinuousDataset::
getMatrixView() const
{
    return itl->getMatrixView();
}

std::shared_ptr<ColumnIndex>
ContinuousDataset::
getColumnIndex() const
{
    return itl->getColumnIndex();
}

RestRequestMatchResult
ContinuousDataset::
handleRequest(RestConnection & connection,
              const RestRequest & request,
              RestRequestParsingContext & context) const
{
    return itl->handleRequest(connection, request, context);
}

static RegisterDatasetType<ContinuousDataset, ContinuousDatasetConfig>
regContinuous(builtinPackage(),
              "continuous",
              "Dataset that can be continuously recorded to",
              "datasets/ContinuousDataset.md.html");


/*****************************************************************************/
/* CONTINUOUS WINDOW DATASET CONFIG                                          */
/*****************************************************************************/

ContinuousWindowDatasetConfig::
ContinuousWindowDatasetConfig()
    : datasetFilter(SqlExpression::TRUE)
{
}

DEFINE_STRUCTURE_DESCRIPTION(ContinuousWindowDatasetConfig);


ContinuousWindowDatasetConfigDescription::
ContinuousWindowDatasetConfigDescription()
{
    addField("metadataDataset", &ContinuousWindowDatasetConfig::metadataDataset,
             "Dataset used to store metadata in");
    addField("from", &ContinuousWindowDatasetConfig::from,
             "Earliest date to include within the dataset");
    addField("to", &ContinuousWindowDatasetConfig::to,
             "Latest date to include within the dataset");
    addField("datasetFilter", &ContinuousWindowDatasetConfig::datasetFilter,
             "Filter to apply to dataset metadata when choosing datasets",
             SqlExpression::TRUE);
}


/*****************************************************************************/
/* CONTINUOUS WINDOW DATASET                                                 */
/*****************************************************************************/

/** Given a row from a metadata dataset query, return a configuration
    object for the dataset.
*/
static PolyConfigT<const Dataset>
reconstituteConfig(const MatrixNamedRow & row)
{
    Json::Value current;

    for (auto & col: row.columns) {
        Json::Value * p = &current;
        vector<string> parts = ML::split(std::get<0>(col).toString(), '.');
        if (parts[0] != "config")
            continue;
        for (unsigned i = 1;  i < parts.size();  ++i) {
            Json::Value & v2 = (*p)[parts[i]];
            p = &v2;
        }

        *p = jsonEncode(std::get<1>(col));
    }

    current["id"] = jsonEncode(row.rowName);

    return jsonDecode<PolyConfigT<const Dataset> >(current);
}

PolyConfigT<const Dataset>
ContinuousWindowDataset::
getDatasetConfig(std::shared_ptr<SqlExpression> datasetsWhere,
                 Date from,
                 Date to)
{
    // Construct a query that gets us our datasets from from and to
    // This is earliest <= to and latest >= from

    Utf8String where = "(" + datasetsWhere->surface + ") "
        + "AND earliest <= CAST ('" + CellValue(to).toString() + "' AS TIMESTAMP) "
        + "AND latest >= CAST ('" + CellValue(from).toString() + "' AS TIMESTAMP)";
    
    //cerr << "where is " << where << endl;

    // Query our metadata dataset for the datasets to load up
    auto datasets
        = metadataDataset
        ->queryStructured(SelectExpression::STAR,
                          WhenExpression::TRUE /* when */,
                          SqlExpression::parse(where) /* where */,
                          OrderByExpression::parse("rowName() ASC"),
                          TupleExpression(),
                          SqlExpression::TRUE /* having */,
                          SqlExpression::parse("rowName()") /* rowName */,
                          0 /* offset */,
                          -1 /* limit */,
                          "" /* alias */);

    // TODO:
    // 1.  Use from and to
    // 2.  If datasets overhang, then add a filter in
    
    MergedDatasetConfig params;

    for (auto & ds: datasets) {
        // Reconstitute a configuration
        params.datasets.emplace_back(reconstituteConfig(ds));
    }
    
    PolyConfigT<const Dataset> result;
    result.type = "merged";
    result.params = params;
    return result;
}

ContinuousWindowDataset::
ContinuousWindowDataset(MldbServer * owner,
                        PolyConfig config_,
                        const std::function<bool (const Json::Value &)> & onProgress)
    : ForwardedDataset(owner)
{
    auto config = config_.params.convert<ContinuousWindowDatasetConfig>();

    try {
        // Get the metadata dataset.  This is what stores the internal
        // metadata about which datasets are available.
        metadataDataset = obtainDataset(server, config.metadataDataset);
    } JML_CATCH_ALL {
        rethrowHttpException(-1, "Error initializing continuous window dataset in "
                             "metadata initialization: " + ML::getExceptionString(),
                             "continuousDatasetConfig", config);
    }

    PolyConfigT<const Dataset> toLoadConfig;

    try {
        // Query the metadata dataset for the datasets that we need to load
        // up, and turn it into a merged dataset configuration.
        toLoadConfig = getDatasetConfig(config.datasetFilter, config.from, config.to);
    } JML_CATCH_ALL {
        rethrowHttpException(-1, "Error initializing continuous window dataset in "
                             "metadata query: " + ML::getExceptionString(),
                             "continuousDatasetConfig", config);
    }
    
    try {
        // Obtain the merged dataset, recursively
        std::shared_ptr<Dataset> underlying
            = obtainDataset(server, toLoadConfig, onProgress);
        setUnderlying(underlying);
    } JML_CATCH_ALL {
        rethrowHttpException(-1, "Error initializing continuous window dataset in "
                             "metadata query: " + ML::getExceptionString(),
                             "continuousDatasetConfig", config);
    }
}

static RegisterDatasetType<ContinuousWindowDataset,
                           ContinuousWindowDatasetConfig>
regContinuousWindow(builtinPackage(),
                    "continuous.window",
                    "View of a static time window view over a continuous dataset",
                    "datasets/ContinuousDataset.md.html");

} // namespace MLDB
} // namespace Datacratic

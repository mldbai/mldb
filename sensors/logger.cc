/** logger.cc
    Jeremy Barnes, 14 November 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

    Data logger.
*/

#include "sensors_plugin.h"
#include "mldb/core/sensor.h"
#include "mldb/core/recorder.h"
#include "mldb/watch/watch.h"
#include "mldb/watch/watch_impl.h"
#include "mldb/types/periodic_utils.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/types/structure_description.h"
#include "mldb/sql/binding_contexts.h"
#include "mldb/server/dataset_context.h"
#include "mldb/server/mldb_server.h"


using namespace std;


namespace MLDB {

struct LoggerConfig {
    LoggerConfig()
    {
        output.withType("dataset");
    }

    /// Expression that we select on
    SelectExpression select;

    /// Frequency with which we record
    TimePeriod interval = "1s";

    /// The output dataset.  Rows will be dumped into here via insertRows.
    PolyConfigT<Recorder> output;
};

DECLARE_STRUCTURE_DESCRIPTION(LoggerConfig);
DEFINE_STRUCTURE_DESCRIPTION(LoggerConfig);

LoggerConfigDescription::
LoggerConfigDescription()
{
    addField("select", &LoggerConfig::select,
             "SQL expression for metrics to log");
    addField("interval", &LoggerConfig::interval,
             "Time interval to log under");
    addField("output", &LoggerConfig::output,
             "Recorder to output data to");
}

struct Logger: public Sensor {

    const std::vector<Utf8String> argNames = { "timestamp", "previous" };

    const std::vector<std::shared_ptr<ExpressionValueInfo> > argInfo = {
        std::make_shared<TimestampValueInfo>(),
        std::make_shared<UnknownRowValueInfo>()
    };

    Logger(MldbServer * server,
           const PolyConfig & pconfig,
           std::function<bool (Json::Value)> onProgress)
        : Sensor(server),
          mldbScope(server),
          evalScope(mldbScope, argInfo, argNames)
    {
        config = pconfig.params.convert<LoggerConfig>();

        bound = config.select.bind(evalScope);

        recorder = createRecorder(server, config.output, onProgress);

        // Set up an interval for the sampling operation
        // This must happen at the end so that we don't try to do
        // so before we've started.
        if (config.interval.interval > 0) {
            timer = server->getTimer
                (Date::now().plusSeconds(config.interval.interval),
                 config.interval.interval,
                 [=] (Date date)
                 {
                     this->logOne(date);
                 });
        }
    }
    
    ~Logger()
    {
        // Stop the events flowing
        timer = WatchT<Date>();
    }

    ExpressionValue
    get(const ExpressionValue & previous,
        Date now) const 
    {
        SqlRowScope mldbScope;
        std::vector<ExpressionValue> args = {
            ExpressionValue(now, now),
            previous
        };

        auto rowScope = evalScope.getRowScope(mldbScope, args);

        ExpressionValue storage;
        return bound(rowScope, storage, GET_LATEST);
    }

    void logOne(Date now)
    {
        std::unique_lock<std::mutex> guard(mutex);
        auto current = get(previous, now);
        Path rowName = PathElement(sampleNumber.fetch_add(1));
        recorder->recordRowExprDestructive(rowName, current);
        previous = current;
    }

    virtual ExpressionValue latest()
    {
        std::unique_lock<std::mutex> guard(mutex);
        return get(previous, Date::now());
    }
    
    virtual std::shared_ptr<ExpressionValueInfo> resultInfo() const
    {
        return bound.info;
    }

    SqlExpressionMldbScope mldbScope;
    SqlExpressionEvalScope evalScope;
    LoggerConfig config;
    BoundSqlExpression bound;

    std::mutex mutex;
    ExpressionValue previous;  // protected by mutex
    std::shared_ptr<Recorder> recorder;
    std::atomic<uint64_t> sampleNumber = ATOMIC_VAR_INIT(0);

    WatchT<Date> timer;
};

static RegisterSensorType<Logger, LoggerConfig>
regLogger(sensorsPackage(),
          "logger",
          "Sensor that will log data periodically",
          "Logger.md.html");


} // namespace MLDB

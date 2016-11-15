/** memory_sensor.cc
    Jeremy Barnes, 11 November 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

    Simple sensor that evaluates an memory.
*/

#include "sensors_plugin.h"
#include "mldb/core/sensor.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/base/scope.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/date.h"
#include "mldb/sql/expression_value.h"
#include "mldb/arch/vm.h"
#include <fcntl.h>
#include <unistd.h>
#include <mutex>


using namespace std;


namespace MLDB {

struct MemorySensorConfig {
};

DECLARE_STRUCTURE_DESCRIPTION(MemorySensorConfig);
DEFINE_STRUCTURE_DESCRIPTION(MemorySensorConfig);

MemorySensorConfigDescription::
MemorySensorConfigDescription()
{
}

struct MemorySensor: public Sensor {
    MemorySensor(MldbServer * server,
                     const PolyConfig & pconfig,
                     std::function<bool (Json::Value)> onProgress)
        : Sensor(server)
    {
        config = pconfig.params.convert<MemorySensorConfig>();
        fd = ::open("/proc/self/statm", 0);
        if (fd == -1) {
            throw HttpReturnException(400, "Error opening proc file /proc/self/statm: "
                                      + string(strerror(errno)));
        }
    }

    ~MemorySensor()
    {
        ::close(fd);
    }

    virtual ExpressionValue latest()
    {
        std::unique_lock<std::mutex> guard(mutex);
        {
            int res = lseek(fd, 0, SEEK_SET);
            if (res == -1) {
                throw HttpReturnException(400, "Error seeking proc file: "
                                          + string(strerror(errno)));
            }
        }

        constexpr size_t NCHARS = 256;
        char buf[NCHARS];
        ssize_t res = read(fd, buf, NCHARS);
        
        if (res == -1) {
            throw HttpReturnException(600, "Unable to read from memory sensor");
        }
        buf[res] = 0;

        guard.unlock();

        Date now = Date::now();

        constexpr size_t NVALS = 7;
        static const PathElement names[NVALS] = {
            "mem", "rss", "shared", "text", "data", "library", "dirty"
        };

        StructValue result;

        char * p = buf;
        for (int i = 0;  i < NVALS;  ++i) {
            char * e;
            result.emplace_back(names[i], ExpressionValue(strtoll(p, &e, 10) * page_size, now));
            ExcAssertGreater(e, p);
            if (i == NVALS - 1)
                break;
            p = e + 1;  // skip space
            ExcAssertLessEqual(p, buf + res);
        }

        return std::move(result);
    }
    
    virtual std::shared_ptr<ExpressionValueInfo>
    resultInfo() const
    {
        return std::make_shared<UnknownRowValueInfo>();
    }

    int fd = -1;
    std::mutex mutex;

    MemorySensorConfig config;
};

namespace {

static RegisterSensorType<MemorySensor, MemorySensorConfig>
regMemorySensor(sensorsPackage(),
                "mldb.memory",
                "Sensor for the memory usage of MLDB",
                "MemorySensor.md.html");

} // file scope

} // namespace MLDB


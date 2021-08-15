/** per_thread_accumulator.h                                       -*- C++ -*-
    Jeremy Barnes, 30 July 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Object that allows map-reduce over multiple threads and accumulation
    at the end.
*/

#pragma once

#include "mldb/arch/thread_specific.h"
#include <functional>
#include <vector>
#include <thread>

namespace MLDB {

template<typename Payload>
struct PerThreadAccumulator {

    std::mutex threadsLock;
    std::vector<std::shared_ptr<Payload> > threads;
    ThreadSpecificInstanceInfo<std::shared_ptr<Payload>> payloadPerThread;

    std::function<Payload * ()> createPayload;
    std::function<void (Payload *)> destroyPayload;
    
    PerThreadAccumulator()
        : createPayload([] () { return new Payload(); }),
          destroyPayload([] (Payload * p) { delete p; })
    {
    }
    
    PerThreadAccumulator(std::function<Payload * ()> createPayload,
                         std::function<void (Payload *)> destroyPayload
                         = [] (Payload * p) { delete p; })
        : createPayload(std::move(createPayload)),
          destroyPayload(std::move(destroyPayload))
    {
    }
    
    Payload & get()
    {
        bool hadInfo = false;
        std::shared_ptr<Payload> * payload = payloadPerThread.get(&hadInfo);

        if (MLDB_UNLIKELY(!hadInfo)) {
            ExcAssert(!payload->get());
            payload->reset(createPayload(), destroyPayload);

            std::unique_lock<std::mutex> guard(threadsLock);
            threads.emplace_back(*payload);
        }

#if 0        
        // verify that thread specific accesses work...
        std::shared_ptr<Payload> * payload2 = payloadPerThread.get(&hadInfo);
        ExcAssertEqual(payload, payload2);
        ExcAssertEqual(hadInfo, true);
#endif        
        
        return **payload;
    }

    template<typename Fn>
    void forEach(Fn onPayload)
    {
        std::unique_lock<std::mutex> guard(threadsLock);
        for (auto t: threads) {
            onPayload(t.get());
        }
    }

    size_t numThreads() const
    {
        return threads.size();
    }
};

} // namespace MLDB


/** processing_state.h
    Jeremy Barnes, 29 November 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#include "processing_state_fwd.h"
#include <atomic>
#include <exception>
#include <functional>
#include <type_traits>
#include <string>
#include <memory>

namespace MLDB {

// Function to generate a priority, that may be constructed from:
// - a lambda function
// - a priority value
// - nullptr

template<typename... Params>
struct PriorityFn<int (Params...)> {
    PriorityFn() = default;
    PriorityFn(int priority) : priority_(priority) { }
    PriorityFn(std::nullptr_t) {}

    template<typename Callable>
    PriorityFn(Callable && fn, decltype(fn(std::declval<Params>()...)) = 1) : fn_(std::move(fn)) { }

    template<typename... Args>
    int operator () (Args&&... args) const
    {
        if (this->fn_)
            return this->fn_(std::forward<Args>(args)...);
        else return this->priority_;
    }

    std::function<int (Params...)> fn_;
    int priority_ = 0;
};

// Function to call a continuation for when work is done and processing needs to
// continue. This can also indicate that the processing should stop. It can be
// constructed from:
// - a lambda function returning a bool, indicating whether to continue
// - a lambda function returning void, indicating that the processing should continue
// - nullptr

template<typename... Params>
struct ContinuationFn<bool (Params...)> {
    // Default constructor: ignore
    ContinuationFn() = default;

    // nullptr constructor: ignore
    ContinuationFn(std::nullptr_t) {}

    // Constructor for a function that returns a bool
    template<typename Callable>
    ContinuationFn(Callable && fn, std::enable_if_t<std::is_convertible_v<decltype(fn(std::declval<Params>()...)), bool>> * = 0)
        : bool_fn_(std::move(fn)) { }

    // Constructor for a function that returns void
    template<typename Callable>
    ContinuationFn(Callable && fn, std::enable_if_t<std::is_void_v<decltype(fn(std::declval<Params>()...))>> * = 0)
        : void_fn_(std::move(fn)) { }

    template<typename... Args>
    bool operator () (Args&&... args) const
    {
        if (this->bool_fn_)
            return this->bool_fn_(std::forward<Args>(args)...);
        else if (this->void_fn_) 
            this->void_fn_(std::forward<Args>(args)...);
        return true;
    }

    std::function<bool (Params...)> bool_fn_;
    std::function<void (Params...)> void_fn_;
};

// Structure that keep track of the overall state of processing
struct ProcessingState {

    enum State {
        RUNNING,
        STOPPED_FINISHED,
        STOPPED_EXCEPTION,
        STOPPED_USER
    };

    ProcessingState(int maxParallelism = -1);
    ProcessingState(ProcessingState & parent, int maxParallelism = -1);
    ~ProcessingState();

    // Must be called from a catch statement. This takes ownership of the currently active
    // exception and stops the processing.
    void takeException(std::string info);

    bool hasException() const;

    // Return an authorative answer to whether the processing has stopped. This is an expensive
    // call in a multi-threaded and especially multi-processor environment.
    bool is_stopped() const { return state_.load(std::memory_order_seq_cst) != RUNNING; }

    // Return a fast answer to whether the processing has stopped. This may return false negatives.
    bool relaxed_stopped() const { return state_.load(std::memory_order_relaxed) != RUNNING; }

    // Stop the processing
    void stop();

    // Submit a job with the given priority to be processed.
    void submit(int priority, std::string info, std::function<void ()> fn);

    void waitForAll();

    void rethrowIfException();

    // Is the processing single-threaded? If so, we need to ensure that we never block in any
    // of the jobs. Note that using this function is usually a design flaw on the processing
    // function.
    bool single_threaded() const;

    // Perform some useful work and return immediately if none is available or otherwise once
    // some forward progress has been made.
    void work() const;

    // Perform work until there is nothing outstanding in this pool of work.
    void work_until_finished() const;

protected:
    std::atomic<State> state_ = RUNNING;
    struct Itl;
    std::unique_ptr<Itl> itl;

    void runThread();
};

} // namespace MLDB

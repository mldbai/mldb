/* scoreboard.h
    Jeremy Barnes, 21 February 2007
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Scoreboard for keeping track of the progress of multithreaded operations.
*/

#pragma once

#include <atomic>
#include <optional>
#include <vector>
#include <mutex>
#include <iostream>
#include "mldb/arch/spinlock.h"
#include "mldb/compiler/compiler.h"
#include "mldb/arch/exception.h"
#include "mldb/base/exc_check.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

/*****************************************************************************/
/* SCOREBOARD                                                                */
/*****************************************************************************/

/** This structure allows us to keep track of the in progress and done
    status of work that is done across multiple threads.

    The work is conceptually split into work items, which are numbered
    sequentially from zero.

    When a thread begins working on a work item, it calls beginWorkItem().
    
    When it finishes, it calls endWorkItem(). This will return the number of
    the previous highest work item done, and the number of the new highest
    work item done. If there is cleanup to be done once a work item ends,
    then it can be done by that thread.

    The highestFinishedWorkItem() method returns the index of the highest
    work item that has been completed. Note that there may be earlier work
    items not completed.

    The highestInProgressWorkItem() method returns the index of the highest
    work item that is in progress.

    The progress() method returns the highest item which is both done and all
    of the previous work items are done.

    This structure is thread-safe. Initial implementation is via a spinlock
    but it could probably be made lock-free without changing the contract
    too much if it were to become a bottleneck (probably not due to the
    expectation that the work items should be large & they can be pre-
    chunked if necessary).
*/

using namespace std;

struct LinearScoreboard {

    LinearScoreboard(size_t maxInProgressWorkItems = 128)
        : maxInProgressWorkItems_(maxInProgressWorkItems),
          workItems_(maxInProgressWorkItems * 8, WorkItemState::NOT_STARTED)
    {
    }

    /** Begin work new work item n.  Will return true if this is the first
        thread to work on the item, or false if another thread has already
        started it.

        If this thread is too far ahead (index is more than
        maxInProgressWorkItems_ ahead of the highestInProgressWorkItem_),
        then it will return false.

        Output:
        - If started is true, then tooFarAhead is false, and the item started
        - Otherwise, if tooFarAhead is true, then started is false, and the
          item was not started because the rest of the work needs to catch up.
          This thread should probably either wait or contribute to the work
          already in progress.
        - If both are false, then the item was already in progress and this
          thread should not do anything. This may indicate a logic error in
          the program.
    */
    struct BeginWorkItemResult {
        bool started = false;
        bool tooFarAhead = false;
    };

    BeginWorkItemResult beginWorkItem(size_t index) MLDB_WARN_UNUSED_RESULT
    {
        //cerr << endl << "Begin work item " << index << endl;
        //dumpState();
        //cerr << endl;
        std::unique_lock<Mutex> guard(mutex_);
        ExcCheckGreaterEqual(index, workItemsBase_, "attempting to restart old work item");
        size_t i = index - workItemsBase_;
        if (i >= earliestNonCompletedItem_ + maxInProgressWorkItems_) {
            // Too many in progress
            return { false, true };
        }
        ExcAssert(i < workItems_.size());
        if (workItems_[i] != WorkItemState::NOT_STARTED) {
            // Already started
            return { false, false };
        }

        ++workItemsStarted_;

        workItems_[i] = WorkItemState::IN_PROGRESS;
        if (index >= nextWorkItem_) {
            nextWorkItem_ = index + 1;
        }

        // Started
        return { true, false };
    }

    /** End work on a work item.  Returns the number of the work item that
        was in progress before this one, and the number of the work item
        that is now in progress. */

    struct EndWorkItemResult {
        int64_t prevEarliestUncompletedWork = -1;
        int64_t newEarliestUncompletedWork = -1;
        int64_t count() const { return newEarliestUncompletedWork - prevEarliestUncompletedWork; }
        int64_t begin() const { return prevEarliestUncompletedWork; }
        int64_t end() const { return newEarliestUncompletedWork; }
    };

    EndWorkItemResult endWorkItem(size_t index)
    {
        //cerr << "endWorkItem " << index << endl;
        //dumpState();
        std::unique_lock<Mutex> guard(mutex_);
        ExcCheckGreaterEqual(index, workItemsBase_, "attempting to restart old work item");
        size_t i = index - workItemsBase_;
        ExcAssert(i < workItems_.size());
        if (workItems_[i] != WorkItemState::IN_PROGRESS) {
            // Not started or already finished
            MLDB_THROW_LOGIC_ERROR("attempt to finish unstarted or previously finished work item %d", (int)index);
        }

        ++workItemsCompleted_;

        EndWorkItemResult result;
        result.prevEarliestUncompletedWork = earliestNonCompletedItem_;

        workItems_[i] = WorkItemState::DONE;
        if (index == earliestNonCompletedItem_) {
            // Find the next earliest work item
            while (earliestNonCompletedItem_ < nextWorkItem_ &&
                   workItems_[earliestNonCompletedItem_ - workItemsBase_] == WorkItemState::DONE) {
                ++earliestNonCompletedItem_;
            }
        }

        size_t numUnused = earliestNonCompletedItem_ - workItemsBase_ - 1;
        if (numUnused + maxInProgressWorkItems_ > workItems_.size()) {
            //cerr << endl << "SHIFTING by " << numUnused << endl;
            //dumpState();
            //cerr << "numUnused = " << numUnused << " maxInProgressWorkItems_ = " << maxInProgressWorkItems_ << " workItems_.size() = " << workItems_.size() << endl;
            //cerr << "earliestNonCompletedItem_ = " << earliestNonCompletedItem_ << " workItemsBase_ = " << workItemsBase_ << endl;
            std::copy(workItems_.begin() + numUnused, workItems_.end(), workItems_.begin());
            std::fill(workItems_.begin() + workItems_.size() - numUnused, workItems_.end(), WorkItemState::NOT_STARTED);
            workItemsBase_ += numUnused;
            //cerr << "state after shift: " << endl;
            //dumpState();
        }

        result.newEarliestUncompletedWork = earliestNonCompletedItem_;
        return result;
    }

    ssize_t earliestNonCompletedItem() const
    {
        std::unique_lock<Mutex> guard(mutex_);
        return earliestNonCompletedItem_;
    }

#if 0
    /** Return the highest work item that has been completed. */
    ssize_t highestFinishedWorkItem() const
    {
        std::unique_lock<Mutex> guard(mutex_);
        return highestFinishedWorkItem_;
    }

    /** Return the highest work item that is in progress, or nothing
        if no work is in progress. */
    std::optional<size_t> highestInProgressWorkItem() const
    {
        std::unique_lock<Mutex> guard(mutex_);
        ssize_t result = nextWorkItem_ - 1;
        return result == earliestNonCompletedItem_ ? std::nullopt : std::optional<size_t>(result);
    }
#endif

    /** Return the highest work item that is both done and all of the previous
        work items are done, plus one.
        
        If zero work items have been submitted, progress is zero.
        If 100 work items have been submitted and all are done, progress is 100.
        If 100 work items have been submitted and all are done apart from the first, progress is zero.
    */
    size_t progress() const
    {
        std::unique_lock<Mutex> guard(mutex_);
        return earliestNonCompletedItem_;
    }

    void dumpState() const
    {
        auto dumpItemState = [] (WorkItemState state) -> char {
            return state == WorkItemState::NOT_STARTED ? 'N' :
                   state == WorkItemState::IN_PROGRESS ? 'I' : 'D';
        };

        cerr << "earliestNonCompletedItem_ = " << earliestNonCompletedItem_ << endl;
        cerr << "nextWorkItem_ = " << nextWorkItem_ << endl;
        cerr << "workItemsBase_ = " << workItemsBase_ << endl;
        cerr << "workItemsStarted_ = " << workItemsStarted_ << endl;
        cerr << "workItemsCompleted_ = " << workItemsCompleted_ << endl;

        cerr << "items: ";
        for (size_t i = 0; i < workItems_.size(); ++i) {
            cerr << dumpItemState(workItems_[i]);
        }
        cerr << endl;

#if 0
        cerr << "pre:    " << workItemsBase_ << " * C ";
        cerr << "before: " << earliestNonCompletedItem_ - workItemsBase << " = ";
        for (auto it = workItems_.begin(); it != workItems_.begin() + earliestNonCompletedItem_ - workItemsBase_; ++it) {
            cerr << dumpItemState(*it);
        }
        cerr << "curr:   " << nextWorkItem_ - workItemsBase_ << " = ";
        for (auto it = workItems_.begin() + earliestNonCompletedItem_ - workItemsBase_; it != workItems_.begin() + nextWorkItem_ - workItemsBase_; ++it) {
            cerr << dumpItemState(*it);
        }
        cerr << endl;
        for (sizeIt -
#endif
    }

private:
    enum class WorkItemState: uint8_t {
        NOT_STARTED,
        IN_PROGRESS,
        DONE
    };

    using Mutex = Spinlock;
    mutable Mutex mutex_;
    size_t maxInProgressWorkItems_;
    std::vector<WorkItemState> workItems_;
    ssize_t workItemsBase_ = 0;              //< offset for first work item
    ssize_t earliestNonCompletedItem_ = 0;   //< earliest work item that is not comlpeted.
    ssize_t nextWorkItem_ = 0;               //< all work items from this one are unstarted
    size_t workItemsStarted_ = 0;
    size_t workItemsCompleted_ = 0;
};


} // namespace MLDB

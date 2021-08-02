// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* runner_common.cc
   Wolfgang Sourdeau, 10 December 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
*/

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/errno.h>

#include "mldb/arch/exception.h"
#include "mldb/types/value_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/libc_value_descriptions.h"

#include "runner_common.h"


using namespace std;
using namespace MLDB;


namespace MLDB {

std::string
strLaunchError(LaunchError error)
{
    switch (error) {
    case LaunchError::NONE: return "no error";
    case LaunchError::READ_STATUS_PIPE: return "read() on status pipe";
    case LaunchError::STATUS_PIPE_WRONG_LENGTH:
        return "wrong message size reading launch pipe";
    case LaunchError::SUBTASK_LAUNCH: return "exec() launching subtask";
    case LaunchError::SUBTASK_WAITPID: return "waitpid waiting for subtask";
    case LaunchError::WRONG_CHILD: return "waitpid() returned the wrong child";
    }
    throw MLDB::Exception("unknown error launch error code %d",
                        error);
}

DEFINE_ENUM_DESCRIPTION(LaunchError);

LaunchErrorDescription::
LaunchErrorDescription()
{
    addValue("NONE",                     LaunchError::NONE,                     "No launch error");
    addValue("READ_STATUS_PIPE",         LaunchError::READ_STATUS_PIPE,         "Error reading status pipe");
    addValue("STATUS_PIPE_WRONG_LENGTH", LaunchError::STATUS_PIPE_WRONG_LENGTH, "Wrong length reading status pipe");
    addValue("SUBTASK_LAUNCH",           LaunchError::SUBTASK_LAUNCH,           "Error launching subtask");
    addValue("SUBTASK_WAITPID",          LaunchError::SUBTASK_WAITPID,          "Error calling waidpid()");
    addValue("WRONG_CHILD",              LaunchError::WRONG_CHILD,              "Wrong child was reaped");
}

std::string
statusStateAsString(ProcessState statusState)
{
    switch (statusState) {
    case ProcessState::UNKNOWN: return "UNKNOWN";
    case ProcessState::LAUNCHING: return "LAUNCHING";
    case ProcessState::RUNNING: return "RUNNING";
    case ProcessState::STOPPED: return "STOPPED";
    case ProcessState::DONE: return "DONE";
    }
    throw MLDB::Exception("unknown status %d", statusState);
}

DEFINE_ENUM_DESCRIPTION(ProcessState);

ProcessStateDescription::
ProcessStateDescription()
{
    addValue("UNKNOWN",   ProcessState::UNKNOWN,   "Unknown state");
    addValue("LAUNCHING", ProcessState::LAUNCHING, "Launching process");
    addValue("RUNNING",   ProcessState::RUNNING,   "Process is running");
    addValue("STOPPED",   ProcessState::STOPPED,   "Process has stopped");
    addValue("DONE",      ProcessState::DONE,      "Process is done");
}


/****************************************************************************/
/* PROCESS STATUS                                                           */
/****************************************************************************/

ProcessStatus::
ProcessStatus()
{
    // Doing it this way keeps ValGrind happy
    ::memset(this, 0, sizeof(*this));

    state = ProcessState::UNKNOWN;
    pid = -1;
    childStatus = -1;
    launchErrno = 0;
    launchErrorCode = LaunchError::NONE;
}

void
ProcessStatus::
setErrorCodes(int newLaunchErrno, LaunchError newErrorCode)
{
    launchErrno = newLaunchErrno;
    launchErrorCode = newErrorCode;
}

DEFINE_STRUCTURE_DESCRIPTION(ProcessStatus);

ProcessStatusDescription::
ProcessStatusDescription()
{
    addField("state", &ProcessStatus::state, "State of process");
    addAuto("pid", &ProcessStatus::pid, "Process ID of status");
    addField("childStatus", &ProcessStatus::childStatus, "Status of child", -1);
    addField("launchErrno", &ProcessStatus::launchErrno,
             "Errno for launch error", 0);
    addField("launchError", &ProcessStatus::launchErrno,
             "Error number for launch error");
    addField("usage", &ProcessStatus::usage,
             "Process statistics as returned by getrusage()");
}


/****************************************************************************/
/* PROCESS FDS                                                              */
/****************************************************************************/

ProcessFds::
ProcessFds()
    : stdIn(::fileno(stdin)),
      stdOut(::fileno(stdout)),
      stdErr(::fileno(stderr)),
      statusFd(-1)
{
}

/* child api */
void
ProcessFds::
closeRemainingFds()
{
    struct rlimit limits;
    ::getrlimit(RLIMIT_NOFILE, &limits);

    for (int fd = 0; fd < limits.rlim_cur; fd++) {
        if ((fd != STDIN_FILENO || stdIn == -1)
            && fd != STDOUT_FILENO && fd != STDERR_FILENO
            && fd != statusFd) {
            ::close(fd);
        }
    }
}

void
ProcessFds::
dupToStdStreams()
{
    auto dupToStdStream = [&] (int oldFd, int newFd) {
        if (oldFd != newFd) {
            int rc = ::dup2(oldFd, newFd);
            if (rc == -1) {
                throw MLDB::Exception(errno,
                                    "ProcessFds::dupToStdStream dup2");
            }
        }
    };
    if (stdIn != -1) {
        dupToStdStream(stdIn, STDIN_FILENO);
    }
    dupToStdStream(stdOut, STDOUT_FILENO);
    dupToStdStream(stdErr, STDERR_FILENO);
}

/* parent & child api */
void
ProcessFds::
close()
{
    auto closeIfNotEqual = [&] (int & fd, int notValue) {
        if (fd != notValue) {
            ::close(fd);
        }
    };
    closeIfNotEqual(stdIn, STDIN_FILENO);
    closeIfNotEqual(stdOut, STDOUT_FILENO);
    closeIfNotEqual(stdErr, STDERR_FILENO);
    closeIfNotEqual(statusFd, -1);
}

void
ProcessFds::
encodeToBuffer(char * buffer, size_t bufferSize)
    const
{
    int written = ::sprintf(buffer, "%.8x/%.8x/%.8x/%.8x",
                            stdIn, stdOut, stdErr, statusFd);
    if (written < 0) {
        throw MLDB::Exception("encoding failed");
    }

    /* bufferSize must be equal to the number of bytes used above, plus 1 for
       '\0' */
    if (written >= bufferSize) {
        throw MLDB::Exception("buffer overflow");
    }
}

void
ProcessFds::
decodeFromBuffer(const char * buffer)
{
    int decoded = ::sscanf(buffer, "%x/%x/%x/%x",
                           &stdIn, &stdOut, &stdErr, &statusFd);
    if (decoded < 0) {
        throw MLDB::Exception(errno, "decoding failed");
    }
}

void
ProcessFds::
writeStatus(const ProcessStatus & status)
    const
{
    int res = ::write(statusFd, &status, sizeof(status));
    if (res == -1)
        throw MLDB::Exception(errno, "write");
    else if (res != sizeof(status))
        throw MLDB::Exception("writing of status is incomplete");
}

} // namespace MLDB

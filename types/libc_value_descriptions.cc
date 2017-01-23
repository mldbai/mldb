/** libc_value_descriptions.cc
    Wolfgang Sourdeau, 7 July 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "mldb/types/structure_description.h"
#include "mldb/types/libc_value_descriptions.h"

using namespace MLDB;


DEFINE_STRUCTURE_DESCRIPTION(timeval);

timevalDescription::
timevalDescription()
{
    addField("tv_sec", &timeval::tv_sec, "seconds");
    addField("tv_usec", &timeval::tv_usec, "micro seconds", (long)0);
}


DEFINE_STRUCTURE_DESCRIPTION(rusage);

rusageDescription::
rusageDescription()
{
    addField("utime", &rusage::ru_utime, "user CPU time used");
    addField("stime", &rusage::ru_stime, "system CPU time used");
    addField("maxrss", &rusage::ru_maxrss, "maximum resident set size", (long)0);
    addField("ixrss", &rusage::ru_ixrss, "integral shared memory size", (long)0);
    addField("idrss", &rusage::ru_idrss, "integral unshared data size", (long)0);
    addField("isrss", &rusage::ru_isrss, "integral unshared stack size", (long)0);
    addField("minflt", &rusage::ru_minflt, "page reclaims (soft page faults)", (long)0);
    addField("majflt", &rusage::ru_majflt, "page faults (hard page faults)", (long)0);
    addField("nswap", &rusage::ru_nswap, "swaps", (long)0);
    addField("inblock", &rusage::ru_inblock, "block input operations", (long)0);
    addField("oublock", &rusage::ru_oublock, "block output operations", (long)0);
    addField("msgsnd", &rusage::ru_msgsnd, "IPC messages sent", (long)0);
    addField("msgrcv", &rusage::ru_msgrcv, "IPC messages received", (long)0);
    addField("nsignals", &rusage::ru_nsignals, "signals received", (long)0);
    addField("nvcsw", &rusage::ru_nvcsw, "voluntary context switches", (long)0);
    addField("nivcsw", &rusage::ru_nivcsw, "involuntary context switches", (long)0);
}

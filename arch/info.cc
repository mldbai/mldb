// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* info.cc
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

*/

#include "mldb/arch/info.h"
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <pwd.h>
#include <errno.h>
#include "mldb/arch/exception.h"
#include "mldb/arch/cpuid.h"
#include <fstream>
#include <iostream>
#include <sys/time.h>
#include <time.h>
#include "mldb/arch/cpu_info.h"
#include "mldb/base/scope.h"
#include <dirent.h>
#include "mldb/arch/format.h"
#include <string.h>
#include <sys/stat.h>

#ifdef __APPLE__
#include <libproc.h>
#include <sys/proc_info.h>
#include <mach-o/dyld.h>
#endif /* __APPLE__ */

using namespace std;

namespace MLDB {

int userid()
{
    return getuid();
}

std::string userid_to_username(int userid)
{
    struct passwd pwbuf;
    size_t buflen = sysconf(_SC_GETPW_R_SIZE_MAX);
    char buf[buflen];
    struct passwd * result = 0;

    int res = getpwuid_r(userid, &pwbuf, buf, buflen, &result);

    if (res != 0)
        throw Exception(errno, "userid_to_username()", "getpwuid_r");

    if (result == 0)
        throw Exception("usedid_to_username(): userid unknown");
    
    return result->pw_name;
}

std::string username()
{
    return userid_to_username(userid());
}

std::string hostname()
{
    char buf[128];
    int res = gethostname(buf, 128);
    if (res != 0)
        throw Exception(errno, "hostname", "hostname");
    buf[127] = 0;
    return buf;
}

std::string now()
{
    struct timeval tv;

    int res = gettimeofday(&tv, NULL);
    if (res != 0)
        throw Exception(errno, "now", "gettimeofday");
    
    char buf[128];
    ctime_r(&tv.tv_sec, buf);

    if (res != 0)
        throw Exception(errno, "now", "ctime");
    
    return buf;
}

std::string all_info()
{
    return now() + " " + username() + " " + hostname();
}

size_t num_open_files()
{
#ifdef __linux__
    DIR * dfd = opendir("/proc/self/fd");
#elif defined(__APPLE__)
    DIR * dfd = opendir("/dev/fd");
#else
#  error "Tell us how to get open file count on your OS"
#endif

    if (dfd == 0)
        throw Exception("num_open_files(): opendir(): "
                        + string(strerror(errno)));

    Scope_Exit(closedir(dfd));

    size_t result = 0;
    
    dirent entry;
    for (dirent * current = &entry;  current;  ++result) {
        int res = readdir_r(dfd, &entry, &current);
        if (res != 0)
            throw Exception("num_open_files(): readdir_r: "
                            + string(strerror(errno)));
    }

    return result;
}

std::string fd_to_filename(int fd)
{
#if __linux__
    if (fd == -1)
        throw Exception("fd_to_filename(): invalid filename");

    size_t buffer_size = 256;

    string fn = format("/proc/self/fd/%d", fd);

    for (;;) {
        char buf[buffer_size];
        ssize_t ret = readlink(fn.c_str(), buf, buffer_size);
        if (ret == -1 && errno == -ENOENT)
            throw Exception("fd_to_filename(): fd is unknown");
        if (ret == -1)
            throw Exception(errno, "fd_to_filename", "readlink()");

        if (ret == buffer_size) {
            if (buffer_size > 1024)
                throw Exception("filename too long");
            buffer_size *= 2;
            continue;
        }

        buf[ret] = 0;
        return buf;
    }
#elif __APPLE__
    // Get the buffer size needed
    struct vnode_fdinfowithpath info;
    memset(&info, 0, sizeof(info));
    errno = 0;
    int res = proc_pidfdinfo(getpid(), fd, PROC_PIDFDVNODEPATHINFO, &info, sizeof(info));
    if (res == 0)
        throw MLDB::Exception("proc_pidfdinfo() error: " + string(strerror(errno)));
    return info.pvip.vip_path;
#else
#  error "Tell us how to turn a fd into a filename for your platform"
#endif
}

namespace {

#if defined(__linux__)
static std::string get_link_target(const std::string & link)
{
    /* Interface to the readlink call */
    size_t bufsize = 1024;
    
    /* Loop over, making the buffer successively larger if it is too small. */
    while (true) {  // break in loop
        char buf[bufsize];
        int res = readlink(link.c_str(), buf, bufsize);
        if (res == -1)
            throw MLDB::Exception(errno, "readlink", "get_link_name()");
        if (res == bufsize) {
            bufsize *= 2;
            continue;
        }
        buf[res] = 0;
        return buf;
    }
}
#endif

} // file scope

std::string get_exe_name()
{
#if defined(__linux__)
    return get_link_target("/proc/self/exe");
#elif defined(__APPLE__)
    char path[PATH_MAX + 1];
    uint32_t size = sizeof(path);
    if (_NSGetExecutablePath(path, &size) == 0)
        return path;
    else
        throw MLDB::Exception("_NSGetExecutablePath: logic error; PATH_MAX is not enough");
#endif
}

} // namespace MLDB

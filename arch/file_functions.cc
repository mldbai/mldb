/* file_functions.cc
   Jeremy Barnes, 14 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Functions to deal with files.
*/

#include "mldb/arch/file_functions.h"
#include "mldb/base/exc_assert.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include "mldb/utils/string_functions.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/spinlock.h"
#include <iostream>
#include <map>
#include <grp.h>
#include <exception>
#include "mldb/base/scope.h"
#include <mutex>


using namespace std;
using namespace MLDB;

namespace MLDB {

size_t get_file_size(int fd)
{
    struct stat stats;
    int result = fstat(fd, &stats);
    if (result != 0)
        throw Exception("fstat: " + string(strerror(errno)));
    return stats.st_size;
}

size_t get_file_size(const std::string & filename)
{
    struct stat stats;
    int result = stat(filename.c_str(), &stats);
    if (result != 0)
        throw Exception("stat: " + string(strerror(errno)));
    return stats.st_size;
}

std::string get_link_target(const std::string & link)
{
    /* Interface to the readlink call */
    size_t bufsize = 1024;
    
    /* Loop over, making the buffer successively larger if it is too small. */
    while (true) {  // break in loop
        char buf[bufsize];
        int res = readlink(link.c_str(), buf, bufsize);
        if (res == -1)
            throw Exception(errno, "readlink", "get_link_name()");
        if (res == bufsize) {
            bufsize *= 2;
            continue;
        }
        buf[res] = 0;
        return buf;
    }
}

std::string get_name_from_fd(int fd)
{
    string fname = MLDB::format("/proc/self/fd/%d", fd);
    return get_link_target(fname);
}

inode_type get_inode(const std::string & filename)
{
    struct stat st;

    int res = stat(filename.c_str(), &st);

    if (res != 0)
        throw Exception(errno, "fstat", "get_inode()");

    return make_pair(st.st_dev, st.st_ino);
}

inode_type get_inode(int fd)
{
    struct stat st;

    int res = fstat(fd, &st);

    if (res != 0)
        throw Exception(errno, "fstat", "get_inode()");

    return make_pair(st.st_dev, st.st_ino);
}

void delete_file(const std::string & filename)
{
    int res = unlink(filename.c_str());
    if (res != 0)
        throw Exception(errno, "couldn't delete file " + filename, "unlink");
}

void set_file_flag(int fd, int newFlag)
{
    int oldFlags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, oldFlags | newFlag);
}

void unset_file_flag(int fd, int oldFlag)
{
    int oldFlags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, oldFlags & ~oldFlag);
}

bool is_file_flag_set(int fd, int flag)
{
    int oldFlags = fcntl(fd, F_GETFL, 0);
    return ((oldFlags & flag) == flag);
}

/** Does the file exist? */
bool fileExists(const std::string & filename)
{
    struct stat stats;
    int res = stat(filename.c_str(), &stats);
    if (res == -1)
        return false;  // no chunks
    return true;
}

void set_permissions(std::string filename,
                     const std::string & perms,
                     const std::string & group)
{
    if (filename.find("ipc://") == 0)
        filename = string(filename, 6);

    char * endptr;
    if (perms != "") {
        int mode = strtol(perms.c_str(), &endptr, 8);
        if (*endptr != '\0')
            throw Exception(errno, "parsing permission modes %s",
                            perms.c_str());
        
        cerr << "setting permissions of " << filename << " to " << perms
             << "(" << mode << ")" << endl;

        int res = chmod(filename.c_str(), mode);
        if (res == -1)
            throw Exception(errno, "chmod on %s in setPermissions");
    }

    if (group != "") {
        int groupNum = strtol(group.c_str(), &endptr, 10);
        if (*endptr != '\0') {
            struct group * gr = getgrnam(group.c_str());
            if (gr == 0)
                throw Exception(errno, "finding group \"%s\"", group.c_str());
            groupNum = gr->gr_gid;
        }
        
        cerr << "setting group of " << filename << " to " << groupNum
             << " for group " << group << endl;
        
        int res = chown(filename.c_str(), -1, groupNum);

        if (res == -1)
            throw Exception(errno, "chown in setPermissions");
    }
}

/** Call fdatasync on the file. */
void syncFile(const std::string & filename)
{
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd == -1)
        throw MLDB::Exception(errno, "syncFile for " + filename);
    Scope_Exit(close(fd));
    int res = fdatasync(fd);
    if (res == -1)
        throw MLDB::Exception(errno, "fdatasync for " + filename);
}

} // namespace MLDB

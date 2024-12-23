/* file_functions.cc
   Jeremy Barnes, 14 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Functions to deal with files.
*/

#include "file_read_buffer.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <map>
#include "mldb/arch/spinlock.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/file_functions.h"
#include <mutex>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* FILE_READ_BUFFER                                                          */
/*****************************************************************************/

/** Helper class to specify and clean up a memory mapped region. */
class File_Read_Buffer::MMap_Region
    : public File_Read_Buffer::Region {

    inode_type inode;

    MMap_Region(int fd, inode_type inode)
        : inode(inode)
    {
        size = get_file_size(fd);

        if (size == 0) {
            start = 0;
        }
        else {
            start = (const char *)mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
            if (start == MAP_FAILED)
                throw Exception(errno, "mmap", "MMap_Region()");
        }

    }

    MMap_Region(const MMap_Region & other);

    MMap_Region & operator = (const MMap_Region & other);

    typedef std::map<inode_type, std::weak_ptr<MMap_Region> > cache_type;

    static cache_type & cache()
    {
        static cache_type result;
        return result;
    }

    static Spinlock & lock()
    {
        static Spinlock result;
        return result;
    }

public:
    static std::shared_ptr<MMap_Region> get(int fd)
    {
        std::lock_guard<Spinlock> guard(lock());

        inode_type inode = get_inode(fd);
        std::weak_ptr<MMap_Region> & ptr = cache()[inode];

        std::shared_ptr<MMap_Region> result;
        result = ptr.lock();
        
        if (!result) {
            result.reset(new MMap_Region(fd, inode));
            ptr = result;
        }

        return result;
    }

    virtual ~MMap_Region()
    {
        if  (size) munmap((void *)start, size);

        std::lock_guard<Spinlock> guard(lock());
        if (cache()[inode].expired())
            cache().erase(inode);
    }
};

/** Region that comes from a memory block. */
class File_Read_Buffer::Mem_Region : public File_Read_Buffer::Region {
public:
    Mem_Region(const char * start, size_t size,
               std::function<void ()> onDone)
    {
        this->start = start;
        this->size = size;
        this->onDone = onDone;
    }

    std::function<void ()> onDone;

    virtual ~Mem_Region()
    {
        if (onDone)
            onDone();
    }
};

File_Read_Buffer::Region::~Region()
{
}


/*****************************************************************************/
/* FILE_READ_BUFFER                                                          */
/*****************************************************************************/

File_Read_Buffer::File_Read_Buffer()
{
}

File_Read_Buffer::File_Read_Buffer(const Utf8String & filename)
{
    open(filename);
}

File_Read_Buffer::File_Read_Buffer(int fd)
{
    open(fd);
}

File_Read_Buffer::File_Read_Buffer(const char * start, size_t length,
                                   const Utf8String & filename,
                                   std::function<void ()> onDone)
{
    open(start, length, filename, onDone);
}

File_Read_Buffer::File_Read_Buffer(const File_Read_Buffer & other)
    : filename_(other.filename_), region(other.region)
{
}

void File_Read_Buffer::open(const Utf8String & filename)
{
    int fd = ::open(filename.c_str(), O_RDONLY);

    if (fd == -1)
        throw Exception(errno, filename, "File_Read_Buffer::open()");

    try {
        region = MMap_Region::get(fd);
        filename_ = filename;
    }
    catch (...) {
        ::close(fd);
        throw;
    }
    ::close(fd);
}

void File_Read_Buffer::open(int fd)
{
    region = MMap_Region::get(fd);
    filename_ = get_name_from_fd(fd);
}

void File_Read_Buffer::open(const char * start, size_t length,
                            const Utf8String & filename,
                            std::function<void ()> onDone)
{
    region.reset(new Mem_Region(start, length, onDone));
    filename_ = filename;
}

void File_Read_Buffer::close()
{
    region.reset();
    filename_ = "";
}

} // namespace MLDB

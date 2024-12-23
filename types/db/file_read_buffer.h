/* file_read_buffer.h                                              -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   
   Memory map wrapper.

*/

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <stdint.h>
#include "mldb/types/string.h"

namespace MLDB {

/*****************************************************************************/
/* FILE_READ_BUFFER                                                          */
/*****************************************************************************/

/** A class that memory maps a file in order to allow it to be read as a
    buffer.  Usually much faster than streams, as the virtual memory hardware
    can do all the paging for us.
*/

class File_Read_Buffer {
public:
    File_Read_Buffer();
    File_Read_Buffer(const Utf8String & filename);
    File_Read_Buffer(int fd);
    File_Read_Buffer(const File_Read_Buffer & other);
    File_Read_Buffer(const char * start, size_t length,
                     const Utf8String & fileName = "anonymous memory",
                     std::function<void ()> onDone = nullptr);

    void open(const Utf8String & filename);
    void open(int fd);

    void open(const char * start, size_t length,
              const Utf8String & filename = "anonymous memory",
              std::function<void ()> onDone = nullptr);
    
    void close();

    const char * start() const { return region->start; }
    const char * end() const { return region->start + region->size; }
    size_t size() const { return region->size; }

    Utf8String filename() const { return filename_; }

    /* Only access this if you know what you are doing... */
    class Region {
    public:
        virtual ~Region();
        const char * start;
        size_t size;
    };

    Utf8String filename_;
    std::shared_ptr<Region> region;

    class MMap_Region;
    class Mem_Region;
};

} // namespace MLDB

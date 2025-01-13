/* iostream_adaptors.cc
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
   
   This file is part of "Jeremy's Machine Learning Library", copyright (c)
   1999-2015 Jeremy Barnes.
   
   Apache 2.0 license.

   ---
   
   IOStream adaptors for the implementation of filter streams.
   Interface is based on boost::iostream.
*/

#include "iostream_adaptors.h"
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "mldb/base/scope.h"
#include "mldb/arch/vm.h"


namespace MLDB {

using namespace std;

/*****************************************************************************/
/* FREE FUNCTIONS                                                            */
/*****************************************************************************/


/*****************************************************************************/
/* BASE CLASSES CLASSES                                                      */
/*****************************************************************************/


/*****************************************************************************/
/* FILTER OSTREAM CLASSES                                                     */
/*****************************************************************************/

ssize_t buffered_ostreambuf::flush_buffer()
{
    auto res = sink_write_all(buffer_.data(), pptr() - buffer_.data());
    reset_buffer();
    return res;
}
ssize_t buffered_ostreambuf::sink_write_all(const char * s, size_t n)
{
    size_t n2 = n;
    while (n) {
        auto res = sink_write(s, n);
        if (res == EOF) return EOF;
        n -= res;
        s += res;
    }
    return n2;
}

/*****************************************************************************/
/* FILTER ISTREAM CLASSES                                                     */
/*****************************************************************************/

std::streamsize buffered_istreambuf::xsgetn(char * s, std::streamsize n)
{
    // NOTE: This function should never return EOF. If no characters are read,
    // then it should return 0.

    if (n == 0) return 0;

    // First get any buffered characters
    //cerr << "xsgetn of " << n << " characters" << endl;
    size_t done = std::min(n, egptr() - gptr());
    std::memcpy(s, gptr(), done);
    gbump(done); // results in gptr() == egptr() if done == n
    //cerr << "done = " << done << endl;
    while (!eof() && done < n) {
        auto nread = source_read(s + done, n - done);
        if (nread == EOF) break;
        done += nread;
    }
    //cerr << "end of xsgetn: done = " << done << " eof = " << eof() << endl;
    ExcAssert(done > 0 || eof());
    return done;
}

std::streampos buffered_istreambuf::seekoff(std::streamoff off, std::ios_base::seekdir way, std::ios_base::openmode which)
{
    if (which != std::ios_base::in) return EOF;
    return this->seek(off, way);
}

void buffered_istreambuf::buf_fill()
{
    require_empty();
    auto n = source_read(buffer_.data(), buffer_.size());
    if (n != EOF) setg(buffer_.data(), buffer_.data(), buffer_.data() + n);
}

/*****************************************************************************/
/* MEMORY MAPPING                                                            */
/*****************************************************************************/


mapped_file_source::mapped_file_source(mapped_file_params params)
{
    Scope_Exit(if (params.fd != -1) ::close(params.fd));
    if (params.fd == -1) {
        params.fd = open(params.filename.c_str(), O_RDONLY);
        if (params.fd == -1)
            throw Exception(errno, "open");
    }
    if (params.size == -1) {
        struct stat st;
        if (fstat(params.fd, &st) == -1)
            throw Exception(errno, "fstat");
        params.size = st.st_size;
    }
    size_t offset = roundDownToPageSize(params.offset);
    size_t skip = params.offset - offset;
    size_t capacity = roundUpToPageSize(params.size + params.extra_capacity - offset);
    size_t capacityWithoutGuardPage = roundUpToPageSize(params.size - offset);
    //cerr << "capacity = " << capacity << " capacityWithoutGuardPage = " << capacityWithoutGuardPage << endl;
    //cerr << "skip = " << skip << endl;
    //cerr << "offset = " << offset << endl;
    //
    //cerr << "mapping " << capacity << " of " << params.size << " bytes from " << params.filename << " at offset " << params.offset << endl;
    //cerr << "skipping " << skip << " bytes" << endl;

    auto check_mmap = [&] (void * p) {
        if (p == MAP_FAILED)
            throw Exception(errno, "mmap");
    };

    std::shared_ptr<void> addr;
    // In order to map the file with extra capacity, we may need to map an extra page
    // if the file itself had nearly filled up the last page. If that happens, we first
    // map the entire range as nulls, and then map the file on top of the first part
    // so that the extra capacity is at the end. This is because OSX doesn't like
    // MAP_FIXED on an unmapped page.
    if (capacity == capacityWithoutGuardPage) {
        // No extra page
        addr = std::shared_ptr<void>
            (mmap(nullptr, capacity,
                PROT_READ, MAP_PRIVATE, params.fd, params.offset),
            [=] (void * p) { munmap(p, page_size); });
        check_mmap(addr.get());
    }
    else {
        // Firstly, make an anonymous zero-filled mapping
        addr = std::shared_ptr<void>
            (mmap(nullptr, capacity, PROT_READ, MAP_PRIVATE | MAP_ANON, -1, 0),
            [=] (void * p) { munmap(p, page_size); });
        check_mmap(addr.get());
        
        // Now remap the file on all but the last page
        void * addr2 = mmap(addr.get(), capacity - page_size, PROT_READ, MAP_PRIVATE | MAP_FIXED, params.fd, params.offset);
        if (addr2 == MAP_FAILED)
            throw Exception(errno, "mmap");
        if (addr2 != addr.get())
            MLDB_THROW_LOGIC_ERROR("mmap: remapped to different address");
    }

    data_ = (const char *)addr.get() + skip;
    ptr_ = data_;
    size_ = params.size - params.offset;
    capacity_ = capacity - skip;
    region_ = std::move(addr);

    //cerr << "data_ = " << (void *)data_ << endl;
    //cerr << "ptr_ = " << (void *)ptr_ << endl;
    //cerr << "size_ = " << size_ << endl;
    //cerr << "capacity_ = " << capacity_ << endl;
    //cerr << "region_ = " << (void *)region_.get() << endl;
}

ssize_t mapped_file_source::read(char * s, size_t n)
{
    //cerr << "reading " << n << " bytes from mapped file" << endl;
    size_t left = avail();
    if (left == 0)
        return EOF;
    n = std::min(n, left);
    memcpy(s, ptr_, n);
    ptr_ += n;
    //cerr << "read " << n << " bytes from mapped file" << endl;
    return n;
}

std::streampos mapped_file_source::seek(std::streamoff off, std::ios_base::seekdir way)
{
    ssize_t currentOff = ptr_ - data_;
    cerr << "currentOff = " << currentOff << endl;
    std::streamoff newOff;
    switch (way) {
    case std::ios_base::beg:
        newOff = off;
        break;
    case std::ios_base::cur:
        newOff = currentOff + off;
        break;
    case std::ios_base::end:
        newOff = size_ + off;
        break;
    default:
        MLDB_THROW_LOGIC_ERROR("invalid seek direction");
    }

    if (newOff < 0 || newOff > size_)
        return EOF;

    if (currentOff == newOff)
        return newOff;

    ptr_ = data_ + newOff;
    return newOff == size_ ? EOF : newOff;
}

/*****************************************************************************/
/* FILE DESCRIPTOR                                                           */
/*****************************************************************************/

file_descriptor_sink::file_descriptor_sink(int fd, close_handle close_handle)
    : fd_(fd), close_handle_(close_handle)
{
}

file_descriptor_sink::file_descriptor_sink(file_descriptor_sink && other)
    : fd_(other.fd_), close_handle_(other.close_handle_)
{
    other.fd_ = -1;
}

void file_descriptor_sink::swap(file_descriptor_sink & other)
{
    std::swap(fd_, other.fd_);
    std::swap(close_handle_, other.close_handle_);
}

file_descriptor_sink::~file_descriptor_sink()
{
    close();
}

void file_descriptor_sink::close()
{
    if (fd_ != -1) {
        if (close_handle_ == always_close_handle)
            ::close(fd_);
        fd_ = -1;
    }
}

void file_descriptor_sink::flush()
{
}

ssize_t file_descriptor_sink::write(const char * data, size_t len)
{
    auto res = ::write(fd_, data, len);
    if (res == -1)
        throw Exception(errno, "write");
    return res;
}

} // namespace MLDB

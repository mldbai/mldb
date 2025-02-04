/* iostream_adaptors.h
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

#pragma once

#include <iostream>
#include <streambuf>
#include <vector>
#include <memory>
#include <cstring>
#include "mldb/arch/exception.h"
#include "mldb/compiler/compiler.h"
#include "mldb/base/exc_check.h"
#include "mldb/base/exc_assert.h"

namespace MLDB {

/*****************************************************************************/
/* FREE FUNCTIONS                                                            */
/*****************************************************************************/

template<typename Stream>
struct has_flush {
    template<typename S> static auto test(int) -> decltype(std::declval<S>().flush(), std::true_type());
    template<typename S> static auto test(...) -> std::false_type;
    using type = decltype(test<Stream>(0));
    static constexpr bool value = type::value;
};

template<typename Stream> constexpr bool has_flush_v = has_flush<Stream>::value;
template<typename Stream> using has_flush_t = has_flush<Stream>::type;

template<typename Stream> void flush_stream(Stream& stream, std::enable_if_t<has_flush_v<Stream>> * = 0) { stream.flush(); }
template<typename Stream> void flush_stream(Stream& stream, std::enable_if_t<!has_flush_v<Stream>> * = 0) { }


template<typename Stream>
struct has_close {
    template<typename S> static auto test(int) -> decltype(std::declval<S>().close(), std::true_type());
    template<typename S> static auto test(...) -> std::false_type;
    using type = decltype(test<Stream>(0));
    static constexpr bool value = type::value;
};

template<typename Stream> constexpr bool has_close_v = has_close<Stream>::value;
template<typename Stream> using has_close_t = has_close<Stream>::type;

template<typename Stream> void close_stream(Stream& stream, std::enable_if_t<has_close_v<Stream>> * = 0) { stream.close(); }
template<typename Stream> void close_stream(Stream& stream, std::enable_if_t<!has_close_v<Stream>> * = 0) { }


template<typename SinkOrSource>
struct has_seek {
    template<typename S> static auto test(int) -> decltype(std::declval<S>().seek(0, std::ios::beg), std::true_type());
    template<typename S> static auto test(...) -> std::false_type;
    using type = decltype(test<SinkOrSource>(0));
    static constexpr bool value = type::value;
};

template<typename SinkOrSource> constexpr bool has_seek_v = has_seek<SinkOrSource>::value;
template<typename SinkOrSource> using has_seek_t = has_seek<SinkOrSource>::type;

template<typename SinkOrSource> void seek_stream(SinkOrSource& stream, std::streamoff off, std::ios::seekdir dir, std::enable_if_t<has_seek_v<SinkOrSource>> * = 0) { stream.seek(off, dir); }
template<typename SinkOrSource> void seek_stream(SinkOrSource& stream, std::streamoff off, std::ios::seekdir dir, std::enable_if_t<!has_seek_v<SinkOrSource>> * = 0) { }


template<typename Stream>
ssize_t write_stream(std::ostream& stream, const char * data, size_t len)
{
    stream.write(data, len);
    if (!stream)
        throw Exception("unable to write data");
    return len;
}

template<typename Stream> ssize_t write_stream(Stream& stream, const char * data, size_t len) { return stream.write(data, len); }
template<typename Stream> ssize_t read_stream(Stream& stream, char * data, size_t len) { return stream.read(data, len); }


/*****************************************************************************/
/* BASE CLASSES                                                              */
/*****************************************************************************/

// Similar interface to boost::iostreams, except that these:
// a) don't use boost
// b) minimize template metaprogramming
// c) can map extra space at the end of a memory mapped file, allowing for
//    algorithms that require specific alignment to work efficiently

struct filter_streambuf: public std::streambuf {
    virtual ~filter_streambuf() = default;
    virtual void close() = 0; // should ensure that nothing more is written to the output
    void set_output(std::streambuf * sb) { output_ = sb; }
    bool has_output() const { return output_; }

    std::streamsize write(const char* s, std::streamsize n) { return require_output()->sputn(s, n); }
    std::streamsize read(char* s, std::streamsize n) { return require_output()->sgetn(s, n); }
    void flush() { require_output()->pubsync(); }

protected:
    void check_output() const { ExcCheck(output_, "Attempt to use non-capped filter_streambuf"); }
    std::streambuf * require_output() const { check_output(); return output_; }

private:
    std::streambuf * output_ = nullptr;
};

inline void close_stream(std::streambuf & stream) { if (auto * buf = dynamic_cast<filter_streambuf *>(&stream)) buf->close(); }

template<typename IOStream>
struct filtering_stream: public IOStream {
    using IOStream::rdbuf;
    filtering_stream() : IOStream(nullptr)
    {
    }

    virtual ~filtering_stream()
    {
        MLDB_EAT_EXCEPTIONS(flush_stream((IOStream&)*this));
        rdbuf(nullptr);
        for (auto & filter: filters_) MLDB_EAT_EXCEPTIONS(filter->close());
        if (cap_) MLDB_EAT_EXCEPTIONS(cap_->close());
    }

    void push(std::streambuf & sb, size_t /* buffer_size */ = 4096) { check_not_capped(); push_streambuf(sb); }

    bool empty() const { return filters_.empty() && !cap_ && !rdbuf(); }
    bool capped() const { return rdbuf() && (filters_.empty() || filters_.back()->has_output()); }

protected:
    void check_not_capped() const { ExcCheck(!cap_, "Attempt to push a second sink into a filtering_stream"); }
    void push_streambuf(std::streambuf & sb) { if (!rdbuf()) { rdbuf(&sb); } else filters_.back()->set_output(&sb); }
    template<typename Buf, typename Cap> void push_cap(Cap cap, size_t buffer_size) { check_not_capped(); push_streambuf(*(cap_ = std::make_unique<Buf>(std::move(cap), buffer_size))); }
    template<typename Buf, typename Filter> void push_filter(Filter filter, size_t buffer_size)
    { 
        check_not_capped();
        filters_.emplace_back(std::make_unique<Buf>(std::move(filter), buffer_size));
        push_streambuf(*filters_.back());
    }
private:
    std::vector<std::unique_ptr<filter_streambuf>> filters_;
    std::unique_ptr<filter_streambuf> cap_;
};

template struct filtering_stream<std::istream>;
template struct filtering_stream<std::ostream>;


/*****************************************************************************/
/* FILTER OSTREAM CLASSES                                                     */
/*****************************************************************************/

struct buffered_ostreambuf: public filter_streambuf {
    buffered_ostreambuf(size_t buffer_size = 4096) : buffer_(buffer_size) { reset_buffer(); }
protected:
    std::vector<char> buffer_;
    virtual ssize_t flush_sink() = 0;
    virtual int sync() override { return (flush_buffer() == EOF || flush_sink() == EOF) ? EOF : 0; }
    virtual int overflow(int c) override { if (flush_buffer() == EOF) return EOF; return sputc(c); }
    virtual std::streamsize xsputn(const char * s, std::streamsize n) override { return (flush_buffer() == EOF || sink_write_all(s, n) == EOF) ? EOF : n; }
    ssize_t flush_buffer();
    template<typename Sink, typename... Args> ssize_t sink_write_impl(const char * s, size_t n, Sink & sink, Args&&... args) { return sink.write(std::forward<Args>(args)..., s, n); }
    virtual ssize_t sink_write(const char * s, size_t n) = 0;
    ssize_t sink_write_all(const char * s, size_t n);
    void reset_buffer() { setp(buffer_.data(), buffer_.data() + buffer_.size()); }
};

template<typename Sink>
struct sink_ostreambuf: public buffered_ostreambuf {
    using base_type = buffered_ostreambuf;
    sink_ostreambuf(Sink sink, size_t buffer_size = 4096) : base_type(buffer_size), sink_(std::move(sink)) {}
    virtual ~sink_ostreambuf() = default;
    const Sink & sink() const { return sink_; }
    Sink & sink() { return sink_; }
    virtual void close() override { flush_buffer(); flush_stream(sink_); sink_.close(); }
protected:
    virtual ssize_t flush_sink() override { flush_stream(sink_); return 0; }
    virtual ssize_t sink_write(const char * s, size_t n) override { return sink_write_impl(s, n, sink_); }
private:
    Sink sink_;
};

template<typename Filter>
struct filtering_ostreambuf: public buffered_ostreambuf {
    using base_type = buffered_ostreambuf;
    filtering_ostreambuf(Filter filter, size_t buffer_size = 4096) : base_type(buffer_size), filter_(std::move(filter)) {}
    virtual ~filtering_ostreambuf() = default;
    const Filter & filter() const { return filter_; }
    Filter & filter() { return filter_; }
    virtual void close() override { flush_buffer(); filter_.flush(*this); filter_.close(*this); }
protected:
    virtual ssize_t flush_sink() override { filter_.flush(*this); return 0; }
    virtual ssize_t sink_write(const char * s, size_t n) override { return sink_write_impl(s, n, filter_, *this); }
private:
    Filter filter_;
};

struct filtering_ostream: public filtering_stream<std::ostream> {
    using filtering_stream<std::ostream>::push;
    template<typename Sink> void push(Sink sink, size_t buffer_size = 4096, typename Sink::is_sink::type = {}) { push_cap<sink_ostreambuf<Sink>>(std::move(sink), buffer_size); }
    template<typename Filter> void push(Filter filter, size_t buffer_size = 4096, typename Filter::is_filter::type = {}) { push_filter<filtering_ostreambuf<Filter>>(std::move(filter), buffer_size); }
};


/*****************************************************************************/
/* FILTER ISTREAM CLASSES                                                     */
/*****************************************************************************/

struct buffered_istreambuf: public filter_streambuf {
    buffered_istreambuf(size_t buffer_size = 4096) : buffer_(buffer_size) {}
    virtual int underflow() override { buf_fill(); ExcAssert((eof() && buf_empty()) || !buf_empty()); return eof() ? EOF : *gptr(); }
    virtual std::streamsize xsgetn(char * s, std::streamsize n) override;
    virtual std::streampos seekoff(std::streamoff off, std::ios_base::seekdir way, std::ios_base::openmode which) override;
    bool eof() const { return eof_; }

protected:
    std::vector<char> buffer_;
    ssize_t pos_ = 0;
    bool eof_ = false;
    bool buf_empty() const { return gptr() == egptr(); }
    void require_empty() { ExcAssert(buf_empty()); }
    void buf_fill();
    virtual ssize_t source_read(char * s, size_t n) = 0;
    template<typename Source, typename... Args> ssize_t do_source_read(char * s, size_t n, Source & source, Args&&... args)
    {
        if (eof_) return EOF;
        auto res = source.read(std::forward<Args>(args)..., s, n);
        if (res == 0 || res == EOF) eof_ = true;
        pos_ += (res == EOF ? 0 : res);
        return res;
    }
    size_t current_pos() const { return pos_ - (egptr() - gptr()); };
    virtual std::streampos seek(std::streamoff off, std::ios_base::seekdir way)
    { 
        if (way != std::ios_base::cur || off != 0) return EOF;
        return current_pos();
    }
};

template<typename Source>
struct source_istreambuf: public buffered_istreambuf {
    using base_type = buffered_istreambuf;
    source_istreambuf(Source source, size_t buffer_size = 4096) : base_type(buffer_size), source_(std::move(source)) {}
    virtual ~source_istreambuf() = default;
    const Source & source() const { return source_; }
    Source & source() { return source_; }
    virtual void close() override { close_stream(source_); }
protected:
    virtual ssize_t source_read(char * s, size_t n) override { return do_source_read(s, n, source_); }
    virtual std::streampos seek(std::streamoff off, std::ios_base::seekdir way) override
    {
        if (way == std::ios_base::cur && off == 0) return current_pos();

        // If the source supports seeking, then we can seek to the new position.
        // Since the source doesn't know about buffering at all, we need to calculate
        // a different offset to seek to, and then perform the actual seek.
        if constexpr (has_seek_v<Source>) {
            auto curr_pos = current_pos();
            if (way == std::ios_base::cur) off -= egptr() - gptr();
            std::streampos new_pos = source_.seek(off, way);
            if (curr_pos != new_pos && !eof()) {
                setg(nullptr, nullptr, nullptr); // we no longer have a buffer
                buf_fill();
            }
            return new_pos;
        }
        return base_type::seek(off, way);
    }
    Source source_;
};

template<typename Filter>
struct filtering_istreambuf: public buffered_istreambuf {
    using base_type = buffered_istreambuf;
    filtering_istreambuf(Filter filter, size_t buffer_size = 4096) : base_type(buffer_size), filter_(std::move(filter)) {}
    virtual ~filtering_istreambuf() = default;
    const Filter & filter() const { return filter_; }
    Filter & filter() { return filter_; }
    virtual void close() override { close_stream(filter_); }
protected:
    virtual ssize_t source_read(char * s, size_t n) override { return do_source_read(s, n, filter_, *this); }
    Filter filter_;
};

struct filtering_istream: public filtering_stream<std::istream> {
    using filtering_stream::push;
    template<typename Source> void push(Source source, size_t buffer_size = 4096, typename Source::is_source::type = {}) { push_cap<source_istreambuf<Source>>(std::move(source), buffer_size); }
    template<typename Filter> void push(Filter filter, size_t buffer_size = 4096, typename Filter::is_filter::type = {}) { push_filter<filtering_istreambuf<Filter>>(std::move(filter), buffer_size); }
};


/*****************************************************************************/
/* NULL FILTER                                                               */
/*****************************************************************************/

// Filter which passes its data straight through; mostly for testing

struct null_filter {
    using is_filter = std::true_type;
    template<typename Sink> void flush(Sink & sink) { flush_stream(sink); }
    template<typename SourceOrSink> void close(SourceOrSink & s) { close_stream(s); }
    template<typename Source> ssize_t read(Source & source, char * s, size_t n) { return source.read(s, n); }
    template<typename Sink> ssize_t write(Sink & sink, const char * s, size_t n) { return sink.write(s, n); }
    template<typename SourceOrSink> std::streampos seek(SourceOrSink & s, std::streamoff off, std::ios_base::seekdir way, std::ios_base::openmode which)
    {
        return s.seek(off, way, which);
    }
};


/*****************************************************************************/
/* MEMORY MAPPING                                                            */
/*****************************************************************************/

struct mapped_file_params {
    std::string filename;
    int fd = -1;
    size_t offset = 0;
    ssize_t size = -1;
    ssize_t extra_capacity = 0;
};

struct mapped_file_source {
    using is_source = std::true_type;
    mapped_file_source(mapped_file_params params);

    const char * data() const { return data_; }
    size_t size() const { return size_; }
    size_t avail() const { return data_ + size_ - ptr_; }
    size_t capacity() const { return capacity_; }
    size_t extra_capacity() const { return capacity_ - size_; }

    ssize_t read(char * s, size_t n);
    std::streampos seek(std::streamoff off, std::ios_base::seekdir way);

    int fd_ = -1;
    std::shared_ptr<const void> region_;
    const char * data_ = nullptr;
    const char * ptr_ = nullptr;
    size_t size_ = 0;
    size_t capacity_ = 0;
};


/*****************************************************************************/
/* FILE DESCRIPTOR                                                           */
/*****************************************************************************/

enum close_handle {
    never_close_handle = false,
    always_close_handle = true
};

struct file_descriptor_sink {
    using is_sink = std::true_type;
    file_descriptor_sink(int fd, close_handle close_handle = never_close_handle);
    file_descriptor_sink(file_descriptor_sink && other);

    void swap(file_descriptor_sink & other);
    file_descriptor_sink & operator = (file_descriptor_sink && other)
    {
        swap(other);
        return *this;
    }

    ~file_descriptor_sink();

    void close();
    void flush();

    ssize_t write(const char * data, size_t len);

protected:
    int fd_ = -1;
    close_handle close_handle_ = never_close_handle;
};

} // namespace MLDB

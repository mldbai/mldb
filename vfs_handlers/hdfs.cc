// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* hdfs.cc
   Wolfgang Sourdeau, March 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.
*/

#include <memory>
#include <string>
#include <boost/iostreams/filtering_stream.hpp>

#include "hdfs/hdfs.h"

#include "googleurl/src/url_util.h"
#include "mldb/arch/exception.h"
#include "mldb/base/exc_assert.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/types/url.h"

using namespace std;
using namespace MLDB;

constexpr int DEFAULT_PORT(50070);

namespace {

/****************************************************************************/
/* HDFS SOURCE IMPL                                                         */
/****************************************************************************/

struct HDFSSourceImpl {
    HDFSSourceImpl(const string & urlStr, int mode);
    ~HDFSSourceImpl();

    streamsize read(char * s, streamsize n);
    streamsize write(const char* s, streamsize n);

private:
    void cleanup() noexcept;

    int mode_;
    hdfsFS fsHandle_;
    hdfsFile fileHandle_;
};

HDFSSourceImpl::
HDFSSourceImpl(const string & urlStr, int mode)
    : mode_(mode)
{
    Url url(urlStr);

    string hostname = url.host();
    int port = url.port();
    if (port == -1) {
        port = DEFAULT_PORT;
    }
    string username = url.username();
    string filename = url.path();

    ExcAssertEqual(url.scheme(), "hdfs");
    ExcAssert(!filename.empty());

    hdfsBuilder * builder = hdfsNewBuilder();
    string baseUrl = "hdfs://" + hostname + ":" + to_string(port);
    hdfsBuilderSetNameNode(builder, baseUrl.c_str());

    if (!username.empty()) {
        hdfsBuilderSetUserName(builder, username.c_str());
    }

    bool ok(false);
    ML::Call_Guard guard([&] { if (!ok) cleanup(); });

    fsHandle_ = hdfsBuilderConnect(builder);
    ExcAssert(fsHandle_ != nullptr);

    /* "hdfsExists" returns the opposite of what it means */
    if ((mode & O_WRONLY) == 0 && hdfsExists(fsHandle_, filename.c_str())) {
        throw MLDB::Exception("file does not exist");
    }

    fileHandle_ = hdfsOpenFile(fsHandle_, filename.c_str(), mode, 0,
                               0, 0);
    ExcAssert(fileHandle_ != nullptr);

    ok = true;
}

HDFSSourceImpl::
~HDFSSourceImpl()
{
    cleanup();
}

streamsize
HDFSSourceImpl::
read(char * s, streamsize n)
{
    ExcAssert((mode_ & O_RDONLY) == O_RDONLY);

    tSize readRes;
    {
        MLDB_TRACE_EXCEPTIONS(false);
        readRes = hdfsRead(fsHandle_, fileHandle_, s, n);
    }
    if (readRes == -1) {
        throw MLDB::Exception(errno, "hdfsRead");
    }

    return readRes > 0 ? readRes : -1;
}

streamsize
HDFSSourceImpl::
write(const char* s, streamsize n)
{
    ExcAssert((mode_ & O_WRONLY) == O_WRONLY);

    tSize writeRes = hdfsWrite(fsHandle_, fileHandle_, s, n);
    if (writeRes == -1) {
        throw MLDB::Exception(errno, "hdfsWrite");
    }

    return writeRes;
}

void
HDFSSourceImpl::
cleanup()
    noexcept
{
    if (fileHandle_) {
        hdfsCloseFile(fsHandle_, fileHandle_);
        fileHandle_ = nullptr;
    }
    if (fsHandle_) {
        hdfsDisconnect(fsHandle_);
        fsHandle_ = nullptr;
    }
}


/****************************************************************************/
/* HDFS DL SOURCE                                                           */
/****************************************************************************/

/* Source for filter_istream */
struct HDFSDlSource {
    typedef char char_type;
    struct category
        : //input_seekable,
        boost::iostreams::input,
        boost::iostreams::device_tag,
        boost::iostreams::closable_tag
    { };

    HDFSDlSource(const string & urlStr)
    {
        impl_.reset(new HDFSSourceImpl(urlStr, O_RDONLY));
    }

    streamsize read(char* s, streamsize n)
    {
        return impl_->read(s, n);
    }

    bool is_open() const
    {
        return !!impl_;
    }

    void close()
    {
        impl_.reset();
    }

private:
    shared_ptr<HDFSSourceImpl> impl_;
};


/****************************************************************************/
/* HDFS UL SOURCE                                                           */
/****************************************************************************/

/* Source for filter_ostream */

struct HDFSUlSource {
    typedef char char_type;
    struct category
        : public boost::iostreams::output,
          public boost::iostreams::device_tag,
          public boost::iostreams::closable_tag
    {
    };

    HDFSUlSource(const string & urlStr,
                 const ML::OnUriHandlerException & onException)
        : onException_(onException)
    {
        try {
            impl_.reset(new HDFSSourceImpl(urlStr, O_WRONLY));
        }
        catch (...) {
            if (onException_) {
                onException_();
            }
            throw;
        }
    }

    streamsize write(const char * s, streamsize n)
    {
        try {
            return impl_->write(s, n);
        }
        catch (...) {
            if (onException_) {
                onException_();
            }
            throw;
        }
    }

    bool is_open() const
    {
        return !!impl_;
    }

    void close()
    {
        impl_.reset();
    }

private:
    shared_ptr<HDFSSourceImpl> impl_;
    ML::OnUriHandlerException onException_;
};

/** Register HDFS with the filter streams API.
*/
struct RegisterHDFSHandler {
    static pair<streambuf *, bool>
    getHDFSHandler(const string & scheme,
                   const string & resource,
                   ios_base::open_mode mode,
                   const map<string, string> & options,
                   const ML::OnUriHandlerException & onException)
    {
        streambuf * result;

        string::size_type pos = scheme.find("hdfs://");
        if (pos != string::npos)
            throw MLDB::Exception("malformed hdfs url");
        string url = "hdfs://" + resource;

        if (mode == ios::in) {
            result = new boost::iostreams::stream_buffer<HDFSDlSource>(HDFSDlSource(url), 131072);
        }
        else if (mode == ios::out) {
             result =
                 new boost::iostreams::stream_buffer<HDFSUlSource>(HDFSUlSource(url, onException),
                                                                   131072);
        }
        else {
            throw MLDB::Exception("no way to create HDFS handler for non in/out");
        }
        return make_pair(result, true);
    }

    RegisterHDFSHandler()
    {
        /* this enables googleuri to parse our urls properly */
        url_util::AddStandardScheme("hdfs");
        ML::registerUriHandler("hdfs", getHDFSHandler);
    }

} registerHDFSHandler;

}

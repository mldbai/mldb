/** http_client_impl_v1.cc
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.

    V1 of the HTTP client, based on libcurl.
*/

#include <errno.h>
#include <poll.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>

#include "mldb/arch/exception.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/jml/utils/string_functions.h"

#include "mldb/http/http_header.h"
// #include "openssl_threading.h"
#include "mldb/http/http_client_callbacks.h"
#include "mldb/http/http_client_impl_v1.h"

using namespace std;
using namespace MLDB;


namespace {

HttpClientError
translateError(CURLcode curlError)
{
    HttpClientError error;

    switch (curlError) {
    case CURLE_OK:
        error = HttpClientError::None;
        break;
    case CURLE_OPERATION_TIMEDOUT:
        error = HttpClientError::Timeout;
        break;
    case CURLE_COULDNT_RESOLVE_HOST:
        error = HttpClientError::HostNotFound;
        break;
    case CURLE_COULDNT_CONNECT:
        error = HttpClientError::CouldNotConnect;
        break;
    case CURLE_SEND_ERROR:
        error = HttpClientError::SendError;
        break;
    case CURLE_RECV_ERROR:
        error = HttpClientError::RecvError;
        break;
    default:
        cerr << "returning 'unknown' for code " + to_string(curlError) + "\n";
        error = HttpClientError::Unknown;
    }

    return error;
}

} // file scope


/****************************************************************************/
/* HTTP CLIENT IMPL V1                                                      */
/****************************************************************************/

HttpClientImplV1::
HttpClientImplV1(const string & baseUrl, int numParallel, int queueSize)
    : HttpClientImpl(baseUrl, numParallel, queueSize),
      baseUrl_(baseUrl),
      fd_(-1),
      wakeup_(EFD_NONBLOCK | EFD_CLOEXEC),
      timerFd_(-1),
      multi_(curl_multi_init()),
      connectionStash_(numParallel),
      avlConnections_(numParallel),
      nextAvail_(0)
{
    if (queueSize > 0) {
        throw MLDB::Exception("'queueSize' semantics not implemented");
    }

    bool success(false);
    ML::Call_Guard guard([&] () {
        if (!success) { cleanupFds(); }
    });

    fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (fd_ == -1) {
        throw MLDB::Exception(errno, "epoll_create");
    }

    addFd(wakeup_.fd(), false, EPOLLIN);

    timerFd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    addFd(timerFd_, false, EPOLLIN);

    /* multi */
    ::curl_multi_setopt(multi_.get(), CURLMOPT_SOCKETFUNCTION,
                        socketCallback);
    ::curl_multi_setopt(multi_.get(), CURLMOPT_SOCKETDATA, this);
    ::curl_multi_setopt(multi_.get(), CURLMOPT_TIMERFUNCTION,
                        timerCallback);
    ::curl_multi_setopt(multi_.get(), CURLMOPT_TIMERDATA, this);

    /* available connections */
    for (size_t i = 0; i < connectionStash_.size(); i++) {
        avlConnections_[i] = &connectionStash_[i];
    }

    /* kick start multi */
    int runningHandles;
    CURLMcode rc = ::curl_multi_socket_action(multi_.get(),
                                              CURL_SOCKET_TIMEOUT, 0,
                                              &runningHandles);
    if (rc != ::CURLM_OK) {
        throw MLDB::Exception("curl error " + to_string(rc));
    }

    success = true;
}

void
HttpClientImplV1::
CurlMultiCleanup::
operator () (::CURLM * c)
{
    curl_multi_cleanup(c);
}

HttpClientImplV1::
~HttpClientImplV1()
{
    cleanupFds();
}

void
HttpClientImplV1::
enableDebug(bool value)
{
    debug(value);
}

void
HttpClientImplV1::
enableSSLChecks(bool value)
{
    noSSLChecks_ = !value;
}

void
HttpClientImplV1::
enableTcpNoDelay(bool value)
{
    tcpNoDelay_ = value;
}

void
HttpClientImplV1::
enablePipelining(bool value)
{
    ::curl_multi_setopt(multi_.get(), CURLMOPT_PIPELINING, value ? 1 : 0);
}

void
HttpClientImplV1::
addFd(int fd, bool isMod, int flags)
    const
{
    ::epoll_event event;

    ::memset(&event, 0, sizeof(event));

    event.events = flags;
    event.data.fd = fd;
    int rc = ::epoll_ctl(fd_, isMod ? EPOLL_CTL_MOD : EPOLL_CTL_ADD,
                         fd, &event);
    if (rc == -1) {
        rc = ::epoll_ctl(fd_, isMod ? EPOLL_CTL_ADD : EPOLL_CTL_MOD,
                         fd, &event);
    }
    if (rc == -1) {
	if (errno != EBADF) {
            throw MLDB::Exception(errno, "epoll_ctl");
        }
    }
}

void
HttpClientImplV1::
removeFd(int fd)
    const
{
    ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr);
}

bool
HttpClientImplV1::
enqueueRequest(const string & verb, const string & resource,
               const shared_ptr<HttpClientCallbacks> & callbacks,
               const HttpRequestContent & content,
               const RestParams & queryParams, const RestParams & headers,
               int timeout)
{
    string url = baseUrl_ + resource + queryParams.uriEscaped();
    {
        Guard guard(queueLock_);
        queue_.emplace(std::make_shared<HttpRequest>(verb, url, callbacks, content, headers, timeout));
    }
    wakeup_.signal();

    return true;
}

std::vector<std::shared_ptr<HttpRequest>>
HttpClientImplV1::
popRequests(size_t number)
{
    Guard guard(queueLock_);
    std::vector<std::shared_ptr<HttpRequest>> requests;
    number = min(number, queue_.size());
    requests.reserve(number);

    for (size_t i = 0; i < number; i++) {
        requests.emplace_back(move(queue_.front()));
        queue_.pop();
    }

    return requests;
}

size_t
HttpClientImplV1::
queuedRequests()
    const
{
    Guard guard(queueLock_);
    return queue_.size();
}

void
HttpClientImplV1::
cleanupFds()
    noexcept
{
    if (timerFd_ != -1) {
        ::close(timerFd_);
    }
    if (fd_ != -1) {
        ::close(fd_);
    }
}

int
HttpClientImplV1::
selectFd()
    const
{
    return fd_;
}

bool
HttpClientImplV1::
processOne()
{
    static const int nEvents(1024);
    ::epoll_event events[nEvents];

    while (true) {
        int res = ::epoll_wait(fd_, events, nEvents, 0);
        if (res > 0) {
            for (int i = 0; i < res; i++) {
                handleEvent(events[i]);
            }
        }
        else if (res == 0) {
            break;
        }
        else if (res == -1) {
            if (errno == EINTR) {
                continue;
            }
            else {
                throw MLDB::Exception(errno, "epoll_wait");
            }
        }
    }

    return false;
}

void
HttpClientImplV1::
handleEvent(const ::epoll_event & event)
{
    if (event.data.fd == wakeup_.fd()) {
        handleWakeupEvent();
    }
    else if (event.data.fd == timerFd_) {
        handleTimerEvent();
    }
    else {
        handleMultiEvent(event);
    }
}

void
HttpClientImplV1::
handleWakeupEvent()
{
    /* Deduplication of wakeup events */
    while (wakeup_.tryRead());

    size_t numAvail = avlConnections_.size() - nextAvail_;
    if (numAvail > 0) {
        vector<shared_ptr<HttpRequest>> requests = popRequests(numAvail);
        for (auto & request: requests) {
            HttpConnection *conn = getConnection();
            conn->request_ = move(request);
            conn->perform(noSSLChecks_, tcpNoDelay_, debug_);

            CURLMcode code = ::curl_multi_add_handle(multi_.get(),
                                                     conn->easy_);
            if (code != CURLM_CALL_MULTI_PERFORM && code != CURLM_OK) {
                throw MLDB::Exception("failing to add handle to multi");
            }
        }
    }
}

void
HttpClientImplV1::
handleTimerEvent()
{
    uint64_t misses;
    ssize_t len = ::read(timerFd_, &misses, sizeof(misses));
    if (len == -1) {
        if (errno != EAGAIN) {
            throw MLDB::Exception(errno, "read timerd");
        }
    }
    int runningHandles;
    CURLMcode rc = ::curl_multi_socket_action(multi_.get(),
                                              CURL_SOCKET_TIMEOUT, 0,
                                              &runningHandles);
    if (rc != ::CURLM_OK) {
        throw MLDB::Exception("curl error " + to_string(rc));
    }
    checkMultiInfos();
}

void
HttpClientImplV1::
handleMultiEvent(const ::epoll_event & event)
{
    int actionFlags(0);
    if ((event.events & EPOLLIN) != 0) {
        actionFlags |= CURL_CSELECT_IN;
    }
    if ((event.events & EPOLLOUT) != 0) {
        actionFlags |= CURL_CSELECT_OUT;
    }
    
    int runningHandles;
    CURLMcode rc = ::curl_multi_socket_action(multi_.get(), event.data.fd,
                                              actionFlags,
                                              &runningHandles);
    if (rc != ::CURLM_OK) {
        throw MLDB::Exception("curl error " + to_string(rc));
    }

    checkMultiInfos();
}

void
HttpClientImplV1::
checkMultiInfos()
{
    int remainingMsgs(0);
    CURLMsg * msg;
    while ((msg = ::curl_multi_info_read(multi_.get(), &remainingMsgs))) {
        if (msg->msg == CURLMSG_DONE) {
            HttpConnection * conn(nullptr);
            ::curl_easy_getinfo(msg->easy_handle,
                                CURLINFO_PRIVATE, &conn);

            const auto & cbs = conn->request_->callbacks();
            cbs->onDone(*conn->request_, translateError(msg->data.result));
            conn->clear();

            CURLMcode code = ::curl_multi_remove_handle(multi_.get(),
                                                        conn->easy_);
            if (code != CURLM_CALL_MULTI_PERFORM && code != CURLM_OK) {
                throw MLDB::Exception("failed to remove handle to multi");
            }
            releaseConnection(conn);
            wakeup_.signal();
        }
    }
}

int
HttpClientImplV1::
socketCallback(CURL *e, curl_socket_t s, int what, void *clientP, void *sockp)
{
    HttpClientImplV1 * this_ = static_cast<HttpClientImplV1 *>(clientP);

    return this_->onCurlSocketEvent(e, s, what, sockp);
}

int
HttpClientImplV1::
onCurlSocketEvent(CURL *e, curl_socket_t fd, int what, void *sockp)
{
    // cerr << "onCurlSocketEvent: " + to_string(fd) + " what: " + to_string(what) + "\n";

    if (what == CURL_POLL_REMOVE) {
        removeFd(fd);
    }
    else if (what != CURL_POLL_NONE) {
        int flags(0);
        if ((what & CURL_POLL_IN)) {
            flags |= EPOLLIN;
        }
        if ((what & CURL_POLL_OUT)) {
            flags |= EPOLLOUT;
        }
        addFd(fd, (sockp != nullptr), flags);
        if (sockp == nullptr) {
            CURLMcode rc = ::curl_multi_assign(multi_.get(), fd, this);
            if (rc != ::CURLM_OK) {
                throw MLDB::Exception("curl error " + to_string(rc));
            }
        }
    }

    return 0;
}

int
HttpClientImplV1::
timerCallback(CURLM *multi, long timeoutMs, void *clientP)
{
    HttpClientImplV1 * this_ = static_cast<HttpClientImplV1 *>(clientP);

    return this_->onCurlTimerEvent(timeoutMs);
}

int
HttpClientImplV1::
onCurlTimerEvent(long timeoutMs)
{
    // cerr << "onCurlTimerEvent: timeout = " + to_string(timeoutMs) + "\n";

    if (timeoutMs < -1) {
        throw MLDB::Exception("unhandled timeout value: %ld", timeoutMs);
    }

    struct itimerspec timespec;
    memset(&timespec, 0, sizeof(timespec));
    if (timeoutMs > 0) {
        timespec.it_value.tv_sec = timeoutMs / 1000;
        timespec.it_value.tv_nsec = (timeoutMs % 1000) * 1000000;
    }
    int res = ::timerfd_settime(timerFd_, 0, &timespec, nullptr);
    if (res == -1) {
        throw MLDB::Exception(errno, "timerfd_settime");
    }

    if (timeoutMs == 0) {
        int runningHandles;
        CURLMcode rc = ::curl_multi_socket_action(multi_.get(),
                                                  CURL_SOCKET_TIMEOUT, 0,
                                                  &runningHandles);
        if (rc != ::CURLM_OK) {
            throw MLDB::Exception("curl error " + to_string(rc));
        }
        checkMultiInfos();
    }

    return 0;
}

HttpClientImplV1::
HttpConnection *
HttpClientImplV1::
getConnection()
{
    HttpConnection * conn;

    if (nextAvail_ < avlConnections_.size()) {
        conn = avlConnections_[nextAvail_];
        nextAvail_++;
    }
    else {
        conn = nullptr;
    }

    return conn;
}

void
HttpClientImplV1::
releaseConnection(HttpConnection * oldConnection)
{
    if (nextAvail_ > 0) {
        nextAvail_--;
        avlConnections_[nextAvail_] = oldConnection;
    }
}


/* HTTPCLIENT::HTTPCONNECTION */

HttpClientImplV1::
HttpConnection::
HttpConnection()
    : onHeader_([&] (const char * data, size_t ofs1, size_t ofs2) {
          return this->onCurlHeader(data, ofs1 * ofs2);
      }),
      onWrite_([&] (const char * data, size_t ofs1, size_t ofs2) {
          return this->onCurlWrite(data, ofs1 * ofs2);
      }),
      onRead_([&] (char * data, size_t ofs1, size_t ofs2) {
          return this->onCurlRead(data, ofs1 * ofs2);
      }),
      afterContinue_(false), uploadOffset_(0)
{
}

void
HttpClientImplV1::
HttpConnection::
perform(bool noSSLChecks, bool tcpNoDelay, bool debug)
{
    // cerr << "* performRequest\n";

    afterContinue_ = false;

    easy_.add_option(CURLOPT_URL, request_->url());

    RestParams headers = request_->headers();

    const string & verb = request_->verb();
    if (verb != "GET") {
        const auto & content = request_->content();
        const string & body = content.body();
        if (verb == "PUT") {
            easy_.add_option(CURLOPT_UPLOAD, true);
            easy_.add_option(CURLOPT_INFILESIZE, body.size());
        }
        else if (verb == "POST") {
            easy_.add_option(CURLOPT_POST, true);
            easy_.add_option(CURLOPT_POSTFIELDS, body);
            easy_.add_option(CURLOPT_POSTFIELDSIZE, body.size());
        }
        else if (verb == "HEAD") {
            easy_.add_option(CURLOPT_NOBODY, true);
        }
        headers.emplace_back(make_pair("Content-Length",
                                       to_string(body.size())));
        headers.emplace_back(make_pair("Transfer-Encoding", ""));
        headers.emplace_back(make_pair("Content-Type",
                                       content.contentType()));

        /* Disable "Expect: 100 Continue" header that curl sets automatically
           for uploads larger than 1 Kbyte */
        headers.emplace_back(make_pair("Expect", ""));
    }
    easy_.add_header_option(headers);

    easy_.add_option(CURLOPT_CUSTOMREQUEST, verb);
    easy_.add_data_option(CURLOPT_PRIVATE, this);

    easy_.add_callback_option(CURLOPT_HEADERFUNCTION, CURLOPT_HEADERDATA, onHeader_);
    easy_.add_callback_option(CURLOPT_WRITEFUNCTION, CURLOPT_WRITEDATA, onWrite_);
    easy_.add_callback_option(CURLOPT_READFUNCTION, CURLOPT_READDATA,  onRead_);
    
    easy_.add_option(CURLOPT_BUFFERSIZE, 65536);

    if (request_->timeout() != -1) {
        easy_.add_option(CURLOPT_TIMEOUT, request_->timeout());
    }
    easy_.add_option(CURLOPT_NOSIGNAL, true);
    easy_.add_option(CURLOPT_NOPROGRESS, true);
    if (noSSLChecks) {
        easy_.add_option(CURLOPT_SSL_VERIFYHOST, false);
        easy_.add_option(CURLOPT_SSL_VERIFYPEER, false);
    }
    if (debug) {
        easy_.add_option(CURLOPT_VERBOSE, 1L);
    }
    if (tcpNoDelay) {
        easy_.add_option(CURLOPT_TCP_NODELAY, true);
    }
}

size_t
HttpClientImplV1::
HttpConnection::
onCurlHeader(const char * data, size_t size)
{
    string headerLine(data, size);
    if (headerLine.find("HTTP/1.1 100") == 0) {
        afterContinue_ = true;
    }
    else if (afterContinue_) {
        if (headerLine == "\r\n")
            afterContinue_ = false;
    }
    else {
        const auto & cbs = request_->callbacks();
        if (headerLine.find("HTTP/") == 0) {
            size_t lineSize = headerLine.size();
            size_t oldTokenIdx(0);
            size_t tokenIdx = headerLine.find(" ");
            if (tokenIdx == string::npos || tokenIdx >= lineSize) {
                throw MLDB::Exception("malformed header");
            }
            string version = headerLine.substr(oldTokenIdx, tokenIdx);

            oldTokenIdx = tokenIdx + 1;
            tokenIdx = headerLine.find(" ", oldTokenIdx);
            if (tokenIdx == string::npos || tokenIdx >= lineSize) {
                throw MLDB::Exception("malformed header");
            }
            int code = stoi(headerLine.substr(oldTokenIdx, tokenIdx));

            cbs->onResponseStart(*request_, move(version), code);
        }
        else {
            cbs->onHeader(*request_, data, size);
        }
    }

    return size;
}

size_t
HttpClientImplV1::
HttpConnection::
onCurlWrite(const char * data, size_t size)
{
    request_->callbacks()->onData(*request_, data, size);
    return size;
}

size_t
HttpClientImplV1::
HttpConnection::
onCurlRead(char * buffer, size_t bufferSize)
{
    const string & body = request_->content().body();
    size_t chunkSize = body.size() - uploadOffset_;
    if (chunkSize > bufferSize) {
        chunkSize = bufferSize;
    }
    const char * chunkStart = body.c_str() + uploadOffset_;
    copy(chunkStart, chunkStart + chunkSize, buffer);
    uploadOffset_ += chunkSize;

    return chunkSize;
}

/** http_client_impl_v1.h                                          -*- C++ -*-
    Wolfgang Sourdeau, January 2014
    This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.

    V1 of the HTTP client, based on libcurl:
    - has support for https
    - slow
*/

#pragma once

#include "sys/epoll.h"

#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include "mldb/arch/wakeup_fd.h"
#include "mldb/http/curl_wrapper.h"
#include "mldb/http/http_client.h"
#include "mldb/http/http_client_impl.h"
#include "mldb/http/http_header.h"


namespace MLDB {

/****************************************************************************/
/* HTTP CLIENT IMPL V1                                                      */
/****************************************************************************/

struct HttpClientImplV1 : public HttpClientImpl {
    HttpClientImplV1(const std::string & baseUrl,
                     int numParallel, int queueSize);
    ~HttpClientImplV1();

    /* AsyncEventSource */
    virtual int selectFd() const override;
    virtual bool processOne() override;

    /* HttpClientImpl */
    void enableDebug(bool value) override;
    void enableSSLChecks(bool value) override;
    void enableTcpNoDelay(bool value) override;
    void enablePipelining(bool value) override;

    bool enqueueRequest(const std::string & verb,
                        const std::string & resource,
                        const std::shared_ptr<HttpClientCallbacks> & callbacks,
                        const HttpRequestContent & content,
                        const RestParams & queryParams,
                        const RestParams & headers,
                        int timeout = -1) override;

    size_t queuedRequests() const override;

private:
    void cleanupFds() noexcept;

    /* Local */
    std::vector<std::shared_ptr<HttpRequest>> popRequests(size_t number);

    void handleEvents();
    void handleEvent(const ::epoll_event & event);
    void handleWakeupEvent();
    void handleTimerEvent();
    void handleMultiEvent(const ::epoll_event & event);

    void checkMultiInfos();

    static int socketCallback(CURL * e, curl_socket_t s, int what,
                              void * clientP, void * sockp);
    int onCurlSocketEvent(CURL * e, curl_socket_t s, int what, void * sockp);

    static int timerCallback(CURLM * multi, long timeoutMs, void * clientP);
    int onCurlTimerEvent(long timeout_ms);

    void addFd(int fd, bool isMod, int flags) const;
    void removeFd(int fd) const;

    struct HttpConnection {
        HttpConnection();

        HttpConnection(const HttpConnection & other) = delete;

        void clear()
        {
            easy_.reset();
            request_.reset();
            afterContinue_ = false;
            uploadOffset_ = 0;
        }
        void perform(bool noSSLChecks, bool tcpNoDelay, bool debug);

        /* header and body write callbacks */
        CurlWrapper::Easy::CurlCallback onHeader_;
        CurlWrapper::Easy::CurlCallback onWrite_;
        size_t onCurlHeader(const char * data, size_t size);
        size_t onCurlWrite(const char * data, size_t size);

        /* body read callback */
        CurlWrapper::Easy::CurlCallback onRead_;
        size_t onCurlRead(char * buffer, size_t bufferSize);

        std::shared_ptr<HttpRequest> request_;

        CurlWrapper::Easy easy_;
        // HttpClientResponse response_;
        bool afterContinue_;
        size_t uploadOffset_;

        struct HttpConnection *next;
    };

    HttpConnection * getConnection();
    void releaseConnection(HttpConnection * connection);

    std::string baseUrl_;
    bool expect100Continue_;
    bool tcpNoDelay_;
    bool noSSLChecks_;

    int fd_;
    ML::Wakeup_Fd wakeup_;
    int timerFd_;

    struct CurlMultiCleanup {
        void operator () (CURLM *);
    };

    std::unique_ptr<CURLM, CurlMultiCleanup> multi_;

    std::vector<HttpConnection> connectionStash_;
    std::vector<HttpConnection *> avlConnections_;
    size_t nextAvail_;

    typedef std::mutex Mutex;
    typedef std::unique_lock<Mutex> Guard;
    mutable Mutex queueLock_;
    std::queue<std::shared_ptr<HttpRequest>> queue_; /* queued requests */
};

} // namespace MLDB

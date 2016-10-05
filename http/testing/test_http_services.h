#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "mldb/io/tcp_acceptor.h"
#include "mldb/http/http_socket_handler.h"


namespace MLDB {

struct HttpHeader;
struct TestHttpSocketHandler;

struct TestHttpService
{
    TestHttpService(EventLoop & eventLoop);
    virtual ~TestHttpService();

    std::string start(const std::string & address = "127.0.0.1");

    virtual void handleHttpPayload(TestHttpSocketHandler & handler,
                                   const HttpHeader & header,
                                   const std::string & payload) = 0;

    int portToUse;
    std::atomic<size_t> numReqs;

private:
    std::shared_ptr<TcpSocketHandler> onNewConnection(TcpSocket && socket);

    TcpAcceptor::OnNewConnection onNewConnection_;
    TcpAcceptor acceptor_;
};

struct TestHttpSocketHandler : public HttpLegacySocketHandler
{
    TestHttpSocketHandler(TcpSocket && socket, TestHttpService & service);

    void sendResponse(int code,
                      const std::string & body, const std::string & type);

private:
    virtual void handleHttpPayload(const HttpHeader & header,
                                   const std::string & payload);

    TestHttpService & service_;
};

struct TestHttpGetService : public TestHttpService
{
    TestHttpGetService(EventLoop & eventLoop);

    struct TestResponse {
        TestResponse(int code = 0, const std::string & body = "")
            : code_(code), body_(body)
        {}

        int code_;
        std::string body_;
    };

    void handleHttpPayload(TestHttpSocketHandler & handler,
                           const HttpHeader & header,
                           const std::string & payload);
    void addResponse(const std::string & verb, const std::string & resource,
                     int code, const std::string & body);

    std::map<std::string, TestResponse> responses_;
};

struct TestHttpUploadService : public TestHttpService
{
    TestHttpUploadService(EventLoop & eventLoop);

    void handleHttpPayload(TestHttpSocketHandler & handler,
                           const HttpHeader & header,
                           const std::string & payload);
};

} // namespace MLDB

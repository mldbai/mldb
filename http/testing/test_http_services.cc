#include <chrono>
#include <string>
#include <thread>

#include "mldb/base/exc_assert.h"
#include "mldb/io/port_range_service.h"
#include "mldb/http/http_header.h"

#include "test_http_services.h"

using namespace std;
using namespace MLDB;


/****************************************************************************/
/* TEST HTTP SERVICE                                                        */
/****************************************************************************/

TestHttpService::
TestHttpService(EventLoop & eventLoop)
    : portToUse(0), numReqs(0),
      onNewConnection_(
          [&] (TcpSocket && socket)
          { return this->onNewConnection(std::move(socket)); }
      ),
      acceptor_(eventLoop, onNewConnection_)
{
}

TestHttpService::
~TestHttpService()
{
}

std::shared_ptr<TcpSocketHandler>
TestHttpService::
onNewConnection(TcpSocket && socket)
{
    return make_shared<TestHttpSocketHandler>(std::move(socket), *this);
}

string
TestHttpService::
start(const string & address)
{
    acceptor_.listen(portToUse, address);
    int effectivePort = acceptor_.effectiveTCPv4Port();
    ExcAssertGreater(effectivePort, 0);
    return "http://" + address + ":" + to_string(effectivePort);
}


/****************************************************************************/
/* TEST HTTP SOCKET HANDLER                                                 */
/****************************************************************************/

TestHttpSocketHandler::
TestHttpSocketHandler(TcpSocket && socket, TestHttpService & service)
    : HttpLegacySocketHandler(std::move(socket)),
      service_(service)
{
}

void
TestHttpSocketHandler::
handleHttpPayload(const HttpHeader & header, const std::string & payload)
{
    service_.handleHttpPayload(*this, header, payload);
}

void
TestHttpSocketHandler::
sendResponse(int code, const string & body, const string & type)
{
    putResponseOnWire(HttpResponse(code, type, body));
}


/****************************************************************************/
/* TEST HTTP GET SERVICE                                                    */
/****************************************************************************/

TestHttpGetService::
TestHttpGetService(EventLoop & eventLoop)
    : TestHttpService(eventLoop)
{}

void
TestHttpGetService::
handleHttpPayload(TestHttpSocketHandler & handler,
                  const HttpHeader & header,
                  const string & payload)
{
    numReqs++;
    string key = header.verb + ":" + header.resource;
    if (header.resource == "/timeout") {
        std::this_thread::sleep_for(std::chrono::seconds(3));
        handler.sendResponse(200, string("Will time out"), "text/plain");
    }
    else if (header.resource == "/counter") {
        handler.sendResponse(200, to_string(numReqs), "text/plain");
    }
    else if (header.resource == "/headers") {
        string headersBody("{\n");
        bool first(true);
        for (const auto & it: header.headers) {
            if (first) {
                first = false;
            }
            else {
                headersBody += ",\n";
            }
            headersBody += "  \"" + it.first + "\": \"" + it.second + "\"\n";
        }
        headersBody += "}\n";
        handler.sendResponse(200, headersBody, "application/json");
    }
    else if (header.resource == "/query-params") {
        string body = header.queryParams.uriEscaped();
        handler.sendResponse(200, body, "text/plain");
    }
    else if (header.resource == "/connection-close") {
        handler.send("HTTP/1.1 204 No contents\r\nConnection: close\r\n\r\n",
                     TestHttpSocketHandler::NextAction::NEXT_CLOSE);
    }
    else if (header.resource == "/quiet-connection-close") {
        handler.send("HTTP/1.1 204 No contents\r\n\r\n",
                     TestHttpSocketHandler::NextAction::NEXT_CLOSE);
    }
    else if (header.resource == "/abrupt-connection-close") {
        handler.send("",
                     TestHttpSocketHandler::NextAction::NEXT_CLOSE);
    }
    else {
        const auto & it = responses_.find(key);
        if (it == responses_.end()) {
            handler.sendResponse(404, string("Not found"), "text/plain");
        }
        else {
            const TestResponse & resp = it->second;
            handler.sendResponse(resp.code_, resp.body_, "text/plain");
        }
    }
}

void
TestHttpGetService::
addResponse(const string & verb, const string & resource,
            int code, const string & body)
{
    string key = verb + ":" + resource;
    responses_[key] = TestResponse(code, body);
}


/****************************************************************************/
/* TEST HTTP UPLOAD SERVICE                                                 */
/****************************************************************************/

TestHttpUploadService::
TestHttpUploadService(EventLoop & eventLoop)
    : TestHttpService(eventLoop)
{}

void
TestHttpUploadService::
handleHttpPayload(TestHttpSocketHandler & handler,
                  const HttpHeader & header,
                  const string & payload)
{
    Json::Value response;

    string cType = header.contentType;
    if (cType.empty()) {
        cType = header.tryGetHeader("Content-Type");
    }
    response["verb"] = header.verb;
    response["type"] = cType;
    response["payload"] = payload;
    Json::Value & jsonHeaders = response["headers"];
    for (const auto & it: header.headers) {
        jsonHeaders[it.first] = it.second;
    }

    handler.sendResponse(200, response.toString(), "application/json");
}

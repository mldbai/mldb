// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* http_rest_endpoint.cc
   Jeremy Barnes, 11 November 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Named endpoint for http connections.
*/

#include "mldb/base/exc_assert.h"
#include "mldb/vfs/filter_streams.h"
#include <boost/lexical_cast.hpp>
#include "mldb/io/tcp_acceptor.h"
#include "http_rest_endpoint.h"
#include "mldb/utils/log.h"
#include <iomanip>

using namespace std;

namespace MLDB {

/****************************************************************************/
/* HTTP REST ENDPOINT                                                       */
/****************************************************************************/

HttpRestEndpoint::
HttpRestEndpoint(EventLoop & eventLoop, bool enableLogging)
{
    auto makeHandler = [&, enableLogging] (TcpSocket && socket) {
        return make_shared<RestConnectionHandler>(this, std::move(socket), enableLogging);
    };
    acceptor_.reset(new TcpAcceptor(eventLoop, makeHandler));
}

HttpRestEndpoint::
~HttpRestEndpoint()
{
}

void
HttpRestEndpoint::
allowAllOrigins()
{
    extraHeaders.push_back({ "Access-Control-Allow-Origin", "*" });
}

void
HttpRestEndpoint::
init()
{
}

void
HttpRestEndpoint::
shutdown()
{
    acceptor_->shutdown();
}

void
HttpRestEndpoint::
closePeer()
{
    shutdown();
}

std::string
HttpRestEndpoint::
bindTcpAddress(const std::string & address)
{
    using namespace std;
    auto pos = address.find(':');
    if (pos == string::npos) {
        // No port specification; take any port
        return bindTcp(PortRange(12000, 12999), address);
    }
    string hostPart(address, 0, pos);
    string portPart(address, pos + 1);

    if (portPart.empty())
        throw MLDB::Exception("invalid port " + portPart + " in address "
                            + address);

    if (portPart[portPart.size() - 1] == '+') {
        unsigned port = boost::lexical_cast<unsigned>(string(portPart, 0, portPart.size() - 1));
        if(port < 65536) {
            unsigned last = port + 999;
            return bindTcp(PortRange(port, last), hostPart);
        }

        throw MLDB::Exception("invalid port " + to_string(port));
    }

    return bindTcp(boost::lexical_cast<int>(portPart), hostPart);
}

std::string
HttpRestEndpoint::
bindTcpFixed(std::string host, int port)
{
    return bindTcp(port, host);
}

std::string
HttpRestEndpoint::
bindTcp(PortRange const & portRange, std::string host)
{
    // TODO: generalize this...
    if (host == "" || host == "*")
        host = "0.0.0.0";

    acceptor_->listen(portRange, host);
    int port = acceptor_->effectiveTCPv4Port();
    const char * literate_doc_bind_file = getenv("LITERATE_DOC_BIND_FILENAME");
    if (literate_doc_bind_file) {
        Json::Value v;
        v["port"] = port;
        filter_ostream out(literate_doc_bind_file);
        out << v.toString() << endl;
        out.close();
    }

    return "http://" + host + ":" + to_string(port);
}


/*****************************************************************************/
/* HTTP REST ENDPOINT :: REST CONNECTION HANDLER                             */
/*****************************************************************************/

HttpRestEndpoint::RestConnectionHandler::
RestConnectionHandler(HttpRestEndpoint * endpoint, TcpSocket && socket, bool enableLogging)
    : HttpLegacySocketHandler(std::move(socket)), endpoint(endpoint)
{
    if (enableLogging)
        logger = MLDB::getServerLog();
}

void
HttpRestEndpoint::RestConnectionHandler::
handleHttpPayload(const HttpHeader & header, const std::string & payload)
{
    // We don't lock here, since sending the response will take the lock,
    // and whatever called us must know it's a valid connection

    this->httpHeader = header;
    clock_gettime(CLOCK_REALTIME, &timer);

    try {
        auto ptr = acceptor().findHandlerPtr(this);
        endpoint->onRequest(static_pointer_cast<HttpRestEndpoint::RestConnectionHandler>(ptr),
                            header, payload);
    }
    catch(const std::exception& ex) {
        Json::Value response;
        response["error"] =
            "exception processing request "
            + header.verb + " " + header.resource;

        response["exception"] = ex.what();
        sendErrorResponse(400, response);
    }
    catch(...) {
        Json::Value response;
        response["error"] =
            "exception processing request "
            + header.verb + " " + header.resource;

        sendErrorResponse(400, response);
    }
}

void
HttpRestEndpoint::RestConnectionHandler::
sendErrorResponse(int code, std::string error)
{
    Json::Value val;
    val["error"] = error;

    sendErrorResponse(code, val);
}

void
HttpRestEndpoint::RestConnectionHandler::
sendErrorResponse(int code, const Json::Value & error)
{
    std::string encodedError = error.toString();
    
    logRequest(code);
    putResponseOnWire(HttpResponse(code, "application/json",
                                   std::move(encodedError),
                                   endpoint->extraHeaders),
                      nullptr, NEXT_CLOSE);
}

void
HttpRestEndpoint::RestConnectionHandler::
sendResponse(int code,
             const Json::Value & response,
             std::string contentType,
             RestParams headers)
{
    return sendResponse(code,
                        response.toString(), std::move(contentType),
                        std::move(headers));
}

void
HttpRestEndpoint::RestConnectionHandler::
sendResponse(int code,
             std::string body, std::string contentType,
             RestParams headers)
{
    for (auto & h: endpoint->extraHeaders)
        headers.push_back(h);

    logRequest(code);
    putResponseOnWire(HttpResponse(code,
                                   std::move(contentType), std::move(body),
                                   std::move(headers)));
}

void
HttpRestEndpoint::RestConnectionHandler::
sendResponseHeader(int code, std::string contentType, RestParams headers)
{
    auto onSendFinished = [=] {
        // Do nothing once we've finished sending the response, so that
        // the connection isn't closed
    };
    
    for (auto & h: endpoint->extraHeaders)
        headers.push_back(h);


    logRequest(code);
    putResponseOnWire(HttpResponse(code,
                                   std::move(contentType),
                                   std::move(headers)),
                      onSendFinished);
}

void
HttpRestEndpoint::RestConnectionHandler::
sendHttpChunk(std::string chunk,
              NextAction next,
              OnWriteFinished onWriteFinished)
{
    HttpLegacySocketHandler::send(std::move(chunk), next, onWriteFinished);
}

inline void
HttpRestEndpoint::RestConnectionHandler::
logRequest(int code) const
{
    if (logger) {
        timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        double elapsed = (now.tv_sec - timer.tv_sec) * 1000 + (now.tv_nsec - timer.tv_nsec) * 0.000001;
        INFO_MSG(logger) << "\"" << httpHeader.verb << " " 
                       << httpHeader.resource << "\" " << code
                       << " "  << std::setprecision(3)  << elapsed <<  "ms";
    }
}

} // namespace MLDB


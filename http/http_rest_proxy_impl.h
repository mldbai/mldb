/** http_rest_proxy_impl.h                                         -*- C++ -*-
    Jeremy Barnes, 20 February 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Details of the HttpRestProxy connection class.  Split out for internal
    users that want to use HttpRestProxy's connection pool but not its
    request handling logic.
*/

#pragma once

#include "http_rest_proxy.h"
#include "curl_wrapper.h"


namespace MLDB {

struct HttpRestProxy::ConnectionHandler: public CurlWrapper::Easy {
};

struct HttpRestProxy::Connection {
    Connection(ConnectionHandler * conn,
               HttpRestProxy * proxy)
        : conn(conn), proxy(proxy)
    {
    }

    ~Connection() noexcept
    {
        if (!conn)
            return;

        if (proxy)
            proxy->doneConnection(conn);
        else delete conn;
    }

    Connection(Connection && other) noexcept
        : conn(other.conn), proxy(other.proxy)
    {
        other.conn = nullptr;
    }

    Connection & operator = (Connection && other) noexcept
    {
        this->conn = other.conn;
        this->proxy = other.proxy;
        other.conn = 0;
        return *this;
    }

    ConnectionHandler & operator * ()
    {
        ExcAssert(conn);
        return *conn;
    }

    ConnectionHandler * operator -> ()
    {
        ExcAssert(conn);
        return conn;
    }

private:
    friend class HttpRestProxy;
    ConnectionHandler * conn;
    HttpRestProxy * proxy;
};


} // namespace MLDB

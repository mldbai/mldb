// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tcp_socket.h                                                    -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include <memory>


namespace MLDB {

/* Forward declarations */
struct TcpSocketImpl;


/****************************************************************************/
/* TCP SOCKET                                                               */
/****************************************************************************/

struct TcpSocket {
    TcpSocket(std::shared_ptr<TcpSocketImpl> impl);
    ~TcpSocket();

    TcpSocketImpl & impl()
    {
        return *impl_;
    }

private:
    std::shared_ptr<TcpSocketImpl> impl_;
};

} // namespace MLDB

// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* tcp_socket.h                                                    -*- C++ -*-
   Wolfgang Sourdeau, September 2015
   Copyright (c) 2015 Datacratic.  All rights reserved.

*/

#pragma once

#include <memory>


namespace Datacratic {

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

} // namespace Datacratic

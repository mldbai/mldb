/** exception_ptr.h                                                 -*- C++ -*-
   Wolfgang Sourdeau, July 2015
   This file is part of MLDB. Copyright (c) 2015 mldb.ai inc. All rights reserved.

   A class that provides thread-safe handling of exception_ptr.
*/

#pragma once

#include <exception>
#include <mutex>


namespace MLDB {

/****************************************************************************/
/* EXCEPTION PTR HANDLER                                                    */
/****************************************************************************/

struct ExceptionPtrHandler {
    bool hasException();
    void takeException(std::exception_ptr newPtr);
    void takeCurrentException();
    void rethrowIfSet();
    void clear()
    { takeException(nullptr); }

private:
    std::mutex excLock_;
    std::exception_ptr excPtr_;
};

} // namespace MLDB

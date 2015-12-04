// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* info.h                                                          -*- C++ -*-
   Jeremy Barnes, 3 April 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.


   Generic information about the current machine.
*/

#ifndef __utils__info_h__
#define __utils__info_h__

#include <string>
#include "mldb/compiler/compiler.h"
#include "mldb/arch/cpu_info.h"

namespace ML {

/** A compact string giving context about the current program. */

std::string all_info();

/** Return the username of the current user. */

std::string username();

std::string hostname();

std::string fqdn_hostname(std::string const & port);

int userid();

std::string userid_to_username(int userid);

/** Returns the number of file descriptors that the process has open. */
size_t num_open_files();

/** Turn an fd into a filename */
std::string fd_to_filename(int fd);

} // namespace ML

#endif /* __utils__info_h__ */

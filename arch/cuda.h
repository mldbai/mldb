/* cuda.h                                                          -*- C++ -*-
   Jeremy Barnes, 10 March 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

   Implementation of the interface for the CUDA libraries.  Delayed loading
   to allow dlopening if they exist.
*/

#pragma once

namespace ML {

void init_cuda();

} // namespace ML

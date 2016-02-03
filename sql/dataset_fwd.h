/** dataset_fwd.h                                                  -*- C++ -*-
    Jeremy Barnes, 16 February 2014
    Copyright (c) 2014 Datacratic Inc.  All rigths reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#pragma once

namespace Datacratic {

template<typename Int, int Domain> struct IntWrapper;
template<int Domain> struct HashWrapper;

struct Id;

namespace MLDB {

struct MldbServer;

struct SqlExpression;
struct SqlRowExpression;
struct BoundFunction;

typedef HashWrapper<1> RowHash;
typedef Id RowName;

typedef HashWrapper<3> ColumnHash;
typedef Id ColumnName;

struct Dataset;

struct KnownColumn;

} // namespace MLDB
} // namespace Datacratic

/** dataset_fwd.h                                                  -*- C++ -*-
    Jeremy Barnes, 16 February 2014
    Copyright (c) 2014 mldb.ai inc.  All rigths reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

namespace MLDB {

template<typename Int, int Domain> struct IntWrapper;
template<int Domain> struct HashWrapper;

struct MldbServer;

struct SqlExpression;
struct SqlRowExpression;
struct BoundFunction;

typedef HashWrapper<1> RowHash;

struct PathElement;
struct Path;

typedef Path RowPath;

typedef HashWrapper<3> ColumnHash;
typedef Path ColumnPath;

struct Dataset;

struct KnownColumn;

} // namespace MLDB


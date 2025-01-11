/** python_converters.h                                 -*- C++ -*-
    Rémi Attab, 13 Dec 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Python converters for common types.

    \todo Add a helper function to initialize the converters for the various
    types. These should only be initialized once so use std::call_once.

*/

#pragma once

#include "nanobind/nanobind.h"
#include "mldb/types/date.h"
#include <vector>
#include <memory>
#include "mldb/base/exc_check.h"
#include "mldb/ext/jsoncpp/value.h"
#include "mldb/types/string.h"
#include "mldb/sql/cell_value.h"
#include "mldb/types/path.h"
#include "mldb/http/http_header.h"

namespace MLDB {
namespace Python {

Utf8String srepr(nanobind::handle obj);

/******************************************************************************/
/* CONVERTER IMPLEMENTATIONS                                                  */
/******************************************************************************/

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Utf8String * result) noexcept;
nanobind::handle from_cpp(const Utf8String & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Json::Value * result) noexcept;
nanobind::handle from_cpp(const Json::Value & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Date * result) noexcept;
nanobind::handle from_cpp(const Date & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, CellValue * result) noexcept;
nanobind::handle from_cpp(const CellValue & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, PathElement * result) noexcept;
nanobind::handle from_cpp(const PathElement & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, Path * result) noexcept;
nanobind::handle from_cpp(const Path & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

// RestParams need to convert objects to Json strings (not Python strings), so we need a separate
// converter here that's not just the inbuilt vector<pair<string, string>> one.
bool from_python(nanobind::handle src, uint8_t flags, nanobind::detail::cleanup_list *cleanup, RestParams * result) noexcept;
nanobind::handle from_cpp(const RestParams & value, nanobind::rv_policy policy, nanobind::detail::cleanup_list *cleanup);

} // namespace Python
} // namespace MLDB


NAMESPACE_BEGIN(NB_NAMESPACE)
NAMESPACE_BEGIN(detail)

#define MLDB_SPECIALIZE_TYPE_CASTER(T, name) \
    template<> struct type_caster<T> { \
        NB_TYPE_CASTER(T, const_name(name)); \
        bool from_python(handle src, uint8_t flags, cleanup_list *cleanup) noexcept { \
            return MLDB::Python::from_python(src, flags, cleanup, &value); \
        } \
        static handle from_cpp(const T & value, rv_policy policy, cleanup_list * cleanup) { \
            return MLDB::Python::from_cpp(value, policy, cleanup); \
        } \
    }; \

MLDB_SPECIALIZE_TYPE_CASTER(MLDB::Utf8String, "utf8");
MLDB_SPECIALIZE_TYPE_CASTER(Json::Value, "json");
MLDB_SPECIALIZE_TYPE_CASTER(MLDB::Date, "datetime");
MLDB_SPECIALIZE_TYPE_CASTER(MLDB::CellValue, "atom");
MLDB_SPECIALIZE_TYPE_CASTER(MLDB::Path, "path");
MLDB_SPECIALIZE_TYPE_CASTER(MLDB::PathElement, "path_element");
MLDB_SPECIALIZE_TYPE_CASTER(MLDB::RestParams, "rest_params");

NAMESPACE_END(detail)
NAMESPACE_END(NB_NAMESPACE)

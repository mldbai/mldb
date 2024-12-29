/* cell_value_visitor.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/utils/is_visitable.h"
#include "cell_value.h"

namespace MLDB {

struct CellValueEmptyTag {};

template<typename Visitor, typename... ExtraTypes>
auto visit(Visitor && visitor, const CellValue & context) -> typename std::decay_t<Visitor>::return_type;

template<typename Unknown>
struct HandleCellTypeUnknown {
    using return_type = std::invoke_result_t<Unknown, CellValue>;
    Unknown unknown;
    HandleCellTypeUnknown(Unknown&&unknown) : unknown(std::move(unknown)) {}
};

template<typename Result>
struct ExceptionOnUnknownCellValueReturning {
    using return_type = Result;
    Result unknown(const CellValue & val) const
    {
        MLDB_THROW_LOGIC_ERROR("Encountered unknown CellValue type : %s", val.toUtf8String().c_str());
    }
};

template<typename Result>
struct HandleCellTypeUnknown<ExceptionOnUnknownCellValueReturning<Result>>: ExceptionOnUnknownCellValueReturning<Result> {
    template<typename T>
    HandleCellTypeUnknown(T&&arg): ExceptionOnUnknownCellValueReturning<Result>(std::forward<T>(arg)) {}
};

template<typename Unknown, class... Ops>
struct CellValueLambdaVisitor
    : HandleCellTypeUnknown<Unknown>, Ops... {
    using visitor_base = CellValueLambdaVisitor;
    using Ops::operator ()...;
    using HandleCellTypeUnknown<Unknown>::unknown;

    template<typename Arg>
    auto visit(Arg&& arg) -> std::invoke_result_t<CellValueLambdaVisitor, Arg>
    {
        return (*this)(std::forward<Arg>(arg));
    }
};

template<typename Unknown, class... Ts>
CellValueLambdaVisitor(Unknown, Ts...) -> CellValueLambdaVisitor<Unknown, Ts...>;

template<typename Visitor, typename... ExtraTypes>
auto visit(Visitor && visitor, const CellValue & value) -> typename std::decay_t<Visitor>::return_type
{
    using DecayedVisitor = typename std::decay_t<Visitor>;
    using Return = typename DecayedVisitor::return_type;

    switch (value.cellType()) {
    case CellValue::EMPTY:
        if constexpr (isVisitable<DecayedVisitor, Return(CellValueEmptyTag)>()) {
            return visitor.visit(CellValueEmptyTag{});
        }
        break;
    case CellValue::INTEGER:
        if constexpr (isVisitable<DecayedVisitor, Return(int64_t)>()) {
            if (value.isInt64()) {
                return visitor.visit(value.toInt());
            }
        }
        if constexpr (isVisitable<DecayedVisitor, Return(uint64_t)>()) {
            if (value.isUInt64()) {
                return visitor.visit(value.toUInt());
            }
        }
        break;
    case CellValue::FLOAT:
        if constexpr (isVisitable<DecayedVisitor, Return(double)>()) {
            return visitor.visit(value.toDouble());
        }
        break;
    case CellValue::ASCII_STRING:
        if constexpr (isVisitable<DecayedVisitor, Return(std::string)>()) {
            return visitor.visit(value.toString());
        }
        // fall through
    case CellValue::UTF8_STRING:
        if constexpr (isVisitable<DecayedVisitor, Return(Utf8String)>()) {
            return visitor.visit(value.toUtf8String());
        }
        break;
    case CellValue::TIMESTAMP:
        if constexpr (isVisitable<DecayedVisitor, Return(Date)>()) {
            return visitor.visit(value.toTimestamp());
        }
        break;
    case CellValue::TIMEINTERVAL:
        if constexpr (isVisitable<DecayedVisitor, Return(std::tuple<int64_t, int64_t, double>)>()) {
            return visitor.visit(value.toMonthDaySecond());
        }
        break;
    case CellValue::BLOB:
        if constexpr (isVisitable<DecayedVisitor, Return(std::span<const std::byte>)>()) {
            return visitor.visit(value.blobSpan());
        }
        break;
    case CellValue::PATH:
        if constexpr (isVisitable<DecayedVisitor, Return(Path)>()) {
            return visitor.visit(value.coerceToPath());
        }
        break;
    case CellValue::NUM_CELL_TYPES:
    default:
        MLDB_THROW_LOGIC_ERROR("Unknown cell type %d", (int)value.cellType());
    }

    return visitor.unknown(value);    
}

} // namespace MLDB

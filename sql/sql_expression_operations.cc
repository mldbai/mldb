// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** sql_expression_operations.cc
    Jeremy Barnes, 24 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "sql_expression_operations.h"
#include "mldb/server/function_contexts.h"
#include "mldb/http/http_exception.h"
#include <boost/algorithm/string.hpp>
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "table_expression_operations.h"
#include <unordered_set>
#include "mldb/server/dataset_context.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* COMPARISON EXPRESSION                                                     */
/*****************************************************************************/

ComparisonExpression::
ComparisonExpression(std::shared_ptr<SqlExpression> lhs,
                     std::shared_ptr<SqlExpression> rhs,
                     std::string op)
    : lhs(std::move(lhs)),
      rhs(std::move(rhs)),
      op(op)
{
}

ComparisonExpression::
~ComparisonExpression()
{
}

// Calculate the effective timstamps for an expression involving two
// operands.
static Date calcTs(const ExpressionValue & v1,
                   const ExpressionValue & v2)
{
    return std::max(v1.getEffectiveTimestamp(),
                    v2.getEffectiveTimestamp());
}

BoundSqlExpression
doComparison(const SqlExpression * expr,
             const BoundSqlExpression & boundLhs, const BoundSqlExpression & boundRhs,
             bool (ExpressionValue::* op)(const ExpressionValue &) const)
{
    return {[=] (const SqlRowScope & row, ExpressionValue & storage)
            -> const ExpressionValue &
            {
                ExpressionValue lstorage, rstorage;
                const ExpressionValue & l = boundLhs(row, lstorage);
                const ExpressionValue & r = boundRhs(row, rstorage);
                // cerr << "left " << l << " " << "right " << r << endl;
                Date ts = calcTs(l, r);
                if (l.empty() || r.empty())
                    return storage = std::move(ExpressionValue::null(ts));
 
                return storage = std::move(ExpressionValue((l .* op)(r), ts));
            },
            expr,
            std::make_shared<BooleanValueInfo>()};
}

BoundSqlExpression
ComparisonExpression::
bind(SqlBindingScope & context) const
{
    auto boundLhs = lhs->bind(context);
    auto boundRhs = rhs->bind(context);

    if (op == "=" || op == "==") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator ==);
    }
    else if (op == "!=") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator !=);
    }
    else if (op == ">") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator > );
    }
    else if (op == "<") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator < );
    }
    else if (op == ">=") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator >=);
    }
    else if (op == "<=") {
        return doComparison(this, boundLhs, boundRhs,
                            &ExpressionValue::operator <=);
    }
    else throw HttpReturnException(400, "Unknown comparison op " + op);
}

Utf8String
ComparisonExpression::
print() const
{
    return "compare(\"" + op + "\"," + lhs->print() + "," + rhs->print() + ")";
}

std::shared_ptr<SqlExpression>
ComparisonExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ComparisonExpression>(*this);
    auto newArgs = transformArgs({ lhs, rhs });
    result->lhs = newArgs.at(0);
    result->rhs = newArgs.at(1);
    return result;
}

std::string
ComparisonExpression::
getType() const
{
    return "compare";
}

Utf8String
ComparisonExpression::
getOperation() const
{
    return Utf8String(op);
}

std::vector<std::shared_ptr<SqlExpression> >
ComparisonExpression::
getChildren() const
{
    if (lhs)
        return { lhs, rhs};
    else return { rhs };
}


/*****************************************************************************/
/* ARITHMETIC EXPRESSION                                                     */
/*****************************************************************************/

ArithmeticExpression::
ArithmeticExpression(std::shared_ptr<SqlExpression> lhs,
                     std::shared_ptr<SqlExpression> rhs,
                     std::string op)
    : lhs(std::move(lhs)),
      rhs(std::move(rhs)),
      op(op)
{
}

ArithmeticExpression::
~ArithmeticExpression()
{
}

template<typename ReturnInfo, typename Op>
BoundSqlExpression
doBinaryArithmetic(const SqlExpression * expr,
                   const BoundSqlExpression & boundLhs, const BoundSqlExpression & boundRhs,
                   const Op & op)
{
    return {[=] (const SqlRowScope & row, ExpressionValue & storage)
            -> const ExpressionValue &
            {
                ExpressionValue lstorage, rstorage;
                const ExpressionValue & l = boundLhs(row, lstorage);
                const ExpressionValue & r = boundRhs(row, rstorage);
                Date ts = calcTs(l, r);
                if (l.empty() || r.empty())
                    return storage = std::move(ExpressionValue::null(ts));
                return storage = std::move(ExpressionValue(op(l, r), ts));
            },
            expr,
            std::make_shared<ReturnInfo>()};
}

template<typename ReturnInfo, typename Op>
BoundSqlExpression
doUnaryArithmetic(const SqlExpression * expr,
                  const BoundSqlExpression & boundRhs,
                  const Op & op)
{
    return {[=] (const SqlRowScope & row, ExpressionValue & storage)
            -> const ExpressionValue &
            {
                ExpressionValue rstorage;
                const ExpressionValue & r = boundRhs(row, rstorage);
                if (r.empty())
                    return storage = std::move(ExpressionValue::null(r.getEffectiveTimestamp()));
                return storage = std::move(ExpressionValue(std::move(op(r)), r.getEffectiveTimestamp()));
            },
            expr,
            std::make_shared<ReturnInfo>()};
}

static CellValue binaryPlusOnTimestamp(const ExpressionValue & l, const ExpressionValue & r)
{
    ExcAssert(l.isTimestamp());

    if (r.isInteger())
    {
        //when no interval is specified (integer), operation is done in days
        return std::move(CellValue(l.getAtom().toTimestamp().addDays(r.toInt())));
    }
    else if (r.isTimeinterval())
    {
        //date + interval will give a date
        uint16_t months = 0, days = 0;
        float seconds = 0;

        std::tie(months, days, seconds) = r.getAtom().toMonthDaySecond();

        if (seconds < 0)
            return std::move(CellValue(l.getAtom().toTimestamp().minusMonthDaySecond(months, days, fabs(seconds))));
        else
            return std::move(CellValue(l.getAtom().toTimestamp().plusMonthDaySecond(months, days, seconds)));
    }

    throw HttpReturnException(400, "Adding unsupported type to timetamp");

    return std::move(CellValue(l.getAtom().toTimestamp()));

}

static CellValue binaryPlus(const ExpressionValue & l, const ExpressionValue & r)
{
    if (l.isString() || r.isString())
    {
       return std::move(CellValue(l.toUtf8String() + r.toUtf8String())); 
    }
    else if (l.isTimestamp())
    {
        return binaryPlusOnTimestamp(l, r);
    }
    else if (r.isTimestamp())
    {
        // + is commutative
        return binaryPlusOnTimestamp(r, l);
    }
    else if (l.isTimeinterval())
    {
        int64_t lmonths = 0, ldays = 0, rmonths = 0, rdays = 0;
        float lseconds = 0, rseconds = 0;

        std::tie(lmonths, ldays, lseconds) = l.getAtom().toMonthDaySecond();
        std::tie(rmonths, rdays, rseconds) = r.getAtom().toMonthDaySecond();

        lmonths += rmonths;
        ldays += rdays;
        lseconds += rseconds;

        //no implicit quantization;
        
        return std::move(CellValue::fromMonthDaySecond(lmonths, ldays, lseconds));
    }
    else
    {
        return std::move(CellValue(l.toDouble() + r.toDouble()));
    }
}

static CellValue binaryMinusOnTimestamp(const ExpressionValue & l, const ExpressionValue & r)
{
    ExcAssert(l.isTimestamp());

    if (r.isInteger())
    {
        //when no interval is specified (integer), operation is done in days
        return std::move(CellValue(l.getAtom().toTimestamp().addDays(-r.toInt())));
    }
    else if (r.isTimeinterval())
    {
        //date - interval will give a date
        //date + interval will give a date
        int64_t months = 0, days = 0;
        float seconds = 0;

        std::tie(months, days, seconds) = r.getAtom().toMonthDaySecond();

        if (seconds >= 0)
            return std::move(CellValue(l.getAtom().toTimestamp().minusMonthDaySecond(months, days, fabs(seconds))));
        else
            return std::move(CellValue(l.getAtom().toTimestamp().plusMonthDaySecond(months, days, seconds)));
    }
    else if (r.isTimestamp())
    {
        //date - date gives us an interval
        int64_t days = 0;
        float seconds = 0;
        std::tie(days, seconds) = l.getAtom().toTimestamp().getDaySecondInterval(r.getAtom().toTimestamp());
        return std::move(CellValue::fromMonthDaySecond(0, days, seconds));
    }

    throw HttpReturnException(400, "Substracting unsupported type to timetamp");
    return std::move(CellValue(l.getAtom().toTimestamp()));

}

static CellValue binaryMinus(const ExpressionValue & l, const ExpressionValue & r)
{
    if (l.isTimestamp())
    {
        return binaryMinusOnTimestamp(l, r);
    }
    else if (l.isTimeinterval() && r.isTimeinterval())
    {
        int64_t lmonths = 0, ldays = 0, rmonths = 0, rdays = 0;
        float lseconds = 0, rseconds = 0;

        std::tie(lmonths, ldays, lseconds) = l.getAtom().toMonthDaySecond();
        std::tie(rmonths, rdays, rseconds) = r.getAtom().toMonthDaySecond();

        lmonths -= rmonths;
        ldays -= rdays;
        lseconds -= rseconds;

        //no implicit quantization;
        
        return std::move(CellValue::fromMonthDaySecond(lmonths, ldays, lseconds));
    }

    return std::move(CellValue(l.toDouble() - r.toDouble()));
}

static CellValue unaryMinus(const ExpressionValue & r)
{
    if (r.isInteger())
        return -r.toInt();
    else if (r.isTimeinterval())
    {
        int64_t months = 0, days = 0;
        float seconds = 0.0f;

        std::tie(months, days, seconds) = r.getAtom().toMonthDaySecond();

        if (seconds == 0.0f)
            seconds = -0.0f;
        else if (seconds == -0.0f)
            seconds = 0.0f;
        else 
            seconds = -seconds;

        return std::move(CellValue::fromMonthDaySecond(months, days, seconds));
    }
    else return -r.toDouble();
}

static CellValue multiplyInterval(const ExpressionValue & l, double rvalue)
{
    int64_t months, days;
    float seconds;

    std::tie(months, days, seconds) = l.getAtom().toMonthDaySecond();
    months *= rvalue;

    double ddays = days * rvalue;    
    double fractionalDays = modf(ddays, &ddays); //carry the remainder of days into hours/minute/seconds.
    days = ddays;
    
    seconds *= rvalue;
    seconds += fractionalDays*24.0f*60*60;

    return std::move(CellValue::fromMonthDaySecond(months, days, seconds));
}

static CellValue binaryMultiplication(const ExpressionValue & l, const ExpressionValue & r)
{
    if (l.isTimeinterval() && r.isNumber())
    {
        return multiplyInterval(l,r.toDouble());
    }
    else if (r.isTimeinterval() && l.isNumber())
    {
        return multiplyInterval(r,l.toDouble());
    }
    else return l.toDouble() * r.toDouble();
}

static CellValue binaryDivision(const ExpressionValue & l, const ExpressionValue & r)
{
    if (l.isTimeinterval() && r.isNumber())
    {
        int64_t months, days;
        float seconds;

        double rvalue = r.toDouble();

        std::tie(months, days, seconds) = l.getAtom().toMonthDaySecond();
        months /= rvalue;

        double ddays = days / rvalue;    
        double fractionalDays = modf(ddays, &ddays); //carry the remainder of days into hours/minute/seconds.
        days = ddays;
        
        seconds /= rvalue;
        seconds += fractionalDays*24.0f*60*60;

        return std::move(CellValue::fromMonthDaySecond(months, ddays, seconds));
    }
    else return l.toDouble() / r.toDouble();
}

template<class T1, class T2>
CellValue safeIntegerMod(const T1 a, const T2 b)
{
    if (b == 0)
    {
        throw HttpReturnException(400, "Integer Modulus by a zero dividend");
    }
    else
    {
        return a % b;
    }
}

static CellValue binaryModulus(const ExpressionValue & l, const ExpressionValue & r)
{
    CellValue la = l.getAtom(), ra = r.getAtom();

    if (la.isInteger() && ra.isInteger()) {
        if (la.isUInt64() && ra.isUInt64()) {
            return safeIntegerMod<uint64_t, uint64_t>(la.toUInt(), ra.toUInt());
        }
        else if (la.isUInt64()) {
            return safeIntegerMod<uint64_t, int64_t>(la.toUInt(), ra.toInt());
        }
        else if (ra.isUInt64()) {
            return safeIntegerMod<int64_t, uint64_t>(la.toInt(), ra.toUInt());
        }
        else {
            return safeIntegerMod<int64_t, int64_t>(la.toInt(), ra.toInt());
        }
    }
    else return fmod(la.toDouble(), ra.toDouble());
}

BoundSqlExpression
ArithmeticExpression::
bind(SqlBindingScope & context) const
{
    auto boundLhs = lhs ? lhs->bind(context) : BoundSqlExpression();
    auto boundRhs = rhs->bind(context);

    if (op == "+" && lhs) {
        return doBinaryArithmetic<AtomValueInfo>(this, boundLhs, boundRhs, &binaryPlus);
    }
    else if (op == "-" && lhs) {
        return doBinaryArithmetic<AtomValueInfo>(this, boundLhs, boundRhs, &binaryMinus);
    }
    else if (op == "-" && !lhs) {
        return doUnaryArithmetic<AtomValueInfo>(this, boundRhs, &unaryMinus);
    }
    else if (op == "*" && lhs) {
        return doBinaryArithmetic<AtomValueInfo>(this, boundLhs, boundRhs, &binaryMultiplication);
    }
    else if (op == "/" && lhs) {
        return doBinaryArithmetic<AtomValueInfo>(this, boundLhs, boundRhs, &binaryDivision);
    }
    else if (op == "%" && lhs) {
        return doBinaryArithmetic<Float64ValueInfo>(this, boundLhs, boundRhs, &binaryModulus);
    }
    else throw HttpReturnException(400, "Unknown arithmetic op " + op
                                   + (lhs ? " binary" : " unary"));
}

Utf8String
ArithmeticExpression::
print() const
{
    if (lhs)
        return "arith(\"" + op + "\"," + lhs->print() + "," + rhs->print() + ")";
    else
        return "arith(\"" + op + "\"," + rhs->print() + ")";
}

std::shared_ptr<SqlExpression>
ArithmeticExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ArithmeticExpression>(*this);
    if (lhs) {
        auto newArgs = transformArgs({ lhs, rhs });
        result->lhs = newArgs.at(0);
        result->rhs = newArgs.at(1);
    }
    else {
        auto newArgs = transformArgs({ rhs });
        result->rhs = newArgs.at(0);
    }
    return result;
}

std::string
ArithmeticExpression::
getType() const
{
    return "arith";
}

Utf8String
ArithmeticExpression::
getOperation() const
{
    return Utf8String(op);
}

std::vector<std::shared_ptr<SqlExpression> >
ArithmeticExpression::
getChildren() const
{
    if (lhs)
        return { lhs, rhs};
    else return { rhs };
}


/*****************************************************************************/
/* BITWISE EXPRESSION                                                        */
/*****************************************************************************/

BitwiseExpression::
BitwiseExpression(std::shared_ptr<SqlExpression> lhs,
                     std::shared_ptr<SqlExpression> rhs,
                     std::string op)
    : lhs(std::move(lhs)),
      rhs(std::move(rhs)),
      op(op)
{
}

BitwiseExpression::
~BitwiseExpression()
{
}

template<typename Op>
BoundSqlExpression
doBinaryBitwise(const SqlExpression * expr,
                const BoundSqlExpression & boundLhs, const BoundSqlExpression & boundRhs,
                const Op & op)
{
    return {[=] (const SqlRowScope & row, ExpressionValue & storage)
            -> const ExpressionValue &
            {
                ExpressionValue lstorage, rstorage;
                const ExpressionValue & l = boundLhs(row, lstorage);
                const ExpressionValue & r = boundRhs(row, rstorage);
                Date ts = calcTs(l, r);
                if (l.empty() || r.empty())
                    return storage = std::move(ExpressionValue::null(ts));
                return storage = std::move(ExpressionValue(op(l.toInt(), r.toInt()), ts));
            },
            expr,
            std::make_shared<IntegerValueInfo>()};
}

template<typename Op>
BoundSqlExpression
doUnaryBitwise(const SqlExpression * expr,
               const BoundSqlExpression & boundRhs,
               const Op & op)
{
    return {[=] (const SqlRowScope & row, ExpressionValue & storage)
            -> const ExpressionValue &
            {
                ExpressionValue rstorage;
                const ExpressionValue & r = boundRhs(row, rstorage);
                if (r.empty())
                    return storage = std::move(ExpressionValue::null(r.getEffectiveTimestamp()));
                return storage = std::move(ExpressionValue(std::move(op(r.toInt())), r.getEffectiveTimestamp()));
            },
            expr,
            std::make_shared<IntegerValueInfo>()};
}

static CellValue doBitwiseAnd(int64_t v1, int64_t v2)
{
    return v1 & v2;
}

static CellValue doBitwiseOr(int64_t v1, int64_t v2)
{
    return v1 | v2;
}

static CellValue doBitwiseXor(int64_t v1, int64_t v2)
{
    return v1 | v2;
}

static CellValue doBitwiseNot(int64_t v1)
{
    return ~v1;
}

BoundSqlExpression
BitwiseExpression::
bind(SqlBindingScope & context) const
{
    auto boundLhs = lhs ? lhs->bind(context) : BoundSqlExpression();
    auto boundRhs = rhs->bind(context);

    if (op == "&" && lhs) {
        return doBinaryBitwise(this, boundLhs, boundRhs, &doBitwiseAnd);
    }
    else if (op == "|" && lhs) {
        return doBinaryBitwise(this, boundLhs, boundRhs, &doBitwiseOr);
    }
    else if (op == "^" && lhs) {
        return doBinaryBitwise(this, boundLhs, boundRhs, &doBitwiseXor);
    }
    else if (op == "~" && !lhs) {
        return doUnaryBitwise(this, boundRhs, &doBitwiseNot);
    }
    else throw HttpReturnException(400, "Unknown bitwise op " + op
                                   + (lhs ? " binary" : " unary"));
}

Utf8String
BitwiseExpression::
print() const
{
    if (lhs)
        return "bitwise(\"" + op + "\"," + lhs->print() + "," + rhs->print() + ")";
    else
        return "bitwise(\"" + op + "\"," + rhs->print() + ")";
}

std::shared_ptr<SqlExpression>
BitwiseExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<BitwiseExpression>(*this);
    if (lhs) {
        auto newArgs = transformArgs({ lhs, rhs });
        result->lhs = newArgs.at(0);
        result->rhs = newArgs.at(1);
    }
    else {
        auto newArgs = transformArgs({ rhs });
        result->rhs = newArgs.at(0);
    }
    return result;
}

std::string
BitwiseExpression::
getType() const
{
    return "bitwise";
}

Utf8String
BitwiseExpression::
getOperation() const
{
    return Utf8String(op);
}

std::vector<std::shared_ptr<SqlExpression> >
BitwiseExpression::
getChildren() const
{
    if (lhs)
        return { lhs, rhs};
    else return { rhs };
}

/*****************************************************************************/
/* READ VARIABLE EXPRESSION                                                  */
/*****************************************************************************/

ReadVariableExpression::
ReadVariableExpression(Utf8String tableName, Utf8String variableName)
    : tableName(std::move(tableName)), variableName(std::move(variableName))
{
}

ReadVariableExpression::
~ReadVariableExpression()
{
}

BoundSqlExpression
ReadVariableExpression::
bind(SqlBindingScope & context) const
{
    auto getVariable = context.doGetVariable(tableName, variableName);

    if (!getVariable.info) {
        throw HttpReturnException(400, "context " + ML::type_name(context)
                            + " getVariable '" + variableName
                            + "' didn't return info");
    }

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage) -> const ExpressionValue &
            {
                // TODO: allow it access to storage
                return getVariable(row, storage);
            },
            this,
            getVariable.info};
}

Utf8String
ReadVariableExpression::
print() const
{
    return "variable(\"" + variableName + "\")";
}

std::shared_ptr<SqlExpression>
ReadVariableExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ReadVariableExpression>(*this);
    return result;
}

std::string
ReadVariableExpression::
getType() const
{
    return "variable";
}

Utf8String
ReadVariableExpression::
getOperation() const
{
    return variableName;
}

std::vector<std::shared_ptr<SqlExpression> >
ReadVariableExpression::
getChildren() const
{
    return {};
}

std::map<ScopedName, UnboundVariable>
ReadVariableExpression::
variableNames() const
{
    return { { { tableName, variableName }, { std::make_shared<AnyValueInfo>() } } };
}


/*****************************************************************************/
/* CONSTANT EXPRESSION                                                       */
/*****************************************************************************/

ConstantExpression::
ConstantExpression(ExpressionValue constant)
    : constant(constant)
{
}

ConstantExpression::
~ConstantExpression()
{
}

BoundSqlExpression
ConstantExpression::
bind(SqlBindingScope & context) const
{
    return {[=] (const SqlRowScope &, ExpressionValue & storage) -> const ExpressionValue &
            {
                return storage = constant;
            },
            this,
            constant.getSpecializedValueInfo(),
            true /* is constant */};
}

Utf8String
ConstantExpression::
print() const
{
    return "constant(" + jsonEncodeUtf8(constant) + ")";
}

std::shared_ptr<SqlExpression>
ConstantExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ConstantExpression>(*this);
    return result;
}

bool
ConstantExpression::
isConstant() const
{
    return true;
}

ExpressionValue
ConstantExpression::
constantValue() const
{
    return constant;
}

std::string
ConstantExpression::
getType() const
{
    return "constant";
}

Utf8String
ConstantExpression::
getOperation() const
{
    return jsonEncodeUtf8(constant);
}

std::vector<std::shared_ptr<SqlExpression> >
ConstantExpression::
getChildren() const
{
    return {};
}


/*****************************************************************************/
/* SELECT WITHIN EXPRESSION                                                  */
/*****************************************************************************/

SelectWithinExpression::
SelectWithinExpression(std::shared_ptr<SqlRowExpression> select)
    : select(select)
{
}

SelectWithinExpression::
~SelectWithinExpression()
{
}

BoundSqlExpression
SelectWithinExpression::
bind(SqlBindingScope & context) const
{
    return select->bind(context);
}

Utf8String
SelectWithinExpression::
print() const
{
    return "select(" + select->print() + ")";
}

std::shared_ptr<SqlExpression>
SelectWithinExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<SelectWithinExpression>
        (std::dynamic_pointer_cast<SqlRowExpression>(select->transform(transformArgs)));
    return result;
}

std::string
SelectWithinExpression::
getType() const
{
    return "select";
}

Utf8String
SelectWithinExpression::
getOperation() const
{
    return select->print();
}

std::vector<std::shared_ptr<SqlExpression> >
SelectWithinExpression::
getChildren() const
{
    auto result = select->getChildren();
    result.push_back(select);
    return result;
}

/*****************************************************************************/
/* EMBEDDING EXPRESSION                                                      */
/*****************************************************************************/

EmbeddingLiteralExpression::
EmbeddingLiteralExpression(vector<std::shared_ptr<SqlExpression> >& clauses)
    : clauses(clauses)
{
}

EmbeddingLiteralExpression::
~EmbeddingLiteralExpression()
{
}

BoundSqlExpression
EmbeddingLiteralExpression::
bind(SqlBindingScope & context) const
{
    vector<BoundSqlExpression> boundClauses;
    for (auto & c: clauses) {
        boundClauses.emplace_back(std::move(c->bind(context)));
    }   

    auto outputInfo = std::make_shared<EmbeddingValueInfo>(clauses.size());

    auto exec = [=] (const SqlRowScope & context,
                     ExpressionValue & storage) -> const ExpressionValue &
        {  
            std::vector<CellValue> outputValues;             
            Date ts;

            for (auto & c: boundClauses) {
                ExpressionValue v = c(context);
                if (!v.isAtom()) {
		   throw HttpReturnException(400, "Trying to add non-atomic value to embedding expression");
                }
               
               ts = std::max(v.getEffectiveTimestamp(), ts);
               outputValues.push_back(v.getAtom());

            }
            
            return storage = ExpressionValue(outputValues ,ts);
        };

    return BoundSqlExpression(exec, this, outputInfo, false);
}

Utf8String
EmbeddingLiteralExpression::
print() const
{
    Utf8String output =  "embed[" + clauses[0]->print();

    for (int i = 1; i < clauses.size(); ++i)
    {
        output += "," + clauses[i]->print();
    }

    output += "]";

    return output;
}

std::shared_ptr<SqlExpression>
EmbeddingLiteralExpression::
transform(const TransformArgs & transformArgs) const
{
    vector<std::shared_ptr<SqlExpression> > transformclauses;

    for (auto& c : clauses)
    {
        transformclauses.push_back(c->transform(transformArgs));
    }

    auto result = std::make_shared<EmbeddingLiteralExpression>(transformclauses);

    return result;
}

std::string
EmbeddingLiteralExpression::
getType() const
{
    return "embedding";
}

Utf8String
EmbeddingLiteralExpression::
getOperation() const
{
    Utf8String result("[");
    for (unsigned i = 0;  i < clauses.size();  ++i) {
        if (i > 0)
            result += ", ";
        result += clauses[i]->print();
    }

    result += "]";
    
    return result;
}

std::vector<std::shared_ptr<SqlExpression> >
EmbeddingLiteralExpression::
getChildren() const
{
    return clauses;
}


/*****************************************************************************/
/* BOOLEAN OPERATOR EXPRESSION                                               */
/*****************************************************************************/

BooleanOperatorExpression::
BooleanOperatorExpression(std::shared_ptr<SqlExpression> lhs,
                          std::shared_ptr<SqlExpression> rhs,
                          std::string op)
    : lhs(std::move(lhs)),
      rhs(std::move(rhs)),
      op(op)
{
}

BooleanOperatorExpression::
~BooleanOperatorExpression()
{
}

BoundSqlExpression
BooleanOperatorExpression::
bind(SqlBindingScope & context) const
{
    auto boundLhs = lhs ? lhs->bind(context) : BoundSqlExpression();
    auto boundRhs = rhs->bind(context);

    if (op == "AND" && lhs) {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage) -> const ExpressionValue &
                {
                    ExpressionValue lstorage, rstorage;
                    const ExpressionValue & l = boundLhs(row, lstorage);
                    const ExpressionValue & r = boundRhs(row, rstorage);
                    if (l.isFalse() && r.isFalse()) {
                        Date ts = std::min(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = std::move(ExpressionValue(false, ts));
                    }
                    else if (l.isFalse()) {
                        return storage = std::move(ExpressionValue(false, l.getEffectiveTimestamp()));
                    }
                    else if (r.isFalse()) {
                        return storage = std::move(ExpressionValue(false, r.getEffectiveTimestamp()));
                    }
                    else if (l.empty() && r.empty()) {
                        Date ts = std::min(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = std::move(ExpressionValue::null(ts));
                    }
                    else if (l.empty())
                        return storage = std::move(ExpressionValue::null(l.getEffectiveTimestamp()));
                    else if (r.empty())
                        return storage = std::move(ExpressionValue::null(r.getEffectiveTimestamp()));
                    Date ts = std::max(l.getEffectiveTimestamp(),
                                       r.getEffectiveTimestamp());
                    return storage = std::move(ExpressionValue(true, ts));
                },
                this,
                std::make_shared<BooleanValueInfo>()};
    }
    else if (op == "OR" && lhs) {
        return {[=] (const SqlRowScope & row, ExpressionValue & storage)
                -> const ExpressionValue &
                {
                    ExpressionValue lstorage, rstorage;
                    const ExpressionValue & l = boundLhs(row, lstorage);
                    const ExpressionValue & r = boundRhs(row, rstorage);

                    if (l.isTrue() && r.isTrue()) {
                        Date ts = std::max(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = std::move(ExpressionValue(true, ts));
                    }
                    else if (l.isTrue()) {
                        return storage = std::move(ExpressionValue(true, l.getEffectiveTimestamp()));
                    }
                    else if (r.isTrue()) {
                        return storage = std::move(ExpressionValue(true, r.getEffectiveTimestamp()));
                    }
                    else if (l.empty() && r.empty()) {
                        Date ts = std::max(l.getEffectiveTimestamp(),
                                           r.getEffectiveTimestamp());
                        return storage = std::move(ExpressionValue::null(ts));
                    }
                    else if (l.empty())
                        return storage = std::move(ExpressionValue::null(l.getEffectiveTimestamp()));
                    else if (r.empty())
                        return storage = std::move(ExpressionValue::null(r.getEffectiveTimestamp()));
                    Date ts = std::min(l.getEffectiveTimestamp(),
                                       r.getEffectiveTimestamp());
                    return storage = std::move(ExpressionValue(false, ts));
                },
                this,
                std::make_shared<BooleanValueInfo>()};
    }
    else if (op == "NOT" && !lhs) {
        return {[=] (const SqlRowScope & row, ExpressionValue & storage)
                -> const ExpressionValue &
                {
                    ExpressionValue rstorage;
                    const ExpressionValue & r = boundRhs(row, rstorage);
                    if (r.empty())
                        return storage = std::move(r);
                    return storage = std::move(ExpressionValue(!r.isTrue(), r.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<BooleanValueInfo>()};
    }
    else throw HttpReturnException(400, "Unknown boolean op " + op
                             + (lhs ? " binary" : " unary"));
}

Utf8String
BooleanOperatorExpression::
print() const
{
    if (lhs)
        return "boolean(\"" + op + "\"," + lhs->print() + "," + rhs->print() + ")";
    else
        return "boolean(\"" + op + "\"," + rhs->print() + ")";
}

std::shared_ptr<SqlExpression>
BooleanOperatorExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<BooleanOperatorExpression>(*this);
    if (lhs) {
        auto newArgs = transformArgs({ lhs, rhs });
        result->lhs = newArgs.at(0);
        result->rhs = newArgs.at(1);
    }
    else {
        auto newArgs = transformArgs({ rhs });
        result->rhs = newArgs.at(0);
    }
    return result;
}

std::string
BooleanOperatorExpression::
getType() const
{
    return "boolean";
}

Utf8String
BooleanOperatorExpression::
getOperation() const
{
    return Utf8String(op);
}

std::vector<std::shared_ptr<SqlExpression> >
BooleanOperatorExpression::
getChildren() const
{
    if (lhs)
        return { lhs, rhs};
    else return { rhs };
}


/*****************************************************************************/
/* IS TYPE EXPRESSION                                                        */
/*****************************************************************************/

IsTypeExpression::
IsTypeExpression(std::shared_ptr<SqlExpression> expr,
                 bool notType,
                 std::string type)
    : expr(std::move(expr)),
      notType(notType),
      type(type)
{
}

IsTypeExpression::
~IsTypeExpression()
{
}

BoundSqlExpression
IsTypeExpression::
bind(SqlBindingScope & context) const
{
    auto boundExpr = expr->bind(context);

    bool (ExpressionValue::* fn) () const;
    
    if (type == "string") {
        fn = &ExpressionValue::isString;
    }
    else if (type == "integer") {
        fn = &ExpressionValue::isInteger;
    }
    else if (type == "number") {
        fn = &ExpressionValue::isNumber;
    }
    else if (type == "timestamp") {
        fn = &ExpressionValue::isTimestamp;
    }
    else if (type == "null") {
        fn = &ExpressionValue::empty;
    }
    else if (type == "true") {
        fn = &ExpressionValue::isTrue;
    }
    else if (type == "false") {
        fn = &ExpressionValue::isFalse;
    }
    else throw HttpReturnException(400, "Unknown type `" + type + "' for IsTypeExpression");

    return {[=] (const SqlRowScope & row, ExpressionValue & storage) -> const ExpressionValue &
            {
                auto v = boundExpr(row);
                bool val = (v .* fn) ();
                return storage = std::move(ExpressionValue(notType ? !val : val,
                                                           v.getEffectiveTimestamp()));
            },
            this,
            std::make_shared<BooleanValueInfo>()};
}

Utf8String
IsTypeExpression::
print() const
{
    if (notType) {
        return "istype(" + expr->print() + ",\"" + type + "\",0)";
    }
    else {
        return "istype(" + expr->print() + ",\"" + type + "\",1)";
    }
}

std::shared_ptr<SqlExpression>
IsTypeExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<IsTypeExpression>(*this);
    result->expr = transformArgs({ expr }).at(0);
    return result;
}

std::string
IsTypeExpression::
getType() const
{
    return notType ? "nottype" : "type";
}

Utf8String
IsTypeExpression::
getOperation() const
{
    return Utf8String(type);
}

std::vector<std::shared_ptr<SqlExpression> >
IsTypeExpression::
getChildren() const
{
    return { expr };
}


/*****************************************************************************/
/* FUNCTION CALL WRAPPER                                                     */
/*****************************************************************************/

FunctionCallWrapper::
FunctionCallWrapper(Utf8String tableName,
                        Utf8String function,
                        std::vector<std::shared_ptr<SqlExpression> > args,
                        std::shared_ptr<SqlExpression> extract)
    : tableName(tableName), functionName(function), args(args), extract(extract)
{

}

FunctionCallWrapper::
~FunctionCallWrapper()
{

}

BoundSqlExpression
FunctionCallWrapper::
bind(SqlBindingScope & context) const
{
    //check whether it is a builtin or not
    if (context.functionStackDepth > 100)
            throw HttpReturnException(400, "Reached a stack depth of over 100 functions while analysing query, possible infinite recursion");

    context.functionStackDepth++;
    std::vector<BoundSqlExpression> boundArgs;
    for (auto& arg : args)
    {
        boundArgs.emplace_back(std::move(arg->bind(context)));
    }

    BoundFunction fn = context.doGetFunction(tableName, functionName, boundArgs);
    BoundSqlExpression boundOutput;

    if (fn)
    {
        //context confirm it is builtin
        boundOutput = bindBuiltinFunction(context, boundArgs, fn);
    }
    else
    {
        //assume user
        boundOutput = bindUserFunction(context);
    }
    context.functionStackDepth--;
    return boundOutput;
}

BoundSqlExpression
(*bindSelectApplyFunctionExpressionFn) (const Utf8String & functionName,
                                        const SelectExpression & with,
                                           const SqlRowExpression * expr,
                                        const SqlBindingScope & context);

BoundSqlExpression
(*bindApplyFunctionExpressionFn) (const Utf8String & functionName,
                                  const SelectExpression & with,
                                  const SqlExpression & extract,
                                  const SqlExpression * expr,
                                  SqlBindingScope & context);

BoundSqlExpression
FunctionCallWrapper::
bindUserFunction(SqlBindingScope & context) const
{
    std::vector<std::shared_ptr<SqlRowExpression> > clauses;
    if (args.size() > 0)
    {
         if (args.size() > 1)
            throw HttpReturnException(400, "User function " + functionName + " expected a single row { } argument");

         auto result = std::dynamic_pointer_cast<SelectWithinExpression>(args[0]);
         if (result)
         {
            clauses.push_back(result->select);
         }
         else
         {
            throw HttpReturnException(400, "User function " + functionName
                                   + " expect a row argument ({ }), got " + args[0]->print() );
         }
    }    

    SelectExpression with(clauses);

    if (extract)
    {
        //extract, return the single value
        return bindApplyFunctionExpressionFn(functionName, with, *extract, this, context);
    }
    else
    {
        //no extract, return the whole row
       return bindSelectApplyFunctionExpressionFn(functionName, with, this, context);
    }
}

BoundSqlExpression
FunctionCallWrapper::
bindBuiltinFunction(SqlBindingScope & context, std::vector<BoundSqlExpression>& boundArgs, BoundFunction& fn) const
{
    if (extract)
            throw HttpReturnException(400, "Builtin function " + functionName
                                   + " should not have an extract [] expression, got " + extract->print() );

    Utf8String functionNameLower(boost::algorithm::to_lower_copy(functionName.extractAscii()));

    bool isAggregate = tryLookupAggregator(functionNameLower) != nullptr;

    if (isAggregate)
    {
             return {[=] (const SqlRowScope & row,
                            ExpressionValue & storage) -> const ExpressionValue &
            {
                std::vector<ExpressionValue> evaluatedArgs;
                //Don't evaluate the args for aggregator
                evaluatedArgs.resize(boundArgs.size());
                return storage = std::move(fn(evaluatedArgs, row));
            },
            this,
            fn.resultInfo};
    }
    else
    {
         return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage) -> const ExpressionValue &
            {
                std::vector<ExpressionValue> evaluatedArgs;
                evaluatedArgs.reserve(boundArgs.size());
                for (auto & a: boundArgs)
                    evaluatedArgs.emplace_back(std::move(a(row)));
                
                // TODO: function call that allows function to own its args & have
                // storage
                return storage = std::move(fn(evaluatedArgs, row));
            },
            this,
            fn.resultInfo};
    }
}

Utf8String
FunctionCallWrapper::
print() const
{
    Utf8String result = "function(\"" + functionName + "\"";

    for (auto & a : args)
    {
        result += "," + a->print();
    }

    if (extract)
        result += "," + extract->print();

    result += ")";

    return result;   
}

std::shared_ptr<SqlExpression>
FunctionCallWrapper::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<FunctionCallWrapper>(*this);
    for (auto & a : result->args)
    {
        a = a->transform(transformArgs);
    }

    if (extract)
        result->extract = result->extract->transform(transformArgs);

    return result;
}

std::string
FunctionCallWrapper::
getType() const
{
    return "function";
}

Utf8String
FunctionCallWrapper::
getOperation() const
{
    return functionName;
}

std::vector<std::shared_ptr<SqlExpression> >
FunctionCallWrapper::
getChildren() const
{   
    std::vector<std::shared_ptr<SqlExpression> > res = args;
   
    if (extract)
        res.push_back(extract);

    return res;
}

std::map<ScopedName, UnboundFunction>
FunctionCallWrapper::
functionNames() const
{
    std::map<ScopedName, UnboundFunction> result;
    // TODO: actually get arguments
    result[ScopedName(tableName, functionName)].argsForArity[args.size()] = {};
    
    // Now go into our arguments and also extract the functions called
    for (auto & a: args) {
        std::map<ScopedName, UnboundFunction> argF = a->functionNames();
        for (auto & a: argF) {
            result[a.first].merge(a.second);
        }
    }

    return result;
}


/*****************************************************************************/
/* CASE EXPRESSION                                                           */
/*****************************************************************************/

CaseExpression::
CaseExpression(std::shared_ptr<SqlExpression> expr,
               std::vector<std::pair<std::shared_ptr<SqlExpression>,
                                     std::shared_ptr<SqlExpression> > > when,
               std::shared_ptr<SqlExpression> elseExpr)
    : expr(std::move(expr)),
      when(std::move(when)),
      elseExpr(std::move(elseExpr))
{
}

BoundSqlExpression
CaseExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundElse;
    if (elseExpr)
        boundElse = elseExpr->bind(context);

    std::vector<std::pair<BoundSqlExpression, BoundSqlExpression> > boundWhen;

    for (auto & w: when) {
        boundWhen.emplace_back(w.first->bind(context), w.second->bind(context));
    }

    if (expr) {
        // Simple CASE expression

        auto boundExpr = expr->bind(context);

        return {[=] (const SqlRowScope & row, ExpressionValue & storage)
                    -> const ExpressionValue &
                {
                    ExpressionValue vstorage;
                    const ExpressionValue & v = boundExpr(row, vstorage);
                    
                    if (!v.empty()) {
                        for (auto & w: boundWhen) {
                            ExpressionValue wstorage;
                            const ExpressionValue & v2 = w.first(row, wstorage);
                            if (v2.empty())
                                continue;
                            if (v2 == v)
                                return w.second(row, storage);
                        }
                    }

                    if (elseExpr)
                        return boundElse(row, storage);
                    else return storage = std::move(ExpressionValue());
                },
                this,
                // TODO: infer the type
                std::make_shared<AnyValueInfo>()};
    }
    else {
        // Searched CASE expression
        return {[=] (const SqlRowScope & row, ExpressionValue & storage)
                    -> const ExpressionValue &
                {
                    for (auto & w: boundWhen) {
                        ExpressionValue wstorage;
                        const ExpressionValue & c = w.first(row, wstorage);
                        if (c.isTrue())
                            return w.second(row, storage);
                    }

                    if (elseExpr)
                        return boundElse(row, storage);
                    else return storage = std::move(ExpressionValue());
                },
                this,
                std::make_shared<AnyValueInfo>()};
    }
}

Utf8String
CaseExpression::
print() const
{
    Utf8String result
        = "case(\""
        + (expr ? expr->print() : Utf8String("null"))
        + ",[";

    bool first = true;
    for (auto & w: when) {
        if (first) first = false;
        else result += ",";
        result += "[" + w.first->print() + "," + w.second->print() + "]";
    }
    result += "]";
    if (elseExpr) {
        result += "," + elseExpr->print();
    }
    result += ")";
    return result;
}

std::shared_ptr<SqlExpression>
CaseExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<CaseExpression>(*this);

    if (expr)
        result->expr = result->expr->transform(transformArgs);
    for (auto & w: result->when) {
        w.first = w.first->transform(transformArgs);
        w.second = w.second->transform(transformArgs);
    }
    if (elseExpr)
        result->elseExpr = result->elseExpr->transform(transformArgs);

    return result;
}

std::string
CaseExpression::
getType() const
{
    return "case";
}

Utf8String
CaseExpression::
getOperation() const
{
    return expr ? Utf8String("simple"): Utf8String("matched");
}

std::vector<std::shared_ptr<SqlExpression> >
CaseExpression::
getChildren() const
{
    std::vector<std::shared_ptr<SqlExpression> > res;
    if (expr)
        res.emplace_back(expr);
    for (auto & w: when) {
        res.emplace_back(w.first);
        res.emplace_back(w.second);
    }
    if (elseExpr)
        res.emplace_back(elseExpr);

    return res;
}


/*****************************************************************************/
/* BETWEEN EXPRESSION                                                        */
/*****************************************************************************/

BetweenExpression::
BetweenExpression(std::shared_ptr<SqlExpression> expr,
                  std::shared_ptr<SqlExpression> lower,
                  std::shared_ptr<SqlExpression> upper,
                  bool notBetween)
    : expr(std::move(expr)),
      lower(std::move(lower)),
      upper(std::move(upper)),
      notBetween(notBetween)
{
}

BoundSqlExpression
BetweenExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundExpr  = expr->bind(context);
    BoundSqlExpression boundLower = lower->bind(context);
    BoundSqlExpression boundUpper = upper->bind(context);

    return {[=] (const SqlRowScope & row,
                 ExpressionValue & storage) -> const ExpressionValue &
            {
                ExpressionValue vstorage, lstorage, ustorage;

                const ExpressionValue & v = boundExpr(row, vstorage);
                const ExpressionValue & l = boundLower(row, lstorage);
                const ExpressionValue & u = boundUpper(row, ustorage);
                if (v.empty())
                    return storage = v;

                if (l.empty())
                    return storage = l;

                if (v < l)
                    return storage = std::move(ExpressionValue(notBetween,
                                                               std::max(v.getEffectiveTimestamp(),
                                                                        l.getEffectiveTimestamp())));

                if (u.empty())
                    return storage = u;
                if (v > u)
                    return storage = std::move(ExpressionValue(notBetween,
                                           std::max(v.getEffectiveTimestamp(),
                                                    u.getEffectiveTimestamp())));

                return storage = std::move(ExpressionValue(!notBetween,
                                                           std::max(std::max(v.getEffectiveTimestamp(),
                                                                             u.getEffectiveTimestamp()),
                                                                    l.getEffectiveTimestamp())));
            },
            this,
            std::make_shared<BooleanValueInfo>()};
}

Utf8String
BetweenExpression::
print() const
{
    Utf8String result
        = "between(\""
        + expr->print()
        + ","
        + lower->print()
        + ","
        + upper->print()
        + ")";
    return result;
}

std::shared_ptr<SqlExpression>
BetweenExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<BetweenExpression>(*this);
    result->expr  = result->expr->transform(transformArgs);
    result->lower = result->lower->transform(transformArgs);
    result->upper = result->upper->transform(transformArgs);
    return result;
}

std::string
BetweenExpression::
getType() const
{
    return "between";
}

Utf8String
BetweenExpression::
getOperation() const
{
    return Utf8String();
}

std::vector<std::shared_ptr<SqlExpression> >
BetweenExpression::
getChildren() const
{
    return { expr, lower, upper };
}

/*****************************************************************************/
/* IN EXPRESSION                                                        */
/*****************************************************************************/

InExpression::
InExpression(std::shared_ptr<SqlExpression> expr,
             std::shared_ptr<TupleExpression> tuple,
             bool negative)
    : expr(std::move(expr)),
      tuple(std::move(tuple)),
      isnegative(negative)
{
}

InExpression::
InExpression(std::shared_ptr<SqlExpression> expr,
             std::shared_ptr<SelectSubtableExpression> subtable,
             bool negative)
: expr(std::move(expr)),
      subtable(std::move(subtable)),
      isnegative(negative)
{

}

BoundSqlExpression
InExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundExpr  = expr->bind(context);

    if (subtable)
    {
        BoundTableExpression boundTable = subtable->bind(context);

        // TODO: we need to detect a correlated subquery
        bool correlatedSubquery = false;

        if (correlatedSubquery) {
            // We re-execute on each call, since the results change
            throw HttpReturnException(500, "Correlated subqueries not supported yet");
        }
        else {
            // POTENTIAL OPT: a subquery with no GROUP BY could be run directly
            // without binding, avoiding the need to create a subtable.

            // non-corelated subquery; we can execute the subquery once and
            // for all.  We do this by getting the first column and making
            // it into a set

            static const SelectExpression select = SelectExpression::parse("*");
            static const WhenExpression when = WhenExpression::parse("true");
            static const OrderByExpression orderBy;
            static const std::shared_ptr<SqlExpression> where = SqlExpression::parse("true");
            ssize_t offset = 0;
            ssize_t limit = -1;

            BasicRowGenerator generator
                = boundTable.table.runQuery(context, SelectExpression::STAR,
                                            WhenExpression::TRUE,
                                            *SqlExpression::TRUE,
                                            orderBy,
                                            offset, limit, true /* allowParallel */);
            
            // This is a set of all values we can search for in our expression
            auto valsPtr = std::make_shared<std::unordered_set<ExpressionValue> >();

            // Generate all outputs of the query
            std::vector<NamedRowValue> rowOutputs = generator(-1);
            
            // Scan them to add to our set
            for (auto & row: rowOutputs) {
                for (auto & col: row.columns) {
                    const ExpressionValue & val = std::get<1>(col);
                    if (!val.empty())
                        valsPtr->insert(val);
                }
            }

            auto exec = [=] (const SqlRowScope & rowScope,
                             ExpressionValue & storage) -> const ExpressionValue &
                { 
                    // 1.  What are we looking to see if it's in
                    ExpressionValue vstorage;
                    const ExpressionValue & v = boundExpr(rowScope, vstorage);

                    // 2.  If we have a null, we return a null
                    if (v.empty())
                        return storage = v;
          
                    // 3.  Lookup in our set of values
                    bool found = valsPtr->count(v);

                    // 4.  Return our result
                    return storage = std::move(ExpressionValue(isnegative ? !found : found,
                                                               v.getEffectiveTimestamp()));
                };
            
            return { exec, this, std::make_shared<BooleanValueInfo>() };
        }
    }
    else
    {
        // We have an explicit tuple, not a subquery.

        std::vector<BoundSqlExpression> tupleExpressions;
        tupleExpressions.reserve(tuple->clauses.size());

        for (auto & tupleItem: tuple->clauses) {
            tupleExpressions.emplace_back(tupleItem->bind(context));
        }

        return {[=] (const SqlRowScope & rowScope,
                     ExpressionValue & storage) -> const ExpressionValue &
        {
            ExpressionValue vstorage, istorage;

            const ExpressionValue & v = boundExpr(rowScope, vstorage);

            if (v.empty())
                return storage = v;

            for (auto item : tupleExpressions)
            {
                const ExpressionValue & itemValue = item(rowScope, istorage);

                if (itemValue.empty())
                    continue;

                if ((v == itemValue))
                {
                    return storage = std::move(ExpressionValue(!isnegative,
                                                           std::max(v.getEffectiveTimestamp(),
                                                                    itemValue.getEffectiveTimestamp())));
                }
            }

            return storage = std::move(ExpressionValue(isnegative, v.getEffectiveTimestamp()));
        },
        this,
        std::make_shared<BooleanValueInfo>()};
    }
}

Utf8String
InExpression::
print() const
{
    Utf8String result
        = "in(\""
        + expr->print()
        + ","
        + (subtable ? subtable->print() : tuple->print())
        + ")";
    return result;
}

std::shared_ptr<SqlExpression>
InExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<InExpression>(*this);
    result->expr  = result->expr->transform(transformArgs);

    if (subtable)
        result->subtable = std::make_shared<SelectSubtableExpression>(*(result->subtable));
    else
        result->tuple = std::make_shared<TupleExpression>(result->tuple->transform(transformArgs));

    return result;
}

std::string
InExpression::
getType() const
{
    return "in";
}

Utf8String
InExpression::
getOperation() const
{
    return Utf8String();
}

std::vector<std::shared_ptr<SqlExpression> >
InExpression::
getChildren() const
{
    std::vector<std::shared_ptr<SqlExpression> > childrens;

    childrens.emplace_back(std::move(expr));

    if (tuple)
    {
        childrens.insert(childrens.end(), tuple->clauses.begin(), tuple->clauses.end());
    }

    return childrens;
}

/*****************************************************************************/
/* CAST EXPRESSION                                                           */
/*****************************************************************************/

CastExpression::
CastExpression(std::shared_ptr<SqlExpression> expr,
               std::string type)
    : expr(std::move(expr)), type(std::move(type))
{
    boost::algorithm::to_lower(type);
}

BoundSqlExpression
CastExpression::
bind(SqlBindingScope & context) const
{
    BoundSqlExpression boundExpr  = expr->bind(context);

    if (type == "string") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage);
                    return storage = std::move(ExpressionValue(val.coerceToString(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<StringValueInfo>()};
    }
    else if (type == "integer") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage);
                    return storage = std::move(ExpressionValue(val.coerceToInteger(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<IntegerValueInfo>()};
    }
    else if (type == "number") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage);
                    return storage = std::move(ExpressionValue(val.coerceToNumber(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<Float64ValueInfo>()};
    }
    else if (type == "timestamp") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage);
                    return storage = std::move(ExpressionValue(val.coerceToTimestamp(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<IntegerValueInfo>()};
    }
    else if (type == "boolean") {
        return {[=] (const SqlRowScope & row,
                     ExpressionValue & storage) -> const ExpressionValue &
                {
                    ExpressionValue valStorage;
                    const ExpressionValue & val = boundExpr(row, valStorage);
                    return storage = std::move(ExpressionValue(val.coerceToBoolean(),
                                                               val.getEffectiveTimestamp()));
                },
                this,
                std::make_shared<BooleanValueInfo>()};
    }
    else throw HttpReturnException(400, "Unknown type '" + type
                                   + "' for CAST (" + expr->surface
                                   + " AS " + type + ")");
}

Utf8String
CastExpression::
print() const
{
    Utf8String result
        = "cast(\""
        + expr->print()
        + ",\""
        + type
        + "\")";
    return result;
}

std::shared_ptr<SqlExpression>
CastExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<CastExpression>(*this);
    result->expr  = result->expr->transform(transformArgs);
    return result;
}

std::string
CastExpression::
getType() const
{
    return "cast";
}

Utf8String
CastExpression::
getOperation() const
{
    return Utf8String(type);
}

std::vector<std::shared_ptr<SqlExpression> >
CastExpression::
getChildren() const
{
    return { expr };
}


/*****************************************************************************/
/* BOUND PARAMETER EXPRESSION                                                */
/*****************************************************************************/

BoundParameterExpression::
BoundParameterExpression(Utf8String paramName)
    : paramName(std::move(paramName))
{
}

BoundSqlExpression
BoundParameterExpression::
bind(SqlBindingScope & context) const
{
    auto getParam = context.doGetBoundParameter(paramName);

    if (!getParam.info) {
        throw HttpReturnException(400, "context " + ML::type_name(context)
                            + " getBoundParameter '" + paramName
                            + "' didn't return info");
    }

    return {[=] (const SqlRowScope & row, ExpressionValue & storage) -> const ExpressionValue &
            {
                return getParam(row, storage);
            },
            this,
            getParam.info};
}

Utf8String
BoundParameterExpression::
print() const
{
    return "param(\"" + paramName + "\")";
}

std::map<Utf8String, UnboundVariable>
BoundParameterExpression::
parameterNames() const
{
    return { { paramName, { std::make_shared<AnyValueInfo>() } } };
}

std::shared_ptr<SqlExpression>
BoundParameterExpression::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<BoundParameterExpression>(*this);
    return result;
}

std::string
BoundParameterExpression::
getType() const
{
    return "param";
}

Utf8String
BoundParameterExpression::
getOperation() const
{
    return paramName;
}

std::vector<std::shared_ptr<SqlExpression> >
BoundParameterExpression::
getChildren() const
{
    return {};
}

/*****************************************************************************/
/* WILDCARD EXPRESSION                                                       */
/*****************************************************************************/

WildcardExpression::
WildcardExpression(Utf8String tableName,
                   Utf8String prefix,
                   Utf8String asPrefix,
                   std::vector<std::pair<Utf8String, bool> > excluding)
    : tableName(tableName), prefix(prefix), asPrefix(asPrefix),
      excluding(excluding)
{
}

BoundSqlExpression
WildcardExpression::
bind(SqlBindingScope & context) const
{
    // This function figures out the new name of the column.  If it's excluded,
    // then it returns the empty string
    auto newColumnName = [&] (const Utf8String & inputColumnName) -> Utf8String
        {
            // First, check it matches the prefix
            if (!inputColumnName.startsWith(prefix))
                return Utf8String();

            // Second, check it doesn't match an exclusion
            for (auto & ex: excluding) {
                if (ex.second) {
                    // prefix
                    if (inputColumnName.startsWith(ex.first))
                        return Utf8String();
                }
                else {
                    // exact match
                    if (inputColumnName == ex.first)
                        return Utf8String();
                }
            }

            // Finally, replace the prefix with the new prefix
            Utf8String result = inputColumnName;
            result.replace(0, prefix.length(), asPrefix);
            return result;
        };

    auto allColumns = context.doGetAllColumns(tableName, newColumnName);

    auto exec = [=] (const SqlRowScope & scope,
                     ExpressionValue & storage)
        -> const ExpressionValue &
        {
            return storage = std::move(allColumns.exec(scope));
        };

    BoundSqlExpression result(exec, this, allColumns.info);

    return result;
}

Utf8String
WildcardExpression::
print() const
{
    Utf8String result = "columns(\"" + tableName + "\",\"" + prefix + "\",\"" + asPrefix + "\",[";

    bool first = true;
    for (auto ex: excluding) {
        if (!first) {
            result += ",";
        }
        first = false;
        if (ex.second)
            result += "wildcard(\"" + ex.first + "\")";
        else 
            result += "column(\"" + ex.first + "\")";
    }
    result += "])";

    return result;
}

std::shared_ptr<SqlExpression>
WildcardExpression::
transform(const TransformArgs & transformArgs) const
{
    return std::make_shared<WildcardExpression>(*this);
}

std::vector<std::shared_ptr<SqlExpression> >
WildcardExpression::
getChildren() const
{
    // tough to do without a context...
    return {};
}

std::map<ScopedName, UnboundWildcard>
WildcardExpression::
wildcards() const
{
    std::map<ScopedName, UnboundWildcard> result;
    result[{tableName, prefix + "*"}].prefix = prefix;
    return result;
}

bool
WildcardExpression::
isIdentitySelect(SqlExpressionDatasetContext & context) const
{
    // A select * is identified like this
    return prefix.empty() && asPrefix.empty() && excluding.empty() && (tableName.empty() || context.childaliases.empty());
}


/*****************************************************************************/
/* COMPUTED VARIABLE                                                         */
/*****************************************************************************/

ComputedVariable::
ComputedVariable(Utf8String alias,
                 std::shared_ptr<SqlExpression> expression)
    : alias(alias),
      expression(expression)
{
}

BoundSqlExpression
ComputedVariable::
bind(SqlBindingScope & context) const
{
    auto exprBound = expression->bind(context);

    if (!exprBound.info)
        cerr << "expression didn't return info: " << expression->print() << endl;
    ExcAssert(exprBound.info);

    //cerr << "binding " << print() << " surface " << surface << " " << jsonEncode(exprBound.info)
    //<< endl;

    if (alias.empty()) {
        // This must be a row-returning function, and we merge the row with the
        // output.  Extract what the row is producing to be merged.
        // MLDB-763

        auto info = exprBound.info;
     
        auto exec = [=] (const SqlRowScope & context,
                         ExpressionValue & storage)
            -> const ExpressionValue &
            {
                const ExpressionValue & val = exprBound(context, storage);

                if (!val.isRow())
                    throw HttpReturnException(400, "Expression with AS * must return a row",
                                              "valueReturned", val,
                                              "ast", print(),
                                              "surface", surface);
                return val;
            };

        BoundSqlExpression result(exec, this, info);
        return result;
    }
    else {
        ColumnName aliasCol(alias);

        auto exec = [=] (const SqlRowScope & context,
                         ExpressionValue & storage)
            -> const ExpressionValue &
            {
                const ExpressionValue & val = exprBound(context, storage);

                if (&val == &storage) {
                    // We own the only copy; we can move it
                    StructValue row;
                    row.emplace_back(aliasCol, std::move(storage));
                    return storage = std::move(ExpressionValue(row));
                }
                else {
                    // We got a reference; copy it
                    StructValue row;
                    row.emplace_back(aliasCol, val);
                    return storage = std::move(ExpressionValue(row));
                }
            };

        std::vector<KnownColumn> knownColumns = {
            KnownColumn(aliasCol, exprBound.info, COLUMN_IS_DENSE) };
        
        auto info = std::make_shared<RowValueInfo>(knownColumns, SCHEMA_CLOSED);

        BoundSqlExpression result(exec, this, info);
        
        return result;
    }
}

Utf8String
ComputedVariable::
print() const
{
    return "computed(\"" + alias + "\"," + expression->print() + ")";
}

std::shared_ptr<SqlExpression>
ComputedVariable::
transform(const TransformArgs & transformArgs) const
{
    auto result = std::make_shared<ComputedVariable>(*this);
    result->expression = transformArgs({ expression}).at(0);
    return result;
}

std::vector<std::shared_ptr<SqlExpression> >
ComputedVariable::
getChildren() const
{
    return { expression };
}

/*****************************************************************************/
/* SELECT COLUMN EXPRESSION                                                  */
/*****************************************************************************/

SelectColumnExpression::
SelectColumnExpression(std::shared_ptr<SqlExpression> select,
                       std::shared_ptr<SqlExpression> as,
                       std::shared_ptr<SqlExpression> where,
                       OrderByExpression orderBy,
                       int64_t offset,
                       int64_t limit)
    : select(std::move(select)),
      as(std::move(as)),
      where(std::move(where)),
      orderBy(std::move(orderBy)),
      offset(offset),
      limit(limit)
{
}

BoundSqlExpression
SelectColumnExpression::
bind(SqlBindingScope & context) const
{
    // 1.  Get all columns
    auto allColumns = context.doGetAllColumns(Utf8String(""),
                                              [] (const Utf8String & name) { return name; });

    // Only known columns are kept.  For each one, we filter it then calculate the
    // order by expression.

    struct ColumnEntry {
        ColumnName inputColumnName;
        ColumnName columnName;
        ColumnHash columnHash;
        int columnNumber;
        std::vector<ExpressionValue> sortFields;
        std::shared_ptr<ExpressionValueInfo> valueInfo;
        ColumnSparsity sparsity;
    };

    std::vector<ColumnEntry> columns;

    // Bind those expressions that operate in the column context
    ColumnExpressionBindingContext colContext(context);

    auto boundWhere = where->bind(colContext);
    auto boundAs = as->bind(colContext);

    std::vector<BoundSqlExpression> boundOrderBy;
    for (auto & o: orderBy.clauses)
        boundOrderBy.emplace_back(o.first->bind(colContext));

    /// List of all functions to run in our run() operator
    std::vector<std::function<void (const SqlRowScope &, StructValue &)> > functionsToRun;

    std::vector<KnownColumn> knownColumns = allColumns.info->getKnownColumns();
   

    // For each group of columns, find which match
    for (unsigned j = 0;  j < knownColumns.size();  ++j) {
        const auto & col = knownColumns[j];

        const ColumnName & columnName = col.columnName;
            
        auto thisContext = colContext.getColumnContext(columnName);

        bool keep = boundWhere(thisContext).isTrue();
        if (!keep)
            continue;

        Utf8String newColName = boundAs(thisContext).toUtf8String();

        vector<ExpressionValue> orderBy;
        for (auto & c: boundOrderBy) {
            orderBy.emplace_back(c(thisContext));
        }

        ColumnEntry entry;
        entry.inputColumnName = columnName;
        entry.columnName = ColumnName(newColName);
        entry.columnHash = entry.columnName;
        entry.columnNumber = j;
        entry.sortFields = std::move(orderBy);
        entry.valueInfo = col.valueInfo;
        entry.sparsity = col.sparsity;

        columns.emplace_back(std::move(entry));
    }

    //cerr << "considering " << columns.size() << " columns" << endl;
    
    // Compare two columns according to the sort criteria
    auto compareColumns = [&] (const ColumnEntry & col1,
                               const ColumnEntry & col2)
        {
            for (unsigned i = 0;  i < boundOrderBy.size();  ++i) {
                const ExpressionValue & e1 = col1.sortFields[i];
                const ExpressionValue & e2 = col2.sortFields[i];
                int cmp = e1.compare(e2);
                //ExcAssertEqual(e1.compare(e1), 0);
                //ExcAssertEqual(e2.compare(e2), 0);
                //ExcAssertEqual(e2.compare(e1), -cmp);
                //cerr << "i = " << i << " cmp = " << cmp << " val1 = "
                //     << e1 << " val2 = " << e2 << endl;
                if (orderBy.clauses[i].second == DESC)
                    cmp *= -1;
                if (cmp == -1)
                    return true;
                else if (cmp == 1)
                    return false;
                //ExcAssertEqual(cmp, 0);
            }

            // No ordering otherwise
            return false;
            //return col1.second.colHash < col2.second.colHash;
        };

    std::sort(columns.begin(), columns.end(), compareColumns);

    for (unsigned i = 0;  i < 10 && i < columns.size();  ++i) {
        cerr << "column " << i << " name " << columns[i].columnName
             << " sort " << jsonEncodeStr(columns[i].sortFields) << endl;
    }

    // Now apply the windowing functions
    ssize_t offset = this->offset;
    ssize_t limit = this->limit;

    ExcAssertGreaterEqual(offset, 0);
    if (offset > columns.size())
        offset = columns.size();

    columns.erase(columns.begin(), columns.begin() + offset);
    
    ExcAssertGreaterEqual(limit, -1);
    if (limit == -1 || limit > columns.size())
        limit = columns.size();

    columns.erase(columns.begin() + limit, columns.end());

    cerr << "restricted set of columns has " << columns.size() << " entries" << endl;

    std::unordered_map<Utf8String, Utf8String> keepColumns;
    for (auto & c: columns)
        keepColumns[c.inputColumnName.toUtf8String()] = c.columnName.toUtf8String();
    
    auto filterColumns = [=] (const Utf8String & name) -> Utf8String
        {
            auto it = keepColumns.find(name);
            if (it == keepColumns.end())
                return Utf8String();
            return it->second;
        };
    
    // Finally, return a filtered set from the underlying dataset
    auto outputColumns
        = context.doGetAllColumns(Utf8String(""), filterColumns);

    auto exec = [=] (const SqlRowScope & context, ExpressionValue & storage)
        -> const ExpressionValue &
        {
            return storage = std::move(outputColumns.exec(context));
        };

    BoundSqlExpression result(exec, this, outputColumns.info);
    
    return result;
}

Utf8String
SelectColumnExpression::
print() const
{
    return "columnExpr("
        + select->print() + ","
        + as->print() + ","
        + where->print() + ","
        + orderBy.print() + ","
        + std::to_string(offset) + ","
        + std::to_string(limit)
        + ")";
}

std::shared_ptr<SqlExpression>
SelectColumnExpression::
transform(const TransformArgs & transformArgs) const
{
    throw HttpReturnException(400, "SelectColumnExpression::transform()");
}

std::vector<std::shared_ptr<SqlExpression> >
SelectColumnExpression::
getChildren() const
{
    // NOTE: these are indirect children, in that they bind their own
    // variables.  So we may not want to return them.  Needs better
    // analysis and testing.
    //
    // If you are trying to debug errors with binding of aggregators
    // or similar functions that look in the expression tree to identify
    // particular constructs, try making this function return nothing
    // and see if that fixes the problem.  If that is the case, then
    // we will probably need to revisit the getChildren() and
    // unbound entity detection logic to differentiate between things
    // that are bound internally versus unbound externally.

    std::vector<std::shared_ptr<SqlExpression> > result;
    
    auto add = [&] (std::vector<std::shared_ptr<SqlExpression> > exprs)
        {
            result.insert(result.end(),
                          std::make_move_iterator(exprs.begin()),
                          std::make_move_iterator(exprs.end()));
        };

    add(select->getChildren());
    add(as->getChildren());
    add(where->getChildren());
    for (auto & c: orderBy.clauses)
        add(c.first->getChildren());

    return result;
}


} // namespace MLDB
} // namespace Datacratic


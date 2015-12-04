// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** builtin_aggregators.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Builtin aggregators for SQL.
*/

#include "sql_expression.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/utils/csv.h"
#include "mldb/types/vector_description.h"
#include <array>


using namespace std;


namespace Datacratic {
namespace MLDB {
namespace Builtins {

typedef BoundAggregator (&BuiltinAggregator) (const std::vector<BoundSqlExpression> &);

struct RegisterAggregator {
    template<typename... Names>
    RegisterAggregator(const BuiltinAggregator & aggregator, Names&&... names)
    {
        doRegister(aggregator, std::forward<Names>(names)...);
    }

    void doRegister(const BuiltinAggregator & aggregator)
    {
    }

    template<typename... Names>
    void doRegister(const BuiltinAggregator & aggregator, std::string name,
                    Names&&... names)
    {
        auto fn = [&] (const Utf8String & str,
                       const std::vector<BoundSqlExpression> & args,
                       const SqlBindingScope & context)
            -> BoundAggregator
            {
                return std::move(aggregator(args));
            };
        handles.push_back(registerAggregator(Utf8String(name), fn));
        doRegister(aggregator, std::forward<Names>(names)...);
    }

    std::vector<std::shared_ptr<void> > handles;
};

template<typename State>
struct AggregatorT {
    /** This is the function that is actually called when we want to use
        an aggregator.  It meets the interface used by the normal aggregator
        registration functionality.
    */
    static BoundAggregator entry(const std::vector<BoundSqlExpression> & args)
    {
        // These can only take a single argument
        ExcAssertEqual(args.size(), 1);
        ExcAssert(args[0].info);

        if (args[0].info->isRow()) {
            return enterRow(args);
        }
        else if (args[0].info->isScalar()) {
            return enterScalar(args);
        }
        else {
            return enterAmbiguous(args);
        }
    }

    //////// Scalar  ///////////

    static std::shared_ptr<State> scalarInit()
    {
        return std::make_shared<State>();
    }

    static void scalarProcess(const ExpressionValue * args,
                              size_t nargs,
                              void * data)
    {
        State * state = static_cast<State *>(data);
        state->process(args, nargs);
    }

    static ExpressionValue scalarExtract(void * data)
    {
        State * state = static_cast<State *>(data);
        return state->extract();
    }

    static void scalarMerge(void* dest, void* src)
    {
        State * state = static_cast<State *>(dest);
        State * srcState = static_cast<State *>(src);
        state->merge(srcState);
    }
    
    /** Entry point for when we are called with the first argument as a scalar.
        This does a normal SQL aggregation.
    */
    static BoundAggregator enterScalar(const std::vector<BoundSqlExpression> & args)
    {
        return { scalarInit, scalarProcess, scalarExtract, scalarMerge, State::info(args) };
    }

    //////// Row ///////////

    /** Structure used to keep the state when in row mode.  It keeps a separate
        state for each of the columns.
    */
    struct RowState {
        std::map<ColumnName, State> columns;

        static std::shared_ptr<ExpressionValueInfo>
        info(const std::vector<BoundSqlExpression> & args)
        {
            ExcAssertEqual(args.size(), 1);
            ExcAssert(args[0].info);

            // Create a value info object for the output.  It has the same
            // shape as the input row, but the field type is whatever the
            // value info provides.
            auto outputColumnInfo = State::info(args);

            auto cols = args[0].info->getKnownColumns();
            SchemaCompleteness hasUnknown = args[0].info->getSchemaCompleteness();

            // For each known column, give the output type
            for (KnownColumn & c: cols)
                c.valueInfo = outputColumnInfo;

            std::sort(cols.begin(), cols.end(),
                      [] (const KnownColumn & c1, const KnownColumn & c2)
                      {
                          return c1.columnName < c2.columnName;
                      });
            
            return std::make_shared<RowValueInfo>(cols, hasUnknown);
        }

        void process(const ExpressionValue * args, size_t nargs)
        {
            ExcAssertEqual(nargs, 1);
            const ExpressionValue & val = args[0];

            // This must be a row...
            auto onSubExpression = [&] (const Id & columnName,
                                        const Id & prefix,
                                        const ExpressionValue & val)
                {
                    columns[columnName].process(&val, 1);
                    return true;
                };
            
            val.forEachSubexpression(onSubExpression);
        }

        ExpressionValue extract()
        {
            StructValue result;

            for (auto & v: columns) {
                result.emplace_back(v.first, v.second.extract());
            }

            return ExpressionValue(std::move(result));
        }

        void merge(RowState* from)
        {
            for (auto & v: from->columns) {
                columns[v.first].merge(&v.second);
            }
        }
    };

    static std::shared_ptr<RowState> rowInit()
    {
        return std::make_shared<RowState>();
    }
    
    static void rowProcess(const ExpressionValue * args,
                           size_t nargs,
                           void * data)
    {
        RowState * state = static_cast<RowState *>(data);
        state->process(args, nargs);
    }

    static void rowMerge(void* dest, void* src)
    {
        RowState * state = static_cast<RowState *>(dest);
        RowState * srcState = static_cast<RowState *>(src);
        state->merge(srcState);
    }

    static ExpressionValue rowExtract(void * data)
    {
        RowState * state = static_cast<RowState *>(data);
        return state->extract();
    }

    /** Entry point for when we are called with the first argument returning a
        row.  This does an aggregation per column in the row.
    */
    static BoundAggregator enterRow(const std::vector<BoundSqlExpression> & args)
    {
        return { rowInit, rowProcess, rowExtract, rowMerge, RowState::info(args) };
    }

    //////// Ambiguous ///////////
    struct AmbiguousState
    {
        AmbiguousState() { isDetermined = false; isRow = false;}

        RowState rowState;
        State    scalarState;
        bool     isDetermined;
        bool     isRow;

    };

    static std::shared_ptr<AmbiguousState> ambiguousStateInit()
    {
        return std::make_shared<AmbiguousState>();
    }
    
    static void ambiguousProcess(const ExpressionValue * args,
                           size_t nargs,
                           void * data)
    {
        AmbiguousState * state = static_cast<AmbiguousState *>(data);

        if (!state->isDetermined) {
            state->isDetermined = true;
            ExcAssertEqual(nargs, 1);
            state->isRow = args[0].isRow();
        }

        if (state->isRow)
            state->rowState.process(args, nargs);
        else
            state->scalarState.process(args, nargs);
       
    }

    static ExpressionValue ambiguousExtract(void * data)
    {
        AmbiguousState * state = static_cast<AmbiguousState *>(data);
        ExcAssert(state->isDetermined);

         if (state->isRow)
                return state->rowState.extract();
         else
                return state->scalarState.extract();
    }

    static void ambiguousMerge(void* dest, void* src)
    {
        AmbiguousState * state = static_cast<AmbiguousState *>(dest);
        AmbiguousState * srcState = static_cast<AmbiguousState *>(src);

        if (srcState->isDetermined)
        {
            if (srcState->isRow)
                state->rowState.merge(&srcState->rowState);
            else
                srcState->scalarState.merge(&srcState->scalarState);

            state->isRow = srcState->isRow;
            state->isDetermined = true;
        }        
    }

    /** Entry point where we don't know wheter the arguqment is a row or a scalar
        will be determined on the first row aggregated
    */
    static BoundAggregator enterAmbiguous(const std::vector<BoundSqlExpression> & args)
    {
        return { ambiguousStateInit, ambiguousProcess, ambiguousExtract, ambiguousMerge, std::make_shared<AnyValueInfo>() };
    }
};

template<typename Accum>
struct RegisterAggregatorT: public RegisterAggregator {
    template<typename... Names>
    RegisterAggregatorT(Names&&... names)
        : RegisterAggregator(AggregatorT<Accum>::entry, std::forward<Names>(names)...)
    {
    }
};

struct AverageAccum {
    AverageAccum()
        : total(0.0), n(0.0), ts(Date::negativeInfinity())
    {
    }

    static std::shared_ptr<ExpressionValueInfo>
    info(const std::vector<BoundSqlExpression> & args)
    {
        return std::make_shared<Float64ValueInfo>();
    }

    void process(const ExpressionValue * args, size_t nargs)
    {
        ExcAssertEqual(nargs, 1);
        const ExpressionValue & val = args[0];
        if (val.empty())
            return;
        total += val.toDouble();
        n += 1;
        ts.setMax(val.getEffectiveTimestamp());
    }
     
    ExpressionValue extract()
    {
        return ExpressionValue(total / n, ts);
    }

    void merge(AverageAccum* from)
    {
        total += from->total;
        n += from->n;
        ts.setMax(from->ts);
    }
    
    double total;
    double n;
    Date ts;
};
        
static RegisterAggregatorT<AverageAccum> registerAvg("avg");

template<typename Op, int Init>
struct ValueAccum {
    ValueAccum()
        : value(Init), ts(Date::negativeInfinity())
    {
    }

    static std::shared_ptr<ExpressionValueInfo>
    info(const std::vector<BoundSqlExpression> & args)
    {
        return std::make_shared<Float64ValueInfo>();
    }

    void process(const ExpressionValue * args, size_t nargs)
    {
        ExcAssertEqual(nargs, 1);
        const ExpressionValue & val = args[0];
        if (val.empty())
            return;
        value = Op()(value, val.toDouble());
        ts.setMax(val.getEffectiveTimestamp());
    }
     
    ExpressionValue extract()
    {
        return ExpressionValue(value, ts);
    }

    void merge(ValueAccum* src)
    {
        value = Op()(value, src->value);
        ts.setMax(src->ts);
    }

    double value;
    Date ts;
};

static RegisterAggregatorT<ValueAccum<std::plus<double>, 0> > registerSum("sum");

template<typename Cmp>
struct MinMaxAccum {
    MinMaxAccum()
        : first(true), ts(Date::negativeInfinity())
    {
    }

    static std::shared_ptr<ExpressionValueInfo>
    info(const std::vector<BoundSqlExpression> & args)
    {
        return args[0].info;
    }

    void process(const ExpressionValue * args,
                 size_t nargs)
    {
        ExcAssertEqual(nargs, 1);
        const ExpressionValue & val = args[0];
        //cerr << "processing " << jsonEncode(val) << endl;
        if (val.empty())
            return;
        if (first) {
            value = val.getAtom();
            first = false;
            ts = val.getEffectiveTimestamp();
        }
        else {
            auto atom = val.getAtom();
            if (Cmp()(atom, value))
                value = atom;
            ts.setMax(val.getEffectiveTimestamp());
        }
        //cerr << "ts now " << ts << endl;
    }
    
    ExpressionValue extract()
    {
        return ExpressionValue(value, ts);
    }

    void merge(MinMaxAccum* src)
    {
        if (first) {
            value = src->value;
            first = src->first;
        } 
        else if (!src->first && Cmp()(src->value, value)) {
            value = src->value;
        }
        ts.setMax(src->ts);
    }

    bool first;
    CellValue value;
    Date ts;
};

static RegisterAggregatorT<MinMaxAccum<std::less<CellValue> > > registerMin("min");
static RegisterAggregatorT<MinMaxAccum<std::greater<CellValue> > > registerMax("max");

struct CountAccum {
    CountAccum()
        : n(0), ts(Date::negativeInfinity())
    {
    }

    static std::shared_ptr<ExpressionValueInfo>
    info(const std::vector<BoundSqlExpression> & args)
    {
        return std::make_shared<IntegerValueInfo>();
    }

    void process (const ExpressionValue * args,
                  size_t nargs)
    {
        ExcAssertEqual(nargs, 1);
        const ExpressionValue & val = args[0];
        if (val.empty())
            return;
        n += 1;
        ts.setMax(val.getEffectiveTimestamp());
    };

    ExpressionValue extract()
    {
        return ExpressionValue(n, ts);
    }

    void merge(CountAccum* src)
    {
        n += src->n;
        ts.setMax(src->ts);
    }
            
    uint64_t n;
    Date ts;
};

static RegisterAggregatorT<CountAccum> registerCount("count");

struct LikelihoodRatioAccum {
    LikelihoodRatioAccum()
        : ts(Date::negativeInfinity())
    {
    }
            
    std::array<uint64_t, 2> n;
    Date ts;
    std::unordered_map<ColumnName, std::array<uint64_t, 2> > counts;
};

BoundAggregator lr(const std::vector<BoundSqlExpression> & args)
{
    auto init = [] () -> std::shared_ptr<void>
        {
            return std::make_shared<LikelihoodRatioAccum>();
        };

    auto process = [] (const ExpressionValue * args,
                       size_t nargs,
                       void * data)
        {
            ExcAssertEqual(nargs, 2);
            const ExpressionValue & val = args[0];
            bool conv = args[1].isTrue();
            LikelihoodRatioAccum & accum = *(LikelihoodRatioAccum *)data;
            // This must be a row...
            auto onAtom = [&] (const Id & columnName,
                               const Id & prefix,
                               const CellValue & val,
                               Date ts)
            {
                accum.counts[columnName][conv] += 1;
                accum.ts.setMax(ts);
                return true;
            };

            val.forEachAtom(onAtom);
            
            accum.n[conv] += 1;
        };

    auto extract = [] (void * data) -> ExpressionValue
        {
            LikelihoodRatioAccum & accum = *(LikelihoodRatioAccum *)data;

            RowValue result;
            for (auto & v: accum.counts) {
                double cnt_false = v.second[0];
                double cnt_true = v.second[1];

                double r_false = cnt_false / accum.n[0];
                double r_true = cnt_true / accum.n[1];

                double lr = log(r_true / r_false);

                result.emplace_back(v.first, lr, accum.ts);
            }

            return ExpressionValue(std::move(result));
        };

     auto merge = [] (void * data, void* src)
        {
            LikelihoodRatioAccum & accum = *(LikelihoodRatioAccum *)data;
            LikelihoodRatioAccum & srcAccum = *(LikelihoodRatioAccum *)src;

            for (auto &iter : srcAccum.counts)
            {
                for (int conv = 0; conv < 2; ++conv)
                {
                    accum.counts[iter.first][conv] += iter.second[conv];
                }                
            }

            for (int conv = 0; conv < 2; ++conv)
            {
                accum.n[conv] += srcAccum.n[conv];
            } 

            accum.ts.setMax(srcAccum.ts);
        };


    return { init, process, extract, merge };
}

static RegisterAggregator registerLikelihoodRatio(lr, "likelihood_ratio");


struct PivotAccum {
    PivotAccum()
    {
    }

    StructValue vals;
};

BoundAggregator pivot(const std::vector<BoundSqlExpression> & args)
{
    auto init = [] () -> std::shared_ptr<void>
        {
            return std::make_shared<PivotAccum>();
        };

    auto process = [] (const ExpressionValue * args,
                       size_t nargs,
                       void * data)
        {
            PivotAccum & accum = *(PivotAccum *)data;

            ExcAssertEqual(nargs, 2);
            const ExpressionValue & col = args[0];
            const ExpressionValue & val = args[1];

            ColumnName columnName(col.toUtf8String());

            accum.vals.emplace_back(columnName, val);
        };

    auto extract = [] (void * data) -> ExpressionValue
        {
            PivotAccum & accum = *(PivotAccum *)data;

            return ExpressionValue(std::move(accum.vals));

        };

     auto merge = [] (void * data, void* src)
        {
            PivotAccum & accum = *(PivotAccum *)data;
            PivotAccum & srcAccum = *(PivotAccum *)src;

            for (auto& c : srcAccum.vals)
            {
                accum.vals.emplace_back(std::move(c));
            }
        };

    return { init, process, extract, merge };
}

static RegisterAggregator registerPivot(pivot, "pivot");

} // namespace Builtins
} // namespace MLDB
} // namespace Datacratic


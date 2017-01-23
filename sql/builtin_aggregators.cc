/** builtin_aggregators.cc
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Builtin aggregators for SQL.
*/

#include "sql_expression.h"
#include "builtin_functions.h"
#include "mldb/http/http_exception.h"
#include "mldb/jml/stats/distribution.h"
#include "mldb/jml/utils/csv.h"
#include "mldb/types/vector_description.h"
#include "mldb/base/optimized_path.h"
#include <array>
#include <unordered_set>

using namespace std;



namespace MLDB {
namespace Builtins {

typedef BoundAggregator (&BuiltinAggregator) (const std::vector<BoundSqlExpression> &,
                                              const string & name);

struct RegisterAggregator {
    template<typename... Names>
    RegisterAggregator(BuiltinAggregator aggregator, Names&&... names)
    {
        doRegister(aggregator, std::forward<Names>(names)...);
    }

    void doRegister(BuiltinAggregator aggregator)
    {
    }

    template<typename... Names>
    void doRegister(BuiltinAggregator aggregator, std::string name,
                    Names&&... names)
    {
        auto fn = [&aggregator, name] (const Utf8String & str,
                       const std::vector<BoundSqlExpression> & args,
                       SqlBindingScope & context)
            -> BoundAggregator
            {
                return aggregator(args, name);
            };
        handles.push_back(registerAggregator(Utf8String(name), fn));
        doRegister(aggregator, std::forward<Names>(names)...);
    }

    std::vector<std::shared_ptr<void> > handles;
};

/// Allow control over whether the given optimization path is run
/// so that we can test both with and without optimization.
static const OptimizedPath optimizeDenseAggregators
("mldb.sql.optimizeDenseAggregators");


template<typename State>
struct AggregatorT {
    /** This is the function that is actually called when we want to use
        an aggregator.  It meets the interface used by the normal aggregator
        registration functionality.
    */
    static BoundAggregator entry(const std::vector<BoundSqlExpression> & args,
                                 const string & name)
    {
        // These take the number of arguments given in the State class
        checkArgsSize(args.size(), State::nargs, State::maxArgs, name);
        ExcAssert(args[0].info);

        if (args[0].info->isRow()) {
            return enterRow(args, name);
        }
        else if (args[0].info->isScalar()) {
            return enterScalar(args, name);
        }
        else {
            return enterAmbiguous(args, name);
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
    static BoundAggregator enterScalar(const std::vector<BoundSqlExpression> & args,
                                       const string & name)
    {
        return { scalarInit, scalarProcess, scalarExtract, scalarMerge, State::info(args) };
    }

    //////// Row ///////////

    /** Structure used to keep the state when in row mode.  It keeps a separate
        state for each of the columns.
    */
    struct SparseRowState {
        SparseRowState()
        {
        }
        
        std::unordered_map<PathElement, State> columns;

        void process(const ExpressionValue * args, size_t nargs)
        {
            checkArgsSize(nargs, 1);
            const ExpressionValue & val = args[0];

            if (val.empty())
                return;

            // This must be a row...
            auto onColumn = [&] (const PathElement & columnName,
                                 const ExpressionValue & val)
                {
                    columns[columnName].process(&val, 1);
                    return true;
                };

            // will keep only the LATEST of each column (if there are duplicates)
            ExpressionValue storage;
            val.getFiltered(GET_LATEST, storage).forEachColumn(onColumn);
        }

        ExpressionValue extract()
        {
            StructValue result;

            for (auto & v: columns) {
                result.emplace_back(v.first, v.second.extract());
            }

            std::sort(result.begin(), result.end());
            
            return ExpressionValue(std::move(result));
        }

        void merge(SparseRowState* from)
        {
            for (auto & v: from->columns) {
                columns[v.first].merge(&v.second);
            }
        }

    };

    /** Structure used to keep the state when in row mode.  It must be
        passed exactly the same values in exactly the same order from
        invocation to invocation.  It keeps a much cheaper separate
        state for each of the columns.
    */
    struct DenseRowState {
        DenseRowState(const std::vector<PathElement> & columnNames)
            : columnNames(columnNames),
              columnState(columnNames.size())
        {
        }
        
        std::vector<PathElement> columnNames;
        std::vector<State> columnState;

        /// If we guessed wrong about denseness, this is the sparse
        /// state we can fall back on
        std::unique_ptr<SparseRowState> fallback;

        /** Fill in the sparse row state as a fallback, and process
            based upon that for when we have non-uniform columns.
        */
        void pessimize()
        {
            ExcAssert(!fallback.get());
            fallback.reset(new SparseRowState());
            for (unsigned i = 0;  i < columnNames.size();  ++i) {
                fallback->columns.emplace(std::move(columnNames[i]),
                                          std::move(columnState[i]));
            }
            columnNames.clear();
            columnState.clear();
        }

        void process(const ExpressionValue * args, size_t nargs)
        {
            checkArgsSize(nargs, 1);

            if (fallback.get()) {
                fallback->process(args, nargs);
                return;
            }
            const ExpressionValue & val = args[0];

            if (val.empty())
                return;

            // Check if the column names or number don't match, and
            // pessimize back to the sparse version if it's the case.
            bool needToPessimize = columnNames.size() != val.rowLength();

            // Counts how many of the elements we've done so far
            size_t n = 0;

            // Set of values we skipped because we just discovered that
            // we need to pessimize.
            StructValue skipped;

            auto onColumn = [&] (const PathElement & columnName,
                                 const ExpressionValue & val)
                {
                    if (needToPessimize || n > columnNames.size()
                        || columnNames[n] != columnName) {
                        needToPessimize = true;
                        skipped.emplace_back(columnName, val);
                    }
                    else {
                        // Names and number of columns matches.  We can go ahead
                        // and process everything on the fast path.
                        columnState[n].process(&val, 1);
                    }
                    ++n;
                    return true;
                };

            val.forEachColumn(onColumn);

            if (!needToPessimize)
                return;

            // Our list of column names or their order has changed.  Too bad;
            // we'll have to move to a sparse format.
            pessimize();

            // We have processed some rows but not others (those that still
            // need to be processed are in skipped).  Here we pessimize, and
            // then pass in a new value with just the unprocessed ones in it.
            vector<ExpressionValue> newArgs{ std::move(skipped) };

            fallback->process(newArgs.data(), newArgs.size());
        }

        ExpressionValue extract()
        {
            if (fallback.get()) {
                return fallback->extract();
            }

            StructValue result;

            for (unsigned i = 0;  i < columnNames.size();  ++i) {
                result.emplace_back(columnNames[i],
                                    columnState[i].extract());
            }

            std::sort(result.begin(), result.end());

            return ExpressionValue(std::move(result));
        }

        void merge(DenseRowState* from)
        {
            if (from->fallback.get()) {
                if (!fallback.get())
                    pessimize();
                fallback->merge(from->fallback.get());
                return;
            }

            for (unsigned i = 0;  i < columnNames.size();  ++i) {
                columnState[i].merge(&from->columnState[i]);
            }
        }
    };

    static std::shared_ptr<SparseRowState> sparseRowInit()
    {
        return std::make_shared<SparseRowState>();
    }
    
    static void sparseRowProcess(const ExpressionValue * args,
                                 size_t nargs,
                                 void * data)
    {
        SparseRowState * state = static_cast<SparseRowState *>(data);
        state->process(args, nargs);
    }

    static void sparseRowMerge(void* dest, void* src)
    {
        SparseRowState * state = static_cast<SparseRowState *>(dest);
        SparseRowState * srcState = static_cast<SparseRowState *>(src);
        state->merge(srcState);
    }

    static ExpressionValue sparseRowExtract(void * data)
    {
        SparseRowState * state = static_cast<SparseRowState *>(data);
        return state->extract();
    }

    static std::shared_ptr<DenseRowState>
    denseRowInit(const std::vector<PathElement> & columnNames)
    {
        return std::make_shared<DenseRowState>(columnNames);
    }
    
    static void denseRowProcess(const ExpressionValue * args,
                                 size_t nargs,
                                 void * data)
    {
        DenseRowState * state = static_cast<DenseRowState *>(data);
        state->process(args, nargs);
    }

    static void denseRowMerge(void* dest, void* src)
    {
        DenseRowState * state = static_cast<DenseRowState *>(dest);
        DenseRowState * srcState = static_cast<DenseRowState *>(src);
        state->merge(srcState);
    }

    static ExpressionValue denseRowExtract(void * data)
    {
        DenseRowState * state = static_cast<DenseRowState *>(data);
        return state->extract();
    }


    /** Entry point for when we are called with the first argument returning a
        row.  This does an aggregation per column in the row.
    */
    static BoundAggregator
    enterRow(const std::vector<BoundSqlExpression> & args, const string & name)
    {
        // Analyzes the input arguments for a row, and figures out:
        // a) what kind of output will be produced
        // b) what is the best way to implement the query
        // First output: information about the row
        // Second output: is it dense (in other words, all rows are the same)?
        checkArgsSize(args.size(), 1, name);
        ExcAssert(args[0].info);

        // Create a value info object for the output.  It has the same
        // shape as the input row, but the field type is whatever the
        // value info provides.
        auto outputColumnInfo = State::info(args);

        auto cols = args[0].info->getKnownColumns();
        SchemaCompleteness hasUnknown = args[0].info->getSchemaCompleteness();

        // Is this regular (one and only one value)?  If so, then we
        // can be far more optimized about it
        bool isDense = hasUnknown == SCHEMA_CLOSED;

        std::vector<PathElement> denseColumnNames;

        // For each known column, give the output type
        for (KnownColumn & c: cols) {
            if (c.sparsity == COLUMN_IS_SPARSE || c.columnName.size() != 1)
                isDense = false;
            c.valueInfo = outputColumnInfo;
            c.sparsity = COLUMN_IS_DENSE;  // always one for each
            if (isDense) {
                // toSimpleName() is OK, since we just checked it was of length 1
                denseColumnNames.push_back(c.columnName.toSimpleName());
            }
        }

        auto rowInfo = std::make_shared<RowValueInfo>(cols, hasUnknown);

        if (optimizeDenseAggregators(isDense)) {
            // ExpressionValues will always sort their columns, so do so here
            // so we don't just mess up the ordering.
            if (!std::is_sorted(denseColumnNames.begin(),
                                denseColumnNames.end()))
                std::sort(denseColumnNames.begin(),
                          denseColumnNames.end());
            
            // Use an optimized version, assuming everything comes in in the
            // same order as the first row.  We may need to pessimize
            // afterwards
            return { std::bind(denseRowInit, denseColumnNames),
                     denseRowProcess,
                     denseRowExtract,
                     denseRowMerge,
                     rowInfo };
        }
        else {
            // Do it the slow way by looking up keys in maps
            return { sparseRowInit,
                     sparseRowProcess,
                     sparseRowExtract,
                     sparseRowMerge,
                     rowInfo };
        }
    }

    //////// Ambiguous ///////////
    struct AmbiguousState
    {
        AmbiguousState() { isDetermined = false; isRow = false;}

        SparseRowState rowState;
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
            checkArgsSize(nargs, 1);
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
                state->scalarState.merge(&srcState->scalarState);

            state->isRow = srcState->isRow;
            state->isDetermined = true;
        }        
    }

    /** Entry point where we don't know whether the argument is a row or a scalar
        will be determined on the first row aggregated
    */
    static BoundAggregator enterAmbiguous(const std::vector<BoundSqlExpression> & args,
                                          const string & name)
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
    static constexpr int nargs = 1;
    static constexpr int maxArgs = nargs;
    
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
        checkArgsSize(nargs, 1);
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
        
static RegisterAggregatorT<AverageAccum> registerAvg("avg", "vertical_avg");

template<typename Op, int Init>
struct ValueAccum {
    static constexpr int nargs = 1;
    static constexpr int maxArgs = nargs;
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
        checkArgsSize(nargs, 1);
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

static RegisterAggregatorT<ValueAccum<std::plus<double>, 0> >
registerSum("sum", "vertical_sum");

struct StringAggAccum {
    static constexpr int nargs = 2;
    static constexpr int maxArgs = 3;
    StringAggAccum()
        : ts(Date::negativeInfinity())
    {
    }

    static std::shared_ptr<ExpressionValueInfo>
    info(const std::vector<BoundSqlExpression> & args)
    {
        return std::make_shared<Utf8StringValueInfo>();
    }

    void process(const ExpressionValue * args, size_t nargs)
    {
        if (nargs < 2 || nargs > 3) {
            checkArgsSize(nargs, 2, 3);
        }
        const ExpressionValue & val = args[0];

        if (val.empty())
            return;

        const ExpressionValue & separator = args[1];
        static const CellValue noSort;
        const CellValue & sort
            = nargs > 2 ? args[2].getAtom() : noSort;
        
        values.emplace_back(sort, val.coerceToString().toUtf8String(),
                            separator.empty() 
                            ? Utf8String()
                            : separator.coerceToString().toUtf8String());

        ts.setMax(val.getEffectiveTimestamp());

        if (isSorted && values.size() > 1
            && values[values.size() - 2] > values.back())
            isSorted = false;
    }
     
    ExpressionValue extract()
    {
        if (!isSorted)
            std::sort(values.begin(), values.end());

        Utf8String result;

        for (size_t i = 0;  i < values.size();  ++i) {
            if (i != 0)
                result += std::get<2>(values[i - 1]);
            result += std::get<1>(values[i]);
        }
        
        return ExpressionValue(std::move(result), ts);
    }

    void merge(StringAggAccum* src)
    {
        ts.setMax(src->ts);

        if (src->values.size() > values.size()) {
            values.swap(src->values);
            std::swap(isSorted, src->isSorted);
        }

        if (values.size() < 3 * src->values.size()) {
            if (!isSorted)
                std::sort(values.begin(), values.end());
            if (!src->isSorted)
                std::sort(src->values.begin(), src->values.end());
            size_t before = values.size();
            values.insert(values.end(),
                          std::make_move_iterator(src->values.begin()),
                          std::make_move_iterator(src->values.end()));
            std::inplace_merge(values.begin(), values.begin() + before,
                               values.end());
            isSorted = true;
        }
        else {
            isSorted = isSorted && src->values.empty();
            values.insert(values.end(),
                          std::make_move_iterator(src->values.begin()),
                          std::make_move_iterator(src->values.end()));
        }
    }

    // sort key, value, separator
    std::vector<std::tuple<CellValue, Utf8String, Utf8String> > values;      ///< Currently accumulated values with separators
    bool isSorted = true;   ///< Is values already sorted?
    Date ts;
};

static RegisterAggregatorT<StringAggAccum>
registerStringAgg("string_agg", "vertical_string_agg");

template<typename Cmp>
struct MinMaxAccum {
    static constexpr int nargs = 1;
    static constexpr int maxArgs = nargs;
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
        checkArgsSize(nargs, 1);
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
            if (Cmp()(atom, value)) {
                value = atom;
                ts = val.getEffectiveTimestamp();
            }
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
            ts = src->ts;
        } 
        else if (!src->first && Cmp()(src->value, value)) {
            value = src->value;
            ts = src->ts;
        }
    }

    bool first;
    CellValue value;
    Date ts;
};

static RegisterAggregatorT<MinMaxAccum<std::less<CellValue> > >
registerMin("min", "vertical_min");
static RegisterAggregatorT<MinMaxAccum<std::greater<CellValue> > >
registerMax("max", "vertical_max");

struct CountAccum {
    static constexpr int nargs = 1;
    static constexpr int maxArgs = nargs;
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
        checkArgsSize(nargs, 1);
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

static RegisterAggregatorT<CountAccum> registerCount("count", "vertical_count");

struct DistinctAccum {
    static constexpr int nargs = 1;
    static constexpr int maxArgs = nargs;
    DistinctAccum()
        : ts(Date::negativeInfinity())
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
        checkArgsSize(nargs, 1);
        const ExpressionValue & val = args[0];
        if (val.empty())
            return;

        knownValues.insert(val.getAtom());
        ts.setMax(val.getEffectiveTimestamp());
    };

    ExpressionValue extract()
    {
       return ExpressionValue(knownValues.size(), ts);
    }

    void merge(DistinctAccum* src)
    {
        knownValues.insert(src->knownValues.begin(), src->knownValues.end());
    }
    
    std::unordered_set<CellValue> knownValues;
    Date ts;
};

static RegisterAggregatorT<DistinctAccum> registerDistinct("count_distinct");

struct LikelihoodRatioAccum {
    LikelihoodRatioAccum()
        : ts(Date::negativeInfinity())
    {
    }
            
    std::array<uint64_t, 2> n;
    Date ts;
    std::unordered_map<ColumnPath, std::array<uint64_t, 2> > counts;
};

BoundAggregator lr(const std::vector<BoundSqlExpression> & args,
                   const string & name)
{
    auto init = [] () -> std::shared_ptr<void>
        {
            return std::make_shared<LikelihoodRatioAccum>();
        };

    auto process = [name] (const ExpressionValue * args,
                       size_t nargs,
                       void * data)
        {
            checkArgsSize(nargs, 2, name);
            const ExpressionValue & val = args[0];
            bool conv = args[1].isTrue();
            LikelihoodRatioAccum & accum = *(LikelihoodRatioAccum *)data;
            // This must be a row...
            auto onAtom = [&] (const Path & columnName,
                               const Path & prefix,
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

BoundAggregator pivot(const std::vector<BoundSqlExpression> & args,
                      const string & name)
{
    auto init = [] () -> std::shared_ptr<void>
        {
            return std::make_shared<PivotAccum>();
        };

    auto process = [name] (const ExpressionValue * args,
                       size_t nargs,
                       void * data)
        {
            PivotAccum & accum = *(PivotAccum *)data;

            checkArgsSize(nargs, 2, name);
            const ExpressionValue & col = args[0];
            const ExpressionValue & val = args[1];

            accum.vals.emplace_back(col.toUtf8String(), val);
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

template<typename AccumCmp>
struct EarliestLatestAccum {
    static constexpr int nargs = 1;
    static constexpr int maxArgs = nargs;
    EarliestLatestAccum()
        : value(ExpressionValue::null(AccumCmp::getInitialDate()))
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
        checkArgsSize(nargs, 1);
        const ExpressionValue & val = args[0];
        //cerr << "processing " << jsonEncode(val) << endl;
        if (val.empty())
            return;
        if (AccumCmp::cmp(val, value))
            value = val;
    }
    
    ExpressionValue extract()
    {
        return value;
    }

    void merge(EarliestLatestAccum* src)
    {
        if(AccumCmp::cmp(src->value, value)) {
            value = src->value;
        }
    }

    ExpressionValue value;
};

struct EarlierAccum {
    static bool cmp(const ExpressionValue & left, const ExpressionValue & right) {
        return left.isEarlier(right.getEffectiveTimestamp(), right);
    }
    static Date getInitialDate() { return Date::positiveInfinity(); }
};

struct LaterAccum {
    static bool cmp(const ExpressionValue & left, const ExpressionValue & right) {
        return left.isLater(right.getEffectiveTimestamp(), right);
    }
    static Date getInitialDate() { return Date::negativeInfinity(); }
};

static RegisterAggregatorT<EarliestLatestAccum<EarlierAccum> > registerEarliest("earliest", "vertical_earliest");
static RegisterAggregatorT<EarliestLatestAccum<LaterAccum> > registerLatest("latest", "vertical_latest");

struct VarAccum {
    static constexpr int nargs = 1;
    static constexpr int maxArgs = nargs;
    int64_t n;
    double mean;
    double M2;
    Date ts;

    VarAccum() : n(0), mean(0), M2(0), ts(Date::negativeInfinity())
    {
    }

    static std::shared_ptr<ExpressionValueInfo>
    info(const std::vector<BoundSqlExpression> & args)
    {
        return std::make_shared<NumericValueInfo>();
    }

    void process(const ExpressionValue * args, size_t nargs)
    {
        checkArgsSize(nargs, 1);
        const ExpressionValue & val = args[0];

        if (val.empty()) {
            return;
        }

        ++ n;
        double delta = val.toDouble() - mean;
        mean += delta / n;
        M2 += delta * (val.toDouble() - mean);

        ts.setMax(val.getEffectiveTimestamp());
    }
    
    double variance() const
    {
        if (n < 2)
            return std::nan("");

        return M2 / (n - 1);
    }

    ExpressionValue extract()
    {
        return ExpressionValue(variance(), ts);
    }

    void merge(VarAccum* src)
    {
        double delta = src->mean - mean;
        M2 = M2 + src->M2 + delta * delta * n * src->n / (n + src->n);
        mean = (n * mean + src->n * src->mean) / (n + src->n);
        n += src->n;
        ts.setMax(src->ts);
    }
};

struct StdDevAccum : public VarAccum {

    StdDevAccum(): VarAccum()
    {
    }

    ExpressionValue extract()
    {
        return ExpressionValue(sqrt(variance()), ts);
    }
};


static RegisterAggregatorT<VarAccum> registerVarAgg("variance", "vertical_variance");
static RegisterAggregatorT<StdDevAccum> registerStdDevAgg("stddev", "vertical_stddev");


} // namespace Builtins
} // namespace MLDB



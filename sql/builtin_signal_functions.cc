/** builtin_geo_functions.cc
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Builtin geometric functions for SQL.
*/

#include <numeric>
#include "mldb/sql/builtin_functions.h"
#include "mldb/ext/pffft/pffft.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/scope.h"
#include "mldb/utils/possibly_dynamic_buffer.h"
#include "mldb/http/http_exception.h"
#include "mldb/arch/simd_vector.h"

using namespace std;



namespace MLDB {
namespace Builtins {

ExpressionValue fft(const std::vector<ExpressionValue> & args,
                    const SqlRowScope & scope)
{
    checkArgsSize(args.size(), 1, 3, "fft");

    if (args[0].empty() || args[1].empty()) {
        return ExpressionValue::null(calcTs(args[0], args[1]));
    }

    Utf8String directionStr
        = getArg(args, 1, "direction", "forward").toUtf8String();
    pffft_direction_t direction;
    if (directionStr == "forward")
        direction = PFFFT_FORWARD;
    else if (directionStr == "backward")
        direction = PFFFT_BACKWARD;
    else throw HttpReturnException(400, "FFT direction must be either 'forward' "
                                   "or 'backward'; got '" + directionStr
                                   + "'.");

    pffft_transform_t type;

    Utf8String typeStr = getArg(args, 2, "type", "real").toUtf8String();
    if (typeStr == "real")
        type = PFFFT_REAL;
    else if (typeStr == "complex")
        type = PFFFT_COMPLEX;
    else throw HttpReturnException(400, "FFT type must be either 'real' "
                                   "or 'complex'; got '" + typeStr
                                   + "'.");
    


    DimsVector dimsVector = args[0].getEmbeddingShape();

    //cerr << "fft " << directionStr << " " << typeStr << " " << dimsVector << endl;

    // Either an embedding of size n (real numbers), or an embedding of size
    // n x 2 (complex numbers).

    if (dimsVector.size() != 1
        && !(dimsVector.size() == 2 && dimsVector[1] == 2)) {
        throw HttpReturnException(400, "FFT requires either a flat embedding "
                                  "for real numbers, or a nx2 embedding for "
                                  "complex numbers.");
    }

    size_t nel = std::accumulate(dimsVector.begin(), dimsVector.end(), (size_t)1,
                                 std::multiplies<size_t>());
    
    if (nel % 32 != 0) {
        // Comments from pffft.c:
        /* unfortunately, the fft size must be a multiple of 16 for complex FFTs 
           and 32 for real FFTs -- a lot of stuff would need to be rewritten to
           handle other cases (or maybe just switch to a scalar fft, I don't know..) */
        //if (transform == PFFFT_REAL) { assert((N%(2*SIMD_SZ*SIMD_SZ))==0 && N>0); }
        //assert((N % 32) == 0);

        throw HttpReturnException(400, "FFT size must be a multiple of 32");
    }

    size_t n = dimsVector[0];

    if (dimsVector.size() == 1) {
        // Real valued input
        // This gives a complex output that is symmetrical about the mid
        // point of the range

        if (direction != PFFFT_FORWARD || type != PFFFT_REAL) {
            throw HttpReturnException
                (400, "Complex input is required for inverse or complex fft");
        }

        PFFFT_Setup * setup = pffft_new_setup(n, type);

        if (!setup) {
            throw HttpReturnException(400, "Couldn't setup fft transform for size "
                                      + to_string(dimsVector[0]));
        }
        Scope_Exit(pffft_destroy_setup(setup));

        // Pffft
        PossiblyDynamicBuffer<float> workspace(n + 3);

        // Get a 16 byte aligned buffer
        float * tmp = workspace.data();
        while (((size_t)tmp & 15) != 0)
            ++tmp;

        std::shared_ptr<float>
            data((float *)pffft_aligned_malloc(n * 2 * sizeof(float)),
                 [] (float * p) {pffft_aligned_free(p); });
    
        args[0].convertEmbedding(data.get(), n, ST_FLOAT32);
    
        pffft_transform_ordered(setup, data.get(), data.get(), tmp,
                                direction);
        
        // From pffft.h
        // (for real transforms, both 0-frequency and half frequency
        // components, which are real, are assembled in the first entry as
        // F(0)+i*F(n/2+1). Note that the original fftpack did place
        // F(n/2+1) at the end of the arrays).
        //data.get()[1] = 0;

        // We have two columns: one with the real, the other with the
        // imaginary.
        DimsVector newShape({n/2, 2});
        
        return ExpressionValue::embedding(args[0].getEffectiveTimestamp(),
                                          data, ST_FLOAT32, newShape);
    }
    else {
        // Complex input, n x 2
        if (type == PFFFT_REAL) {
            n *= 2;   // fft of real n -> n/2 x 2, so ifft of n x 2 -> n*2
        }

        PFFFT_Setup * setup = pffft_new_setup(n, type);

        if (!setup) {
            throw HttpReturnException(400, "Couldn't setup complex fft transform for size "
                                      + to_string(n));
        }
        Scope_Exit(pffft_destroy_setup(setup));

        PossiblyDynamicBuffer<float> workspace((n*2) + 3);

        // Get a 16 byte aligned buffer
        float * tmp = workspace.data();
        while (((size_t)tmp & 15) != 0)
            ++tmp;

        std::shared_ptr<float>
            data((float *)pffft_aligned_malloc(nel * sizeof(float)),
                 [] (float * p) {pffft_aligned_free(p); });
    
        args[0].convertEmbedding(data.get(), nel, ST_FLOAT32);
    
        pffft_transform_ordered(setup, data.get(), data.get(), tmp,
                                direction);

        DimsVector newShape;
        size_t numToScale;
        if (direction == PFFFT_FORWARD || type == PFFFT_COMPLEX) {
            // We have two columns: one with the real, the other with the
            // imaginary.
            newShape = {n, 2};
            numToScale = n * 2;
        }
        else {
            newShape = { n };
            numToScale = n;
        }

        if (direction == PFFFT_BACKWARD) {
            // The ifft doesn't rescale, so we do so here in order to
            // ensure that ifft(fft(x)) = x
            SIMD::vec_scale(data.get(), 1.0 / n, data.get(), numToScale);
        }
        
        return ExpressionValue::embedding(args[0].getEffectiveTimestamp(),
                                          data, ST_FLOAT32, newShape);
    }
}

BoundFunction bind_fft(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1, 3, "fft");

    if (args[0].info->isEmbedding()) {
        auto shape
            = args[0].info->getEmbeddingShape();

        if (shape.size() != 1)
            throw HttpReturnException(500, "only 1d ffts are supported");

        return {
            fft,
            std::make_shared<EmbeddingValueInfo>(shape, ST_FLOAT32)
        };
    }
    else {
        return {
            fft,
            std::make_shared<EmbeddingValueInfo>(ST_FLOAT32) // length unknown
        };
    }
}

static RegisterBuiltin registerFft(bind_fft, "fft");

ExpressionValue sliceEmbedding(const std::vector<ExpressionValue> & args,
                              const SqlRowScope & scope)
{
    checkArgsSize(args.size(), 2, "slice");

    int p = args[1].getAtom().toInt();
    if (p < 0)
        throw HttpReturnException(400, "slice function index is less than "
                                  "zero");

    auto shape = args[0].getEmbeddingShape();
    size_t l = shape.back();

    if (p < 0 || p >= l)
        throw HttpReturnException(400, "slice function index is greater "
                                  "than last dimension size");
    
    shape.pop_back();

    if (shape.empty()) {
        return args[0].getColumn(p);
    }

    size_t n = 1;
    for (auto & s: shape)
        n *= s;

    StorageType st = args[0].getEmbeddingType();

    auto buf = allocateStorageBuffer(shape, st);

    // TODO: more efficient...
    auto cells = args[0].getEmbeddingCell();

    for (size_t i = 0;  i < n;  ++i) {
        fillStorageBuffer(buf.get(), i, st, 1, cells[i * l + p]);
    }

    return ExpressionValue
        ::embedding(args[0].getEffectiveTimestamp(), buf,
                    args[0].getEmbeddingType(), shape);
}

ExpressionValue sliceUnknown(const std::vector<ExpressionValue> & args,
                            const SqlRowScope & scope)
{
    throw HttpReturnException(600, "unknown function slice");
}

BoundFunction bind_slice(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 2, "slice");

    if (args[0].info->isEmbedding()) {
        auto shape
            = args[0].info->getEmbeddingShape();
        if (shape.empty()) {
            throw HttpReturnException(400, "Can't take slice of null embedding");
        }

        return {
            sliceEmbedding,
            std::make_shared<EmbeddingValueInfo>(shape, args[0].info->getEmbeddingType())
        };
    }

    return {
        sliceUnknown,
        std::make_shared<EmbeddingValueInfo>(ST_FLOAT32) // length unknown
    };
}

static RegisterBuiltin registerSlice(bind_slice, "slice");


// Impulse function, which is a vector of given length with a 1 in
// the first position and zeros everywhere else
DEF_SQL_BUILTIN(impulse, 1, "reshape([1], [$1], 0)");

// Shifted impulse function, which is a vector of length $1 with a
// 1 in the $2 position.  We get it by gluing together a leading
// embedding of zeros and a normal impulse function.

DEF_SQL_BUILTIN(shifted_impulse, 2, "concat(reshape([], [$2], 0),impulse($1-$2))");

DEF_SQL_BUILTIN(real, 1, "slice($1, 0)");
DEF_SQL_BUILTIN(imag, 1, "slice($1, 1)");

DEF_SQL_BUILTIN(amplitude, 1, "sqrt(pow(real($1),2)+pow(imag($1),2))");
DEF_SQL_BUILTIN(phase, 1, "atan2(real($1),imag($1))");

} // namespace Builtins
} // namespace MLDB

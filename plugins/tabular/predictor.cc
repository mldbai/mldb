#include "predictor.h"
#include "mmap.h"
#include "mldb/utils/safe_clamp.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/array_description.h"

namespace MLDB {

int64_t Predictor::predict(uint32_t x) const
{
    auto b = basis(x);
    float accum = 0.0;
    for (size_t i = 0;  i < BASIS_DIMS;  ++i)
        accum += b[i] * params[i];

    int64_t val = safely_clamped(std::floorf(accum + 0.5f));
    int64_t res;
    
    if (__builtin_sub_overflow((int64_t)val, (int64_t)offset, &res)) {
        if (offset < 0) {
            // positive overflow
            res = std::numeric_limits<int64_t>::min();
        }
        else {
            // negative overflow
            res = std::numeric_limits<int64_t>::max();
        }
    }

    return res;
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(Predictor)
{
    addField("params", &Predictor::params, "Parameter values");
    addField("offset", &Predictor::offset, "Offset for predicted value");
}

} // namespace MLDB

#pragma once

#include "mldb/compiler/stdlib.h"

namespace std {

#ifdef MLDB_STDLIB_GCC
template<class T1, class T2>
class pair;
#elif MLDB_STDLIB_LLVM
inline namespace __1 {
template<class T1, class T2>
class pair;
}
#else
#  error "Tell us how to forward declare pair for your standard library"
#endif

} // namespace std

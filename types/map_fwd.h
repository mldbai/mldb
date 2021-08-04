#pragma once

#include "mldb/compiler/stdlib.h"

namespace std {

#ifdef MLDB_STDLIB_GCC
template<class K, class V, class L, class A>
class map;
#elif MLDB_STDLIB_LLVM
inline namespace __1 {
template<class K, class V, class L, class A>
class map;    
}
#else
#  error "Tell us how to forward declare set for your standard library"
#endif

} // namespace std

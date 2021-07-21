#pragma once

#include "mldb/compiler/stdlib.h"

namespace std {

#ifdef MLDB_STDLIB_GCC
template<class V, class L, class A>
class set;
#elif MLDB_STDLIB_LLVM
inline namespace __1 {
template<class V, class L, class A>
class set;
}
#else
#  error "Tell us how to forward declare set for your standard library"
#endif

}

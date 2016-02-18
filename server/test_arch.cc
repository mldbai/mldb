#include "mldb/arch/simd.h"
#include <iostream>

int main(int argc, char ** argv)
{
    if (ML::has_sse42()) {
        std::cout << "sse 4.2 supported" << std::endl;
    }
    else {
        std::cout << "sse 4.2 not supported" << std::endl;
    }
}

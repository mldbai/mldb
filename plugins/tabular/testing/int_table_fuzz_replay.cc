#include <stdint.h>
#include <stddef.h>
#include <cassert>
#include <fstream>
#include <vector>
#include <iostream>
#include "util/ostream_vector.hpp"

using namespace std;

extern bool traceTests;
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int main(int argc, char ** argv)
{
    traceTests = true;

    if (argc != 2)
        MLDB_THROW_RUNTIME_ERROR("replay test needs to be called with one argument");
    std::ifstream stream(argv[1]);

    std::vector<char> buf;
    buf.resize(100000000);
    stream.read(buf.data(), buf.size());
    size_t size = stream.gcount();
    buf.resize(size);

    cerr << "size = " << size << endl;

    int res = LLVMFuzzerTestOneInput((const uint8_t *)buf.data(), size);
    cerr << "res = " << res << endl;

    return res;
}


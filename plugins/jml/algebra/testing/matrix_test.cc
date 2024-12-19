#include "catch2/catch_all.hpp"
#include "mldb/plugins/jml/algebra/matrix.h"
#include "mldb/plugins/jml/algebra/matrix_ops.h"

using namespace std;
using namespace MLDB;

TEST_CASE("construct_and_fill")
{
    Matrix<float, 2> m(20, 30);

    CHECK(m.is_contiguous());
    CHECK(m.num_elements() == 600);
    CHECK(m.stride(0) == 30);
    CHECK(m.stride(1) == 1);
    CHECK(m.dim(0) == 20);
    CHECK(m.dim(1) == 30);

    CHECK(m.strides() == std::array<ssize_t, 2>{ 30, 1});
    CHECK(m.shape() == std::array<size_t, 2>{ 20, 30});

    m.fill(1);

    CHECK(m[0][0] == 1);

    m.fill(10);

    CHECK(m[19][29] == 10);

    m[19][29] = 3;

    CHECK(m[19][29] == 3);

    m[3].fill(30);

    CHECK(m[3][0] == 30);
    CHECK(m[3][1] == 30);
    CHECK(m[3][29] == 30);

    CHECK(m[3].is_contiguous());
}

// For testing only
void * operator new[] (size_t size) noexcept(false)
{
    // Fill the memory with 0xff so we can see if it's uninitialized
    void * result = ::malloc(size);
    std::memset(result, 255, size);
    return result;
}

void operator delete[] (void * ptr) noexcept(true)
{
    return ::free(ptr);
}

TEST_CASE("default initialized (should be zero)")
{
    SECTION("size parameters")
    {
        Matrix<float, 2> m(10, 10);
        bool nonzero = false;
        for (unsigned i = 0; i < 10; ++i)
            for (unsigned j = 0; j < 10; ++j)
                if (m[i][j] != 0)
                    nonzero = true;
        CHECK(!nonzero);
    }

    SECTION("shape")
    {
        Matrix<float, 2> m(array<size_t, 2>{10, 10});
        bool nonzero = false;
        for (unsigned i = 0; i < 10; ++i)
            for (unsigned j = 0; j < 10; ++j)
                if (m[i][j] != 0)
                    nonzero = true;
        CHECK(!nonzero);
    }
}

TEST_CASE("uninitialized")
{
    SECTION("size parameters")
    {
        Matrix<float, 2> m(10, 10, matrix_uninitialized);
        bool nonzero = false;
        for (unsigned i = 0; i < 10; ++i)
            for (unsigned j = 0; j < 10; ++j)
                if (m[i][j] != 0)
                    nonzero = true;
        CHECK(nonzero);
    }

    SECTION("shape")
    {
        Matrix<float, 2> m({10, 10}, matrix_uninitialized);
        bool nonzero = false;
        for (unsigned i = 0; i < 10; ++i)
            for (unsigned j = 0; j < 10; ++j)
                if (m[i][j] != 0)
                    nonzero = true;
        CHECK(nonzero);
    }
}

TEST_CASE("zero initialized")
{
    SECTION("size parameters")
    {
        Matrix<float, 2> m(10, 10, matrix_zero);

        bool nonzero = false;
        for (unsigned i = 0; i < 10; ++i)
            for (unsigned j = 0; j < 10; ++j)
                if (m[i][j] != 0)
                    nonzero = true;
        CHECK(!nonzero);
    }

    SECTION("shape")
    {
        Matrix<float, 2> m({10, 10}, matrix_zero);

        bool nonzero = false;
        for (unsigned i = 0; i < 10; ++i)
            for (unsigned j = 0; j < 10; ++j)
                if (m[i][j] != 0)
                    nonzero = true;
        CHECK(!nonzero);
    }
}

TEST_CASE("one initialized")
{
    SECTION("size parameters")
    {
        Matrix<float, 2> m(10, 10, matrix_fill(1));

        bool nonone = false;
        for (unsigned i = 0; i < 10; ++i)
            for (unsigned j = 0; j < 10; ++j)
                if (m[i][j] != 1)
                    nonone = true;
        CHECK(!nonone);
    }

    SECTION("shape")
    {
        Matrix<float, 2> m({10, 10}, matrix_fill(1));

        bool nonone = false;
        for (unsigned i = 0; i < 10; ++i)
            for (unsigned j = 0; j < 10; ++j)
                if (m[i][j] != 1)
                    nonone = true;
        CHECK(!nonone);
    }
}

TEST_CASE("assign")
{
    Matrix<float, 2> m(10, 10);
    m.fill(10);

    Matrix<float, 2> m2 = m;
    m2.fill(20);

    CHECK(m[0][0] == 10);
    CHECK(m2[0][0] == 20);

    m[1].assign(m2[2]);

    CHECK(m[1][0] == 20);
}

TEST_CASE("multiply")
{
    Matrix<float, 2> m { { 2, 3, 4, 5}, { 0, 1, 2, 3 } };
    Matrix<float, 2> m2 { { 1, 0}, { 0, 1 } };

    cerr << "before multiply" << endl;
    auto m3 = m2 * m;
    cerr << "after multiply" << endl;
    cerr << "m3 spahe " << m3.shape() << endl;
    cerr << "m3 type = " << typeid(m3).name() << endl;

    for (unsigned i = 0;  i < 2;  ++i)
        for (unsigned j = 0;  j < 4;  ++j)
            CHECK(m3[i][j] == m[i][j]); 
}

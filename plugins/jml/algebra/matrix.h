/** matrix.h                                                      -*- C++ -*-
    Jeremy Barnes, 1 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Matrix structure, to replace boost::multi_array.
*/


#pragma once

#include <climits>
#include <array>
#include <iostream>
#include <type_traits>
#include <memory>
#include <algorithm>

#include "mldb/utils/ostream_array.h"
#include "mldb/compiler/compiler.h"
#include "mldb/arch/exception.h"

namespace MLDB {

template<typename El, size_t Sz>
constexpr El prod(const std::array<El, Sz> & a)
{
    El result(1);
    for (El el: a)
        result *= el;
    return result;
}

void matrix_throw_out_of_range(const char * function, int line, const char * what, ssize_t index, ssize_t min, ssize_t max) MLDB_NORETURN;
void matrix_throw_incompatible_ranges(const char * function, int line, ssize_t dim1, ssize_t dim2, const char * what) MLDB_NORETURN;
void matrix_throw_incompatible_dimensions(const char * function, int line, const size_t * dims1, size_t nd1, const size_t * dims2, size_t ndims2, const char * what) MLDB_NORETURN;

#define MLDB_MATRIX_CHECK_RANGE(index, min, max, what) \
    if (MLDB_UNLIKELY(index < min || index >= max)) \
        matrix_throw_out_of_range(__PRETTY_FUNCTION__, __LINE__, what, index, min, max)

#define MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(a, b, what) \
    if (MLDB_UNLIKELY(a != b)) \
        matrix_throw_incompatible_ranges(__PRETTY_FUNCTION__, __LINE__, a, b, what)

#define MLDB_MATRIX_THROW_INCOMPATIBLE_DIMENSIONS(a, b, what) \
    matrix_throw_incompatible_dimensions(__PRETTY_FUNCTION__, __LINE__, a.shape().data(), a.shape().size(), b.shape().data(), b.shape().size(), what)

template<typename Float, size_t Dims>
struct MatrixRef;

template<typename Float, size_t Dims>
struct MatrixBase;

// Pass to not initialize the matrix as an optimization
constexpr struct matrix_uninitialized_t {
    template<typename Float, size_t Dims> static void initialize(MatrixRef<Float, Dims> matrix) {}
} matrix_uninitialized;

// Pass to initialize with zeros
constexpr struct matrix_zero_t {
    template<typename Float, size_t Dims> static void initialize(MatrixRef<Float, Dims> matrix) { matrix.fill(0); }
    //template<typename Element> static void initialize(Element & element) { element = 0; }
} matrix_zero;

// Pass to initialize with a constant value
template<typename Float> struct matrix_fill_t {
    Float val;
    template<typename Float2, size_t Dims> void initialize(MatrixRef<Float2, Dims> matrix) { matrix.fill(val); }
    //template<typename Element> void initialize(Element & element) { element = val; }
};
template<typename Float> constexpr matrix_fill_t<Float> matrix_fill(Float val) { return { val }; }


template<typename Float, size_t Dims>
struct MatrixRef {

    MatrixRef() = default;
    MatrixRef(const MatrixRef &) = default;

    MatrixRef(MatrixBase<Float, Dims> && other)
    {
        static_assert(false, "A Matrix cannot be moved into a MatrixRef as nobody would own the data");
    }

    void operator = (MatrixBase<Float, Dims> && other)
    {
        static_assert(false, "A Matrix cannot be moved into a MatrixRef as nobody would own the data");
    }

    MatrixRef & operator = (const MatrixRef & other) = default;

protected:
    template<typename Float2, size_t Dims2>
    friend struct MatrixRef;

    MatrixRef(Float * data, const ssize_t * strides, const size_t * dims)
        : data_(data), strides_(strides), dims_(dims)
    {
    }

public:
#if 0
    template<typename Float2>
    MatrixRef & operator = (const MatrixRef<Float2, Dims> & other)
    {
        this->assign(other);
        return *this;
    }

    MatrixRef & operator = (const MatrixRef & other)
    {
        this->assign(other);
        return *this;
    }
#endif

    template<typename Float2, typename Fill = matrix_zero_t>
    void assign(const MatrixRef<Float2, Dims> & other, Fill fill = matrix_zero)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(other.dim(0), this->dim(0), "Assign of incompatible ranges");

        if (this->num_elements() == other.num_elements() && this->is_contiguous() && other.is_contiguous()) {
            std::copy_n(other.data(), this->num_elements(), this->data());
        }
        else {
            auto n = std::min(this->dim(0), other.dim(0));
            for (size_t i = 0; i < n;  ++i) {
                this->assign(i, other[i], fill);
            }
            for (size_t i = n;  i < this->dim(0);  ++i) {
                fill.initialize(operator [] (i));
            }
        }
    }

    template<typename SubMatrix>
    void assign(const std::initializer_list<SubMatrix> & other)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(other.size(), this->dim(0), "Assign of incompatible ranges");

        auto it = other.begin();
        auto n = std::min<size_t>(other.size(), this->dim(0));
        for (size_t i = 0;  i < n;  ++i, ++it) {
                this->assign(i, *it, matrix_zero);
        }
        for (size_t i = n;  i < this->dim(0);  ++i) {
            this->fill(0);
        }
    }

#if 0
    template<typename SubMatrix, size_t N>
    void assign(const std::array<SubMatrix, N> & other)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(N, this->dim(0), "Assign of incompatible ranges");

        for (size_t i = 0;  i < N;  ++i) {
            this->assign(i, other[N]);
        }
    }
#endif

    template<typename Other, typename Fill>
    void assign(size_t i, const Other & other, Fill fill = matrix_zero)
    {
        this->operator [] (i).assign(other, fill);
    }

    size_t dim(size_t n) const
    {
        MLDB_MATRIX_CHECK_RANGE(n, 0, Dims, "MatrixRef dim number out of range");
        return this->dims_[n];
    }

    ssize_t stride(size_t n) const
    {
        MLDB_MATRIX_CHECK_RANGE(n, 0, Dims, "MatrixRef stride number out of range");
        return this->strides_[n];
    }

    std::array<size_t, Dims> shape() const
    {
        std::array<size_t, Dims> result;
        std::copy_n(dims_, Dims, result.begin());
        return result;
    }

    std::array<ssize_t, Dims> strides() const
    {
        std::array<ssize_t, Dims> result;
        std::copy_n(strides_, Dims, result.begin());
        return result;
    }

    MatrixRef<Float, Dims-1> operator [] (size_t n) const
    {
        MLDB_MATRIX_CHECK_RANGE(n, 0, dims_[0], "MatrixRef index number out of range");
        ssize_t offset = n * strides_[0]; 
        return MatrixRef<Float, Dims-1>{data_ + offset, strides_ + 1, dims_ + 1};
    }

    MatrixRef<Float, Dims-1> operator [] (size_t n)
    {
        MLDB_MATRIX_CHECK_RANGE(n, 0, dims_[0], "MatrixRef index number out of range");
        ssize_t offset = n * strides_[0]; 
        return MatrixRef<Float, Dims-1>{data_ + offset, strides_ + 1, dims_ + 1};
    }

    bool is_contiguous() const
    {
        return stride(0) == dim(1);
    }

    void fill(Float value)
    {
        if (is_contiguous())
            std::fill_n(data(), num_elements(), value);
        else {
            for (size_t i = 0;  i < dim(0);  ++i) {
                this->operator [] (i).fill(value);
            }
        }
    }

    const Float * data() const { return data_; }
    Float * data() { return data_; }

    size_t num_elements() const
    {
        return prod(shape());
    }
    
//protected:
    Float * data_ = nullptr;
    const ssize_t * strides_ = nullptr;
    const size_t * dims_ = nullptr;
};

template<typename Float>
struct MatrixRef<Float, 1> {
    static constexpr size_t Dims = 1;

    template<typename Float2, typename Fill = matrix_zero_t>
    void assign(const MatrixRef<Float2, 1> & other, Fill fill = matrix_zero)
    {
        // TODO: this should work by either partially copying or filling excess
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(other.dim(0), this->dim(0), "Assign of incompatible ranges");

        if (num_elements() == other.num_elements() && is_contiguous() && other.is_contiguous()) {
            std::copy_n(other.data(), num_elements(), data());
        }
        else {
            for (size_t i = 0;  i < dim(0);  ++i) {
                this->operator [] (i) = other[i];
            }
        }
    }

    template<typename Float2, typename Fill = matrix_zero_t>
    void assign(const std::initializer_list<Float2> & other, Fill fill = matrix_zero)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(other.size(), this->dim(0), "Assign of incompatible ranges");

        auto it = other.begin();
        for (size_t i = 0;  i < this->dim(0);  ++i, ++it) {
                this->assign(i, *it, fill);
        }
    }

    template<typename Float2, size_t N, typename Fill = matrix_zero_t>
    void assign(const std::array<Float2, N> & other, Fill fill = matrix_zero)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(N, this->dim(0), "Assign of incompatible ranges");

        if (is_contiguous()) {
            std::copy_n(other.data(), N, data());
        }
        else {
            for (size_t i = 0;  i < this->dim(0);  ++i) {
                    this->assign(i, other[i], fill);
            }
        }
    }

    template<typename Float2, typename Fill = matrix_zero_t>
    void assign(size_t i, Float2 other, Fill fill = matrix_zero)
    {
        this->operator [] (i) = other;
    }

    size_t dim(size_t n) const
    {
        MLDB_MATRIX_CHECK_RANGE(n, 0, Dims, "MatrixRef1 dim number out of range");
        return dims_[0];
    }

    size_t stride(size_t n) const
    {
        MLDB_MATRIX_CHECK_RANGE(n, 0, Dims, "MatrixRef1 stride number out of range");
        return strides_[0];
    }

    std::array<size_t, 1> shape() const
    {
         return { dims_[0] };
    }

    std::array<ssize_t, 1> strides() const
    {
         return { strides_[0] };
    }

    const Float & operator [] (size_t n) const
    {
        MLDB_MATRIX_CHECK_RANGE(n, 0, dims_[0], "MatrixRef1 index number out of range");
        size_t offset = n * strides_[0]; 
        return data_[offset];
    }

    Float & operator [] (size_t n)
    {
        MLDB_MATRIX_CHECK_RANGE(n, 0, dims_[0], "MatrixRef1 index number out of range");
        size_t offset = n * strides_[0]; 
        return data_[offset];
    }

    const Float * data() const { return data_; }
    Float * data() { return data_; }

    size_t num_elements() const
    {
        return dims_[0];
    }

    bool is_contiguous() const
    {
        return strides_[0] == 1;
    }

    void fill(Float value)
    {
        if (is_contiguous())
            std::fill_n(data(), num_elements(), value);
        else {
            auto * p = data();
            auto stride = strides_[0];
            for (size_t i = 0;  i < num_elements();  ++i, p += stride) {
                *p = value;
            }
        }
    }
    
//protected:
    Float * data_ = nullptr;
    const ssize_t * strides_ = nullptr;
    const size_t * dims_ = nullptr;
};

template<typename Float, size_t Dims>
struct MatrixBase: public MatrixRef<Float, Dims> {

    template<typename Init = matrix_zero_t>
    MatrixBase(Init initialize = {}, decltype(std::declval<Init>().initialize(std::declval<MatrixBase &>())) * = nullptr)
    {
        std::array<size_t, Dims> shape;
        std::fill(shape.begin(), shape.end(), 0);
        init(shape, initialize);
    }

    template<typename Init = matrix_zero_t>
    MatrixBase(const std::array<size_t, Dims> & shape, Init initialize = {}, decltype(std::declval<Init>().initialize(std::declval<MatrixBase &>())) * = nullptr)
    {
        init(shape, initialize);
    }

    MatrixBase(const MatrixBase & other)
    {
        using namespace std;
        //cerr << "MatrixBase copy constructor" << endl;
        initFrom(other);
    }

    template<typename Float2 /*, typename Enable = std::is_same_t<Float2, Float>*/>
    MatrixBase(const MatrixRef<Float2, Dims> & other)
    {
        using namespace std;
        //cerr << "MatrixBase copy constructor from MatrixRef " << __PRETTY_FUNCTION__ << endl;
        //cerr << "other.shape() = " << other.shape() << endl;
        initFrom(other);
    }

    MatrixBase(const std::initializer_list<Float> & vals)
    {
        static_assert(Dims == 1, "Attempt to initialize vector from matrix initializer");
        initFrom(vals);
    }

    MatrixBase(const std::initializer_list<std::initializer_list<Float>> & vals)
    {
        static_assert(Dims == 2, "Attempt to initialize matrix from wrong dim initializer");
        initFrom(vals);
    }

    MatrixBase & operator = (const MatrixBase & other)
    {
        using namespace std;
        //cerr << "MatrixBase operator (copy) = " << endl;

        // The shape will be copied from the other matrix, so all elements will be initialized
        resize(other.shape(), matrix_uninitialized);
        
        // Copy each slice on this dimension
        for (size_t i = 0;  i < this->dim(0);  ++i) {
            this->assign(i, other.operator [] (i), matrix_zero);
        }

        return *this;
    }
    
    MatrixBase(MatrixBase && other)
    {
        using namespace std;
        //cerr << "MatrixBase move constructor" << endl;
        swap(other);
    }

    MatrixBase & operator = (MatrixBase && other)
    {
        using namespace std;
        //cerr << "MatrixBase move operator =" << endl;
        swap(other);
        return *this;
    }

    void swap(MatrixBase & other)
    {
        std::swap(this->data_, other.data_);
        std::swap(this->strides_, other.strides_);
        std::swap(this->dims_, other.dims_);
        std::swap(this->state_, other.state_);
    }

    template<size_t Dim, typename... Rest>
    void init_shape(std::array<size_t, Dims> & shape, size_t first, Rest&&... rest)
    {
        static_assert(Dim < Dims, "Too many dimensions passed to Matrix");
        shape[Dim] = first;
        init_shape<Dim + 1>(shape, std::forward<Rest>(rest)...);
    }

    template<size_t Dim>
    void init_shape(std::array<size_t, Dim> & shape)
    {
        init(shape, matrix_zero);
    }

    template<size_t Dim, typename Init>
    void init_shape(std::array<size_t, Dim> & shape, Init initialize)
    {
        init(shape, initialize);
    }

    template<typename... Args>
    MatrixBase(size_t shape0, Args&&... args)
    {
        std::array<size_t, Dims> shape;
        shape[0] = shape0;
        init_shape<1>(shape, std::forward<Args>(args)...);
    }

    template<size_t Dim, typename... Rest>
    void resize_shape(std::array<size_t, Dims> & shape, size_t first, Rest&&... rest)
    {
        static_assert(Dim < Dims, "Too many dimensions passed to Matrix resize");
        shape[Dim] = first;
        resize_shape<Dim + 1>(shape, std::forward<Rest>(rest)...);
    }

    template<size_t Dim>
    void resize_shape(std::array<size_t, Dim> & shape)
    {
        resize(shape, matrix_zero);
    }

    template<size_t Dim, typename Init>
    void resize_shape(std::array<size_t, Dim> & shape, Init initialize)
    {
        resize(shape, initialize);
    }

    template<typename... Args>
    void resize(Args... args)
    {
        std::array<size_t, Dims> shape{};
        resize_shape<0>(shape, args...);
    }

    template<typename Init>
    void resize(const std::array<size_t, Dims> & dims, Init init = matrix_zero,
                decltype(std::declval<Init>().initialize(std::declval<MatrixBase &>())) * = nullptr)
    {
        // TODO: don't copy when it's not needed
        MatrixBase newMatrix(dims);
        size_t n = std::min<size_t>(this->dim(0), newMatrix.dim(0));
        for (size_t i = 0;  i < n;  ++i) {
            newMatrix.assign(i, this->operator [] (i), init);
        }
        for (size_t i = n;  i < newMatrix.dim(0);  ++i) {
            init.initialize(newMatrix[i]);
        }
        swap(newMatrix);
    }

    template<typename Float2>
    MatrixBase & operator += (const MatrixBase<Float2, Dims> & other)
    {
        if (other.dim(0) != this->dim(0)) {
            throw std::range_error("MatrixBase operator +=");
        }
        for (size_t i = 0;  i < this->dim(0);  ++i) {
            this->operator [] (i) += other[i]; 
        }
        return *this;
    }

    MatrixBase & operator += (const Float & other)
    {
        for (size_t i = 0;  i < this->dim(0);  ++i) {
            this->operator [] (i) += other; 
        }
        return *this;
    }

    MatrixBase & operator *= (const Float & other)
    {
        for (size_t i = 0;  i < this->dim(0);  ++i) {
            this->operator [] (i) *= other; 
        }
        return *this;
    }

//protected:
    template<typename Init>
    void init(std::array<size_t, Dims> shape, Init init = matrix_zero)
    {
        state_.reset(new State(shape));
        this->data_ = state_->data;
        this->strides_ = state_->strides;
        this->dims_ = state_->dims;
        init.initialize(*this);
    }

    template<typename Float2>
    static std::array<size_t, Dims> getShape(const MatrixRef<Float2, Dims> & other)
    {
        return other.shape();
    }

    template<typename Float2>
    static std::array<size_t, 1> getShape(const std::initializer_list<Float2> & init)
    {
        return { init.size() };
    }

    template<typename Float2>
    static std::array<size_t, 2> getShape(const std::initializer_list<std::initializer_list<Float2> > & init)
    {
        if (init.size() == 0) {
            return { 0, 0 };
        }
        return { init.size(), init.begin()->size() };
    }

    template<typename Initializer>
    void initFrom(const Initializer & other)
    {
        using namespace std;
        //cerr << "initFrom " << getShape(other) << endl;
        init(getShape(other), matrix_uninitialized);
        //cerr << "shape = " << this->shape() << endl;
        this->assign(other);
        //cerr << "after assign shape = " << this->shape() << endl;
    }

    static constexpr size_t MAX_DIMS = 8;
    struct State {
        State(const std::array<size_t, Dims> & shape)
        {
            size_t n = setDims(shape);
            data = n ? new Float[n] : nullptr;
        }

        ~State()
        {
            delete[] data;
            std::fill_n(strides, Dims, SSIZE_MAX);
            std::fill_n(dims, Dims, (size_t)-1);
        }

        size_t setDims(const std::array<size_t, Dims> & shape)
        {
            size_t n = prod(shape), result = n;
            for (size_t i = 0;  i < Dims;  ++i) {
                n /= shape[i] == 0 ? 1 : shape[i];
                strides[i] = n;
                dims[i] = shape[i];
            }
            return result;
        }

        State(const State & other) = delete;
        void operator = (const State & other) = delete;

        Float * data = nullptr;
        ssize_t strides[Dims];
        size_t dims[Dims];
    };

    std::unique_ptr<State> state_;
};

template<typename Float, size_t Dims> using Matrix = MatrixBase<Float, Dims>;
template<typename Float> using NumericVector = MatrixBase<Float, 1>;

template<class Float>
std::ostream &
operator << (std::ostream & stream, const MatrixRef<Float, 1> & m)
{
    stream << "[";
    for (unsigned j = 0;  j < m.num_elements();  ++j)
        stream << " " << std::to_string(m[j]);
    return stream << " ]";
}

template<class Float>
std::ostream &
operator << (std::ostream & stream, const MatrixRef<Float, 2> & m)
{
    for (unsigned i = 0;  i < m.dim(0);  ++i) {
        stream << "    " << m[i] << std::endl;
    }
    return stream;
}

} // namespace MLDB

namespace std {

template<typename Float, size_t Dims>
void swap(MLDB::MatrixBase<Float, Dims> & a, MLDB::MatrixBase<Float, Dims> & b)
{
    a.swap(b);
}

} // namespace std
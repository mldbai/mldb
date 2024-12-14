/** matrix.h                                                      -*- C++ -*-
    Jeremy Barnes, 1 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Matrix structure, to replace boost::multi_array.
*/


#pragma once

#include <array>
#include <iostream>
#include <type_traits>
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

template<size_t Dims>
struct Extents {
    std::array<size_t, Dims> dims;

    constexpr Extents<Dims + 1> operator [] (size_t n) const
    {
        Extents<Dims + 1> result;
        std::copy_n(dims.begin(), Dims, result.dims.begin());
        result.dims[Dims] = n;
        return result;
    }
};

constexpr Extents<0> extents;

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
struct MatrixBase;


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

    template<typename Float2>
    void assign(const MatrixRef<Float2, Dims> & other)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(other.dim(0), this->dim(0), "Assign of incompatible ranges");

        if (this->num_elements() == other.num_elements() && this->contiguous() && other.contiguous()) {
            std::copy_n(other.data(), this->num_elements(), this->data());
        }
        else {
            auto n = std::min(this->dim(0), other.dim(0));
            for (size_t i = 0; i < n;  ++i) {
                this->assign(i, other[i]);
            }
            for (size_t i = n;  i < this->dim(0);  ++i) {
                this->fill(0);
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
                this->assign(i, *it);
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

    template<typename Other>
    void assign(size_t i, const Other & other)
    {
        this->operator [] (i).assign(other);
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

    bool contiguous() const
    {
        return stride(0) == dim(1);
    }

    void fill(Float value)
    {
        if (contiguous())
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

#if 0
    template<typename Float2>
    MatrixRef & operator = (const MatrixRef<Float2, 1> & other)
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

    template<typename Float2>
    void assign(const MatrixRef<Float2, 1> & other)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(other.dim(0), this->dim(0), "Assign of incompatible ranges");

        if (num_elements() == other.num_elements() && contiguous() && other.contiguous()) {
            std::copy_n(other.data(), num_elements(), data());
        }
        else {
            for (size_t i = 0;  i < dim(0);  ++i) {
                this->operator [] (i) = other[i];
            }
        }
    }

    template<typename Float2>
    void assign(const std::initializer_list<Float2> & other)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(other.size(), this->dim(0), "Assign of incompatible ranges");

        auto it = other.begin();
        for (size_t i = 0;  i < this->dim(0);  ++i, ++it) {
                this->assign(i, *it);
        }
    }

    template<typename Float2, size_t N>
    void assign(const std::array<Float2, N> & other)
    {
        MLDB_MATRIX_CHECK_COMPATIBLE_RANGES(N, this->dim(0), "Assign of incompatible ranges");

        if (contiguous()) {
            std::copy_n(other.data(), N, data());
        }
        else {
            for (size_t i = 0;  i < this->dim(0);  ++i) {
                    this->assign(i, other[i]);
            }
        }
    }

    template<typename Float2>
    void assign(size_t i, Float2 other)
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

    bool contiguous() const
    {
        return strides_[0] == 1;
    }

    void fill(Float value)
    {
        if (contiguous())
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
    MatrixBase()
    {
        std::array<size_t, Dims> shape;
        std::fill(shape.begin(), shape.end(), 0);
        init(shape);
    }

    MatrixBase(const Extents<Dims> & extents)
    {
        init(extents.dims);
    }

    MatrixBase(const std::array<size_t, Dims> & shape)
    {
        init(shape);
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
        resize(other.shape());
        
        // Copy each slice on this dimension
        for (size_t i = 0;  i < this->dim(0);  ++i) {
            this->assign(i, other.operator [] (i));
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

    template<size_t Dim, typename First, typename... Rest>
    void make_shape(std::array<size_t, Dims> & shape, First&&first, Rest&&... rest)
    {
        static_assert(Dim < Dims, "Too many dimensions passed to Matrix");
        shape[Dim] = first;
        make_shape<Dim + 1>(shape, std::forward<Rest>(rest)...);
    }

    template<size_t Dim>
    void make_shape(std::array<size_t, Dims> &)
    {
        static_assert(Dim == Dims, "Not enough dimensions passed to Matrix");
    }

    template<typename... Args>
    MatrixBase(size_t shape0, Args&&... args)
    {
        std::array<size_t, Dims> shape;
        shape[0] = shape0;
        make_shape<1>(shape, std::forward<Args>(args)...);
        init(shape);
    }

    template<typename... ArgDims>
    void resize(ArgDims... args)
    {
        std::array<size_t, Dims> shape{ args... };
        resize(shape);
    }

    void resize(const Extents<Dims> & extents)
    {
        resize(extents.dims);
    }

    void resize(const std::array<size_t, Dims> & dims)
    {
        // TODO: don't copy when it's not needed
        MatrixBase newMatrix(dims);
        size_t n = std::min<size_t>(this->dim(0), newMatrix.dim(0));
        for (size_t i = 0;  i < n;  ++i) {
            newMatrix.assign(i, this->operator [] (i));
        }
        for (size_t i = n;  i < newMatrix.dim(0);  ++i) {
            newMatrix[i].fill(0);
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
    void init(std::array<size_t, Dims> shape)
    {
        state_.reset(new State(shape));
        this->data_ = state_->data;
        this->strides_ = state_->strides;
        this->dims_ = state_->dims;
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
        init(getShape(other));
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
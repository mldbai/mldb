
// This file is part of MLDB. Copyright 2016 MLDB.ai. All rights reserved.

#pragma once

#include <utility>
#include <vector>
#include <cstddef>
#include "mldb/base/exc_assert.h"

namespace MLDB {

template <typename T, typename INDEX_TYPE, typename COORDTYPE>
class Sparse3DGrid {
public :
typedef std::pair<COORDTYPE, T> VALUE_TYPE;
typedef std::vector< VALUE_TYPE > VECTOR_TYPE;

    Sparse3DGrid(size_t numBucketXY, size_t numBucketZ, INDEX_TYPE cellSize) 
        : numBucketXY_(numBucketXY), 
          numBucketZ_(numBucketZ), 
          numBucketTotal(numBucketXY*numBucketXY*numBucketZ_),
          cellSize_(cellSize)  {

          data_.resize(numBucketTotal);
    }

    void insert(const COORDTYPE& coord, const T& value) {

        using namespace std;

        size_t xBucket = (coord.x() / cellSize_) % numBucketXY_;
        size_t yBucket = (coord.y() / cellSize_) % numBucketXY_;
        size_t zBucket = (coord.z() / cellSize_) % numBucketZ_;

        size_t bucket = (xBucket*numBucketXY_*numBucketZ_) + 
                        (yBucket * numBucketZ_) + 
                        (zBucket);

        ExcAssert(bucket < numBucketTotal);
        data_[bucket].emplace_back(coord, value);
    }

    class Iterator {
    public:

        Iterator(Sparse3DGrid<T, INDEX_TYPE, COORDTYPE> & owner) : owner_(owner), currentbucket(0) {                
            subIter = owner_.data_[currentbucket].begin();
            nextValid();
        }

        VALUE_TYPE& operator * () const {
            return *subIter;
        }

        VALUE_TYPE& operator -> () const {
            return *subIter;
        }

        VALUE_TYPE operator ++ () {
            
            advance();

            if (currentbucket < owner_.numBucketTotal)
                return *subIter;
            else 
                return VALUE_TYPE();
        }

        VALUE_TYPE operator ++ (int) {
            VALUE_TYPE tmp;
            if (currentbucket < owner_.numBucketTotal) {
                tmp = *subIter;
                advance();
            }
            return tmp;
        }

        bool operator == (const Iterator& right) {
            return owner_ == right.owner_ && currentbucket == right.currentbucket && subIter == right.subIter;
        }

         bool operator < (const Iterator& right) {
            return owner_ == right.owner_ && (currentbucket < right.currentbucket || ( currentbucket == right.currentbucket && subIter < right.subIter ));
        }

        private:

            void advance() {
                if (currentbucket < owner_.numBucketTotal) {
                    ++subIter;
                    nextValid();
                }
            }

            void nextValid() {
                while(currentbucket < owner_.numBucketTotal && subIter == owner_.data_[currentbucket].end()) {
                    ++currentbucket;
                    subIter = owner_.data_[currentbucket].begin();
                }
            }

            Sparse3DGrid<T, INDEX_TYPE, COORDTYPE> & owner_;
            int currentbucket;
            typename VECTOR_TYPE::Iterator subIter;
    };

    VECTOR_TYPE queryBoundingBox(const COORDTYPE& min, const COORDTYPE& max) const {

        //todo: duplicate cells. Should only happen on really large queries       
        using namespace std;

        VECTOR_TYPE output;

        size_t xCellMin = (min.x() / cellSize_);
        size_t yCellMin = (min.y() / cellSize_);
        size_t zCellMin = (min.z() / cellSize_);

        size_t xCellMax = (max.x() / cellSize_);
        size_t yCellMax = (max.y() / cellSize_);
        size_t zCellMax = (max.z() / cellSize_);

        for (int x = xCellMin; x <= xCellMax; ++x) {
            for (int y = yCellMin; y <= yCellMax; ++y) {
                for (int z = zCellMin; z <= zCellMax; ++z) {
                    size_t xBucket = x % numBucketXY_;
                    size_t yBucket = y % numBucketXY_;
                    size_t zBucket = z % numBucketZ_;

                    size_t bucket = (xBucket*numBucketXY_*numBucketZ_) + 
                                    (yBucket * numBucketZ_) + 
                                    (zBucket);

                    for (const VALUE_TYPE& v : data_[bucket]) {
                        if (greater(v.first , min) && smaller(v.first, max)) {
                            output.push_back(v);
                        }
                    }

                    ExcAssert(bucket < numBucketTotal);
                }            
            }   
        }

        return output;
    }

    static bool smaller (const COORDTYPE& l, const COORDTYPE& r) {
        return (l.x() <= r.x()) && (l.y() <= r.y()) && (l.z() <= r.z());
    }

    static bool greater (const COORDTYPE& l, const COORDTYPE& r) {
        return (l.x() >= r.x()) && (l.y() >= r.y()) && (l.z() >= r.z());
    }

    Iterator begin() {
        return Iterator(*this);
    }

    Iterator end() {
        Iterator it(*this);
        it.currentbucket = numBucketTotal;
        return it;
    }

private:

    const size_t numBucketXY_;
    const size_t numBucketZ_;
    const size_t numBucketTotal;
    const INDEX_TYPE cellSize_;

    std::vector<VECTOR_TYPE> data_;

};

}
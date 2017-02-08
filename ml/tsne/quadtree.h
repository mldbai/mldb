/** quadtree.h                                                     -*- C++ -*-
    Jeremy Barnes, 17 November 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Released under the BSD license, no attribution required.
*/

#pragma once

#include "mldb/utils/compact_vector.h"
#include "mldb/base/exc_assert.h"
#include "mldb/jml/db/persistent_fwd.h"
#include <memory>
#include <iostream>
#include <cmath>

namespace ML {

using MLDB::compact_vector;

typedef compact_vector<float, 3, uint32_t, false> QCoord;

struct QuadtreeNode {

    /** Construct with a single child. */
    QuadtreeNode(QCoord mins, QCoord maxs, QCoord child)
        : mins(mins), maxs(maxs), type(TERMINAL),
          numChildren(1), child(child), centerOfMass(child),
          quadrants(1 << mins.size())
    {
        center.resize(mins.size());
        for (unsigned i = 0;  i < mins.size();  ++i) {
            center[i] = 0.5 * (mins[i] + maxs[i]);
            ExcAssert(std::isfinite(mins[i]));
            ExcAssert(std::isfinite(maxs[i]));
            ExcAssert(std::isfinite(child[i]));
        }
        
        ExcAssert(contains(child));

        diag = diagonalLength();
    }

    /** Construct empty. */
    QuadtreeNode(QCoord mins, QCoord maxs)
        : mins(mins), maxs(maxs), type(EMPTY), numChildren(0),
          centerOfMass(mins.size()),
          quadrants(1 << mins.size())
    {
        center.resize(mins.size());
        for (unsigned i = 0;  i < mins.size();  ++i) {
            center[i] = 0.5 * (mins[i] + maxs[i]);
        }

        diag = diagonalLength();
    }

    QuadtreeNode(DB::Store_Reader & store,
                 int version);

    ~QuadtreeNode()
    {
        for (auto & q: quadrants)
            if (q)
                delete q;
    }

    int numDimensions() const { return mins.size(); }

    QCoord mins;   ///< Minimum coordinates for bounding box
    QCoord maxs;   ///< Maximum coordinates for bounding box
    QCoord center; ///< Cached pre-computation of center of bounding box
    float  diag;   ///< Diagonal distance across box
    
    enum Type {
        EMPTY = 0,     ///< Nothing in it
        NODE = 1,      ///< It's a node with child segments
        TERMINAL = 2   ///< It's a terminal node
    } type;

    int numChildren;   ///< Number of children in this part of tree
    QCoord child;         ///< Child node itself
    QCoord centerOfMass;  ///< Center of mass of children, or child if terminal
    float recipNumChildren[2];  //< 1/numChildren, 1/(numChildren-1

    /** The different quadrants for when we're a NODE. */
    compact_vector<QuadtreeNode *, 4, uint32_t, true> quadrants;

    /** Insert the given point into the tree. */
    void insert(QCoord point, int depth = 0, int n = 1)
    {
        for (unsigned i = 0;  i < point.size();  ++i)
            ExcAssert(std::isfinite(point[i]));

        ExcAssertEqual(point.size(), mins.size());

        if (depth > 100) {
            using namespace std;
            cerr << "infinite depth tree: point " << point
                 << " center " << center << " mins " << mins
                 << " maxs " << maxs << endl;
            ExcAssert(false);
        }

        // Make sure that the point fits within the cell
        for (unsigned i = 0;  i < point.size();  ++i) {
            if (point[i] < mins[i] || point[i] >= maxs[i]) {
                using namespace std;
                cerr << "depth = " << depth << endl;
                cerr << "point = " << point << endl;
                cerr << "mins " << mins << endl;
                cerr << "maxs " << maxs << endl;
                throw MLDB::Exception("point is not within cell");
            }
        }

        if (type == EMPTY) {
            // Easy case: first insertion into root of tree
            type = TERMINAL;
            child = point;
            centerOfMass = point;
            numChildren = n;
        }
        else if (type == NODE) {
            // Insertion into an existing quad
            int quad = quadrant(center, point);
            if (!quadrants[quad]) {
                // Create a new quadrant
                QCoord newMins(point.size());
                QCoord newMaxs(point.size());

                for (unsigned i = 0;  i < point.size();  ++i) {
                    bool less = quad & (1 << i);

                    newMins[i] = less ? mins[i] : center[i];
                    newMaxs[i] = less ? center[i] : maxs[i];
                }

                quadrants[quad] = new QuadtreeNode(newMins, newMaxs, point);
            } else {
                // Recurse down into existing quadrant
                quadrants[quad]->insert(point, depth + 1);
            }

            numChildren += n;
            
            for (unsigned i = 0;  i < point.size();  ++i) {
                centerOfMass[i] += n * point[i];
            }
        }
        else if (type == TERMINAL) {
            // Is this the same point?  If so, we add a count to it
            if (point == child) {
                numChildren += n;
                for (unsigned i = 0;  i < point.size();  ++i)
                    centerOfMass[i] += n * point[i];
            }
            else {
                // First we convert to a non-terminal
                convertToNonTerminal(depth);
                
                // Then we insert a new one
                insert(point, depth, n);
            }
        }
    }
    
    /** Walk the tree.  The function takes a QuadtreeNode and returns a
        bool as to whether to stop descending or not.
    */
    template<typename Fn>
    void walk(const Fn & fn, int depth = 0)
    {
        if (!fn(*this, depth))
            return;
        if (type == NODE) {
            for (auto & q: quadrants) {
                if (q) q->walk(fn, depth + 1);
            }
        }
    }

    /** Finish the structure, including calculating node numbers */
    int finish(int currentNodeNumber = 0)
    {
        recipNumChildren[0] = 1.0 / numChildren;
        recipNumChildren[1] = 1.0 / (numChildren - 1);

        currentNodeNumber += 1;

        for (unsigned i = 0;  i < (1 << center.size());  ++i) {
            if (quadrants[i])
                currentNodeNumber
                    = quadrants[i]->finish(currentNodeNumber);
        }
        
        return currentNodeNumber;
    }

    static float sqr(float val)
    {
        return val * val;
    }

    double diagonalLength() const
    {
        if (mins.size() == 2) {
            return sqrt(sqr(maxs[0] - mins[0]) + sqr(maxs[1] - mins[1]));
        }

        double result = 0.0;
        for (unsigned i = 0;  i < mins.size();  ++i) {
            float dist = maxs[i] - mins[i];
            result += dist * dist;
        }

        return sqrt(result);
    }

    /** Convert a node to a non-terminal. */
    void convertToNonTerminal(int depth)
    {
        ExcAssertEqual(type, TERMINAL);

        QCoord oldChild = child;
        int n = numChildren;
        centerOfMass.clear();
        centerOfMass.resize(oldChild.size());
        numChildren = 0;

        // Convert to a non-terminal
        type = NODE;

        // Insert the current child and clear it
        insert(oldChild, depth, n);
    }

    template<typename Coord>
    int quadrant(const Coord & point) const
    {
        return quadrant(center, point);
    }

    // Return which quadrant the given point is in
    template<typename Coord>
    static int quadrant(const QCoord & center, const Coord & point)
    {
        ExcAssertEqual(center.size(), point.size());
        int result = 0;
        for (unsigned i = 0;  i < center.size();  ++i) {
            result = result | ((point[i] < center[i]) << i);
        }
        return result;
    }

    // Is this point in the quadrant?
    template<typename Point>
    bool contains(const Point & point) const
    {
        if (point.size() == 2) {
            return point[0] >= mins[0]
                && point[1] >= mins[1]
                && point[0] < maxs[0]
                && point[1] < maxs[1];
        }

        for (unsigned i = 0;  i < mins.size();  ++i) {
            if (point[i] < mins[i])
                return false;
            if (point[i] >= maxs[i])
                return false;
        }

        return true;
    }

    size_t memusage() const
    {
        size_t result = sizeof(*this);
        for (auto & q: quadrants) {
            if (q)
                result += q->memusage();
        }

        return result;
    }

    void serialize(DB::Store_Writer & store) const;
    void reconstitute(DB::Store_Reader & store, int version);
};

struct Quadtree {

    Quadtree(QCoord mins, QCoord maxs)
    {
        root.reset(new QuadtreeNode(mins, maxs));
    }

    Quadtree(DB::Store_Reader & store);

    void insert(QCoord coord)
    {
        root->insert(coord);
    }

    std::unique_ptr<QuadtreeNode> root;

    void serialize(DB::Store_Writer & store) const;
    void reconstitute(DB::Store_Reader & store);
};

} // namespace ML

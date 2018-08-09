// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** quadtree.cc
    Jeremy Barnes, 6 February 2015
    Copyright (c) 2015 mldb.ai inc.
    
    Released under the BSD license, no attribution required.
*/

#include "quadtree.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/arch/backtrace.h"
#include "mldb/jml/utils/compact_vector_persistence.h"

using namespace ML::DB;
using namespace std;

namespace ML {

void
QuadtreeNode::
serialize(DB::Store_Writer & store) const
{
    switch (type) {
    case EMPTY:
        store << compact_size_t(0);
        return;
    case NODE:
        store << compact_size_t(1);
        store << compact_size_t(numChildren)
              << centerOfMass << mins << maxs;
        for (auto & q: quadrants) {
            if (!q) {
                store << compact_size_t(0);
            }
            else {
                store << compact_size_t(1);
                q->serialize(store);
            }
        }
        return;
    case TERMINAL:
        store << compact_size_t(2);
        store << compact_size_t(numChildren)
              << child << mins << maxs;
        ExcAssertEqual(child.size(), mins.size());
        ExcAssertEqual(child.size(), maxs.size());
        return;
    }
    throw MLDB::Exception("Unknown quadtree node size");
}

QuadtreeNode::
QuadtreeNode(DB::Store_Reader & store, int version)
    : diag(0.0), type(EMPTY), numChildren(0), recipNumChildren{0, 0}
{
    if (version != 0)
        throw MLDB::Exception("Unknown quadtree node version");

    DB::compact_size_t type(store);
    if (type == 0) {
        throw MLDB::Exception("Reconstituting an empty quadtree node");
    }
    else if (type == 1) {
        type = NODE;
        compact_size_t nc(store);
        numChildren = nc;
        store >> centerOfMass >> mins >> maxs;
        child.clear();

        quadrants.resize(1 << centerOfMass.size());

        for (auto & q: quadrants) {
            compact_size_t indicator(store);
            if (indicator == 0) {
                q = nullptr;
                continue;
            }
            ExcAssertEqual(indicator, 1);
            q = new QuadtreeNode(store, version);
        }
    }
    else if (type == 2) {
        type = TERMINAL;
        compact_size_t nc(store);
        numChildren = nc;
        store >> child >> mins >> maxs;
        mins = maxs = center = centerOfMass = child;

    }
    else throw MLDB::Exception("Unknown quadtree node type");

    center.resize(mins.size());
    for (unsigned i = 0;  i < mins.size();  ++i) {

        // MLDB-926.  Can be removed once we verify it doesn't happen any more
        if (!std::isfinite(mins[i]) || !std::isfinite(maxs[i])
            || (!child.empty() && !std::isfinite(child[i]))) {
            cerr << "numChildren = " << numChildren << endl;
            cerr << "centerOfMass = " << centerOfMass << endl;
            cerr << "type = " << type << endl;
            cerr << "mins = " << mins << endl;
            cerr << "maxs = " << maxs << endl;
            cerr << "child = " << child << endl;
            cerr << "center = " << center << endl;

            MLDB::backtrace();
        }
        
        center[i] = 0.5 * (mins[i] + maxs[i]);
        ExcAssert(std::isfinite(mins[i]));
        ExcAssert(std::isfinite(maxs[i]));
        if (!child.empty())
            ExcAssert(std::isfinite(child[i]));
    }

    diag = diagonalLength();
    recipNumChildren[0] = 1.0 / numChildren;
    recipNumChildren[1] = 1.0 / (numChildren - 1);
}

Quadtree::
Quadtree(DB::Store_Reader & store)
{
    std::string canary;
    store >> canary;
    if (canary != "QTREE")
        throw MLDB::Exception("Unknown quadtree canary");
    DB::compact_size_t version(store);
    if (version != 0)
        throw MLDB::Exception("Unknown quadtree version");

    DB::compact_size_t indicator(store);
    if (indicator == 0)
        return;
    else if (indicator == 1) {
        root.reset(new QuadtreeNode(store, version));
    }
    else throw MLDB::Exception("Unknown quadtree indicator");
}

void
Quadtree::
serialize(DB::Store_Writer & store) const
{
    store << std::string("QTREE") << compact_size_t(0);  // version
    if (root) {
        store << compact_size_t(1);
        root->serialize(store);
    }
    else store << compact_size_t(0);
}

} // namespace ML


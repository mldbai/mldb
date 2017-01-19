// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* tree.cc
   Jeremy Barnes, 24 April 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.
   $Source$

   Tree class.
*/

#include "tree.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/ml/jml/feature_space.h"

#include <mutex>
#include <boost/pool/object_pool.hpp>


using namespace std;
using namespace ML::DB;


namespace ML {


/*****************************************************************************/
/* TREE::NODE                                                                */
/*****************************************************************************/

void Tree::Node::
serialize(DB::Store_Writer & store, const Feature_Space & fs) const
{
    store << compact_size_t(4);  // version
    split.serialize(store, fs);
    store << z << examples << pred;
    child_true.serialize(store, fs);
    child_false.serialize(store, fs);
    child_missing.serialize(store, fs);
}

void Tree::Node::
reconstitute(DB::Store_Reader & store, const Feature_Space & fs,
             Tree & parent)
{
    compact_size_t version(store);
    switch (version) {
    case 3: {
        split.reconstitute(store, fs);
        store >> z >> examples;
        child_true.reconstitute(store, fs, parent);
        child_false.reconstitute(store, fs, parent);
        child_missing.reconstitute(store, fs, parent);
        pred.clear();
        break;
    }
    case 4: {
        split.reconstitute(store, fs);
        store >> z >> examples >> pred;
        child_true.reconstitute(store, fs, parent);
        child_false.reconstitute(store, fs, parent);
        child_missing.reconstitute(store, fs, parent);
        break;
    }
    default:
        throw Exception("Attempt to reconstitute decision tree node of unknown "
                        "version " + ostream_format(version.size_));
    }
}


/*****************************************************************************/
/* TREE::BASE                                                                */
/*****************************************************************************/

void Tree::Base::
serialize(DB::Store_Writer & store) const
{
    store << compact_size_t(1);  // version
    store << pred << examples;
}

void Tree::Base::
reconstitute(DB::Store_Reader & store)
{
    compact_size_t version(store);
    switch (version) {
    case 1:
        store >> pred >> examples;
        break;
    default:
        throw Exception("Attempt to reconstitute decision tree base of unknown "
                        "version " + ostream_format(version.size_));
    }
}


/*****************************************************************************/
/* TREE::PTR                                                                 */
/*****************************************************************************/

void Tree::Ptr::
serialize(DB::Store_Writer & store, const Feature_Space & fs) const
{
    store << compact_size_t(1);  // version
    store << compact_size_t(is_node_);
    if (is_node_) node()->serialize(store, fs);
    else leaf()->serialize(store);
}

void Tree::Ptr::
reconstitute(DB::Store_Reader & store, const Feature_Space & fs,
             Tree & parent)
{
    compact_size_t version(store);
    switch (version) {
    case 1: {
        compact_size_t is_node(store);
        is_node_ = is_node;
        if (is_node_) {
            ptr_.node = parent.new_node();
            node()->reconstitute(store, fs, parent);
        }
        else {
            ptr_.leaf = parent.new_leaf();
            leaf()->reconstitute(store);
        }
        break;
    }
    default:
        throw Exception("Attempt to reconstitute decision tree ptr of unknown "
                        "version " + ostream_format(version.size_));
    }
}

/*****************************************************************************/
/* TREE                                                                      */
/*****************************************************************************/

struct Tree::Itl {
    /// Lock for the allocator
    std::mutex lock;

    /* We make the tree object responsible for holding the entire amount
       of memory for its leafs and nodes.  This gives us the advantage
       of having everything close together in memory, which lets us make
       better use of the cache.
    */
    boost::object_pool<Node> node_pool; ///< Allocator for nodes
    boost::object_pool<Leaf> leaf_pool; ///< Allocator for leafs

    size_t nodes_allocated;
    size_t leafs_allocated;

    size_t nodes_destroyed;
    size_t leafs_destroyed;

    Itl()
        : nodes_allocated(0), leafs_allocated(0),
          nodes_destroyed(0), leafs_destroyed(0)
    {
    }
};

Tree::Tree()
    : itl(new Itl())
{
}

Tree::Ptr 
tree_copy_recursive(const Tree::Ptr & from,
                    Tree & to)
{
    if (from.node()) {
        const Tree::Node * nfrom = from.node();
        Tree::Node * result = to.new_node();
        *result = *from.node();
        result->child_true    = tree_copy_recursive(nfrom->child_true, to);
        result->child_false   = tree_copy_recursive(nfrom->child_false, to);
        result->child_missing = tree_copy_recursive(nfrom->child_missing, to);
        return result;
    }
    else if (from.leaf())
        return to.new_leaf(from.leaf()->pred, from.leaf()->examples);
    return Tree::Ptr();  // null
}

Tree::Tree(const Tree & other)
    : itl(new Itl())
{
    root = tree_copy_recursive(other.root, *this);
}

Tree::
Tree(Tree && other)
    : root(std::move(other.root)),
      itl(std::move(other.itl))
{
}

Tree &
Tree::
operator = (const Tree & other)
{
    if (&other == this) return *this;

    clear();
    root = tree_copy_recursive(other.root, *this);
    
    return *this;
}

Tree::~Tree()
{
    //clear();
}

void
Tree::
clear_recursive(Tree::Ptr & root)
{
    std::unique_lock<std::mutex> guard(itl->lock);
    if (root.node()) {
        Node * nroot = root.node();
        clear_recursive(nroot->child_true);
        clear_recursive(nroot->child_false);
        clear_recursive(nroot->child_missing);
        itl->node_pool.destroy(nroot);
        ++itl->nodes_destroyed;
    }
    else if (root.leaf()) {
        itl->leaf_pool.destroy(root.leaf());
        ++itl->leafs_destroyed;
    }
    
    root = Tree::Ptr();
}

void Tree::clear()
{
    clear_recursive(root);

    //cerr << "Tree::clear(): after clear: alloc " << nodes_allocated
    //     << "/" << leafs_allocated << " destr "
    //     << nodes_destroyed << "/" << leafs_destroyed << endl;
}

Tree::Node * Tree::new_node()
{
    std::unique_lock<std::mutex> guard(itl->lock);
    ++itl->nodes_allocated;
    return itl->node_pool.construct();
}

Tree::Leaf *
Tree::new_leaf(const distribution<float> & dist, float examples)
{
    std::unique_lock<std::mutex> guard(itl->lock);
    ++itl->leafs_allocated;
    return itl->leaf_pool.construct(dist, examples);
}

Tree::Leaf *
Tree::new_leaf()
{
    std::unique_lock<std::mutex> guard(itl->lock);
    ++itl->leafs_allocated;
    return itl->leaf_pool.construct();
}

void Tree::
serialize(DB::Store_Writer & store, const Feature_Space & fs) const
{
    store << compact_size_t(1);  // version
    root.serialize(store, fs);
    store << compact_size_t(3212345);  // end marker
    store << string("END_TREE");
}

void Tree::
reconstitute(DB::Store_Reader & store, const Feature_Space & fs)
{
    compact_size_t version(store);
    switch (version) {
    case 1: {
        root.reconstitute(store, fs, *this);
        break;
    }
    default:
        throw Exception("Decision tree: Attempt to reconstitute tree of "
                        "unknown version " + ostream_format(version.size_));
    }

    compact_size_t marker(store);
    if (marker != 3212345)
        throw Exception("Tree::reconstitute: "
                        "read bad marker at end");

    string s;
    store >> s;
    if (s != "END_TREE")
        throw Exception("Bad end Tree marker " + s);
}

std::string
printLabels(const distribution<float> & dist);

std::string print_outcome(const ML::Tree::Leaf & outcome)
{
    string result;
    const distribution<float> & dist = outcome.pred;
    result += format(" (weight = %.2f) ", outcome.examples);
    result += printLabels(dist);
    return result;
}

} // namespace ML

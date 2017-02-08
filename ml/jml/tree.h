/* tree.h                                                          -*- C++ -*-
   Jeremy Barnes, 24 April 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Class to hold and manage a tree.
*/

#pragma once


#include "feature_set.h"
#include "mldb/jml/stats/distribution.h"
#include "split.h"

namespace ML {

class Feature_Space;


/*****************************************************************************/
/* TREE                                                                      */
/*****************************************************************************/

/** Tree structure.  Contains the entire representation of a tree, as
    well as memory management machinery.
*/
struct Tree {
    Tree();
    Tree(const Tree & other);
    Tree(Tree && other);
    Tree & operator = (const Tree & other);
    ~Tree();

    void clear();

    struct Base;
    struct Node;
    struct Leaf;

    /** Pointer that points to either a leaf or a node. */
    struct Ptr {
        Ptr() : is_node_(false) { ptr_.node = 0; }
        Ptr(Ptr && other)
            : is_node_(other.is_node_), ptr_(other.ptr_)
        {
            other.ptr_.node = nullptr;
        }

        Ptr(const Ptr & other) = default;
        Ptr & operator = (const Ptr & other) = default;

        Ptr(Node * node) : is_node_(true) { ptr_.node = node; }
        Ptr(Leaf * leaf) : is_node_(false) { ptr_.leaf = leaf; }
        Node * node() const { if (!is_node_) return 0;  return ptr_.node; }
        Leaf * leaf() const { if (!is_node_) return ptr_.leaf;  return 0; }

        float examples() const
        {
            if (!ptr_.base) return 0;
            else return ptr_.base->examples;
        }

        const distribution<float> & pred() const
        {
            static const distribution<float> none;
            if (!ptr_.base) return none;
            return ptr_.base->pred;
        }

        operator bool () const { return ptr_.node; }
            
        void serialize(DB::Store_Writer & store,
                       const Feature_Space & fs) const;
        void reconstitute(DB::Store_Reader & store,
                          const Feature_Space & fs,
                          Tree & parent);
    private:
        bool is_node_;
        union {
            Base * base;
            Node * node;
            Leaf * leaf;
        } ptr_;
    };

    struct Base {
        Base() {}
        Base(const distribution<float> & pred, float examples)
            : pred(pred), examples(examples) {}

        distribution<float> pred;  ///< Probs for class, means for regress
        float examples;    ///< Num examples at this point

        void serialize(DB::Store_Writer & store) const;
        void reconstitute(DB::Store_Reader & store);
        
        /* Estimate of the amount of allocated memory. */
        size_t memusage() const;
    };

    /** Leaf node.  Contains a prediction for each class. */
    struct Leaf : public Base {
        Leaf() {}
        Leaf(const distribution<float> & pred, float examples)
            : Base(pred, examples)
        {
        }
    };

    /** Node.  Contains child pointers and a test. */
    struct Node : public Base {
        Split split;       ///< Value to split on
        float z;           ///< Z score for this split
        Ptr child_true;    ///< Result if true
        Ptr child_false;   ///< Result if false
        Ptr child_missing; ///< Result if missing

        void serialize(DB::Store_Writer & store,
                       const Feature_Space & fs) const;
        void reconstitute(DB::Store_Reader & store,
                          const Feature_Space & fs,
                          Tree & parent);

        /* Estimate of the amount of allocated memory. */
        size_t memusage() const;
    };

    Ptr root;  ///< The root node of the tree

    /** Allocate a new tree node. */
    Node * new_node();
        
    /** Allocate a new tree leaf. */
    Leaf * new_leaf(const distribution<float> & dist, float examples);

    /** Allocate a new tree leaf. */
    Leaf * new_leaf();

    /** Helper function to serialize the tree.  We need to pass the feature
        space so we know how to serialize the features. */
    void serialize(DB::Store_Writer & store, const Feature_Space & fs) const;
    /** Helper function to reconstitute the tree.  Same comment appplies to
        the feature space. */
    void reconstitute(DB::Store_Reader & store, const Feature_Space & fs);

    /* Estimate of the amount of allocated memory. */
    size_t memusage() const;

private:
    struct Itl;
    std::unique_ptr<Itl> itl;

    void clear_recursive(Tree::Ptr & root);
};

std::string print_outcome(const ML::Tree::Leaf & outcome);


} // namespace ML

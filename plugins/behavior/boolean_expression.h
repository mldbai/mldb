// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* boolean_expression.h                                            -*- C++ -*-
   Jeremy Barnes, 18 August 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

*/

#ifndef __behaviors__boolean_expression_h__
#define __behaviors__boolean_expression_h__

#include <memory>
#include <vector>
#include <limits>
#include "mldb/plugins/behavior/id.h"
#include "behavior_domain.h"
#include <boost/any.hpp>

namespace MLDB {


struct BooleanExpression;
typedef std::shared_ptr<BooleanExpression> BoolExprPtr;


/*****************************************************************************/
/* BEHAVIOR WRAPPER                                                         */
/*****************************************************************************/

/** Abstracts out the BehaviorDomain to allow boolean expressions to be
    applied to other things.
*/
struct BehaviorWrapper {
    BehaviorWrapper(const BehaviorDomain & behs);
    BehaviorWrapper();

    std::function<std::vector<SH>(SH maxSubject)> allSubjectHashes;

    std::function<std::vector<std::pair<SH, Date> > (BH beh, SH maxSubject)>
    getSubjectHashesAndTimestamps;

    std::function<std::vector<std::pair<SH, Date> > (BH beh, SH maxSubject)>
    getSubjectHashesAndAllTimestamps;

    std::function<std::vector<Id>(const std::string & regex)>
    getBehaviorsMatchingRegex;

    std::function<std::vector<Id>(const std::string & regex)>
    getBehaviorsContainingString;

private:
    const BehaviorDomain * behs;
};


/*****************************************************************************/
/* BOOLEAN EXPRESSION                                                        */
/*****************************************************************************/

struct BooleanExpression {

    virtual ~BooleanExpression()
    {
    }

    /** Print out a human readable version of the expression. */
    virtual std::string print() const = 0;
    virtual std::string printSql() const = 0;

    /** Parse an expresson and return a pointer to the root of the tree. */
    static BoolExprPtr parse(const std::string & expression);

    /** Can this expression generate a list, or can it only filter a
        pre-existing list?  Default says it can generate.
    */
    virtual bool canGenerate() const
    {
        return true;
    }

    /** Return all segments that are implicated in this expression. */
    virtual std::set<Id> getSegments() const = 0;

    /** Create a deep copy of this boolean expression. */
    virtual BoolExprPtr makeCopy() const = 0;

    /** Bind the boolean expression to a given behavior domain,
        creating a copy.  This is used for expressions that need to know
        which behaviors they are running on, for example in order to
        apply a regex over the behavior IDs.

        md is metadata which may be passed through for when you need
        to supply context dependent information in to the bind.

        Default simply calls makeCopy() and is suitable for when there
        is no behavior specific binding necessary.
    */
    virtual BoolExprPtr bind(const BehaviorWrapper & behs,
                             const boost::any & md = boost::any()) const;

    BoolExprPtr bindNoMetadata(const BehaviorWrapper & behs) const
    {
        return bind(behs);
    }

    BoolExprPtr bindNoMetadataBehs(const BehaviorDomain & behs) const
    {
        return bindNoMetadata(behs);
    }

    /** Return the set of matching users (and the date) from the given
        behavior domain.  Note that a given user may appear more than
        once.
    */
    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const = 0;

    std::vector<std::pair<SH, Date> >
    generateBehs(const BehaviorDomain & behs,
                 SH maxSubject) const
    {
        return generate(behs, maxSubject);
    }

    /** Same as generate but makes sure the SH are unique. Will keep the 
        earliest timestamp for each SH */
    virtual std::vector<std::pair<SH, Date> >
    generateUnique(const BehaviorWrapper & behs,
                   SH maxSubject);

    /** Return the set of all matching users and timestamps for the
        given behavior domain.  This differs from generate in that
        all possible timestamps MUST be there.
    */
    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const = 0;

    /** Filter a given list, leaving only the values that remain.  Default
        implementation calls generate() and then removes those values
        from the list passed in.
    */
    virtual std::vector<std::pair<SH, Date> >
    filter(const BehaviorWrapper & behs,
           const std::vector<std::pair<SH, Date> > & input) const;
    
    /** Filter a given list, where the later segments need to occur after
        the earlier ones, leaving only the values that remain.  Default
        implementation calls generate() and then removes those values
        from the list passed in.
    */
    virtual std::vector<std::pair<SH, Date> >
    filter_after(const BehaviorWrapper & behs,
           const std::vector<std::pair<SH, Date> > & input) const;
    
    virtual std::vector<std::pair<SH, Date> >
    filter_after_within(const BehaviorWrapper & behs,
           const std::vector<std::pair<SH, Date> > & input,
           unsigned within) const;
};


/*****************************************************************************/
/* SEG EXPRESSION                                                            */
/*****************************************************************************/

/** An expression that matches a given segment. */

struct SegExpression : public BooleanExpression {
    SegExpression(Id seg);
    
    virtual std::string print() const;
    virtual std::string printSql() const;

    virtual std::set<Id> getSegments() const
    {
        return { seg };
    }

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<SegExpression>(*this);
    }

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;

    Id seg;
};

/*****************************************************************************/
/* CONTAINS EXPRESSION                                                       */
/*****************************************************************************/

/** An expression that matches part of each segment's name. This expands into
 * the equivalent result of an OR expression */

struct SegNameContainsExpression : public BooleanExpression {
    SegNameContainsExpression(const std::string & mustContain);
    
    virtual std::string print() const;
    virtual std::string printSql() const;

    virtual std::set<Id> getSegments() const;

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<SegNameContainsExpression>(*this);
    }

    virtual BoolExprPtr bind(const BehaviorWrapper & behs,
                             const boost::any & md = boost::any()) const;

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;

    std::string mustContain;
};

/*****************************************************************************/
/* REGEX EXPRESSION                                                          */
/*****************************************************************************/

/** An expression that matches a segment's name using a regex.*/

struct RegexExpression : public BooleanExpression {
    RegexExpression(const std::string & mustContain);
    
    virtual std::string print() const;
    virtual std::string printSql() const;

    virtual std::set<Id> getSegments() const;

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<RegexExpression>(*this);
    }

    virtual BoolExprPtr bind(const BehaviorWrapper & behs,
                             const boost::any & md = boost::any()) const;

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;

    std::string regex;
};


/******************************************************************************/
/* TIMES FUNCTION EXPRESSION                                                  */
/******************************************************************************/

/** Returns all subject that contains a given segment on at least n
    different timestamps.
*/
struct TimesFunctionExpression : public BooleanExpression
{
    TimesFunctionExpression(BoolExprPtr base, unsigned n);

    virtual std::string print() const;
    virtual std::string printSql() const;

    virtual std::set<Id> getSegments() const
    {
        return base->getSegments();
    }

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<TimesFunctionExpression>(*this);
    }

    virtual bool canGenerate() const
    {
        return true;
    }

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs, SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;

    virtual BoolExprPtr bind(const BehaviorWrapper & behs,
                             const boost::any & md = boost::any()) const;

private:

    BoolExprPtr base;
    unsigned n;
};


/*****************************************************************************/
/* NOT EXPRESSION                                                            */
/*****************************************************************************/

/** The NOT expression takes the INVERSE membership of the given child set.

    There are various things that can be done about timing:
    - We can assume that the entry into the set 
*/
struct NotExpression : public BooleanExpression {
    NotExpression(BoolExprPtr base);

    virtual std::string print() const;
    virtual std::string printSql() const;

    virtual bool canGenerate() const
    {
        return true;
    }

    virtual std::set<Id> getSegments() const
    {
        return base->getSegments();
    }

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<NotExpression>(base->makeCopy());
    }

    virtual std::vector<std::pair<SH, Date> >
    filter(const BehaviorWrapper & behs,
           const std::vector<std::pair<SH, Date> > & input) const;

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;

    BoolExprPtr base;

    virtual BoolExprPtr bind(const BehaviorWrapper & behs,
                             const boost::any & md = boost::any()) const;
};


/*****************************************************************************/
/* COMPOUND EXPRESSION                                                       */
/*****************************************************************************/

struct CompoundExpression : public BooleanExpression {
    CompoundExpression(const std::string & separator,
                       const std::vector<BoolExprPtr> & exprs)
        : separator(separator), exprs(exprs)
    {
    }

    CompoundExpression(const std::string & separator,
                       std::vector<BoolExprPtr> && exprs)
        : separator(separator), exprs(move(exprs))
    {
    }

    virtual std::set<Id> getSegments() const
    {
        std::set<Id> result;
        for (auto e: exprs) {
            auto s = e->getSegments();
            result.insert(s.begin(), s.end());
        }
        return result;
    }

    /** Create a copy of the entire list of expressions. */
    std::vector<BoolExprPtr> copyExprs() const
    {
        std::vector<BoolExprPtr> newExprs;
        for (auto & e: exprs)
            newExprs.push_back(e->makeCopy());
        return newExprs;
    }

    std::string print_(bool isSql) const;
    virtual std::string print() const {return print_(false);}
    virtual std::string printSql() const {return print_(true);}

    virtual BoolExprPtr bind(const BehaviorWrapper & behs,
                             const boost::any & md = boost::any()) const;

    std::string separator;
    std::vector<BoolExprPtr> exprs;
};


/*****************************************************************************/
/* AND EXPRESSION                                                            */
/*****************************************************************************/

/** The AND expression requires all children to be in the set.  It takes the
    LATEST time for each child.
*/

struct AndExpression : public CompoundExpression {
    AndExpression(const std::vector<BoolExprPtr> & exprs);

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<AndExpression>(copyExprs());
    }

    virtual std::vector<std::pair<SH, Date> >
    filter(const BehaviorWrapper & behs,
           const std::vector<std::pair<SH, Date> > & input) const;
};


/*****************************************************************************/
/* OR EXPRESSION                                                             */
/*****************************************************************************/

/** The OR expression requires one of the children to be in the set.  It takes
    the EARLIEST time for each child.
*/

struct OrExpression : public CompoundExpression {
    OrExpression(const std::vector<BoolExprPtr> & exprs);

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<OrExpression>(copyExprs());
    }

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;
};

/*****************************************************************************/
/* THEN EXPRESSION                                                            */
/*****************************************************************************/

/** The THEN expression requires all children to occur after the previous one.
    It takes the EARLIEST ACCEPTABLE time for each child.
*/

struct ThenExpression : public CompoundExpression {
    ThenExpression(const std::vector<BoolExprPtr> & exprs);

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<ThenExpression>(copyExprs());
    }

    virtual std::vector<std::pair<SH, Date> >
    filter_after(const BehaviorWrapper & behs,
           const std::vector<std::pair<SH, Date> > & input) const;
};


/*****************************************************************************/
/* WITHIN EXPRESSION                                                         */
/*****************************************************************************/

/** The WITHIN expression requires all children to occur after the previous,
    one, but not later than a specified maximum elapsed time. It takes the 
    EARLIEST ACCEPTABLE time for each child.
*/

struct WithinExpression : public CompoundExpression {
    WithinExpression(const std::vector<BoolExprPtr> & exprs, 
                     const std::vector<double> & within_secs);

    std::string print() const;
    virtual std::string printSql() const;

    virtual std::vector<std::pair<SH, Date> >
    generate(const BehaviorWrapper & behs,
             SH maxSubject) const;

    virtual std::vector<std::pair<SH, Date> >
    generateAllTimestamps(const BehaviorWrapper & behs,
                          SH maxSubject) const;

    virtual BoolExprPtr makeCopy() const
    {
        return std::make_shared<WithinExpression>(copyExprs(), within_secs);
    }

    virtual std::vector<std::pair<SH, Date> >
    filter_after_within(const BehaviorWrapper & behs,
           const std::vector<std::pair<SH, Date> > & input,
           unsigned within=std::numeric_limits<unsigned>::max()) const;

    std::vector<double> within_secs;
};

} // namespace MLDB

#endif /* __behaviors__boolean_expression_h__ */

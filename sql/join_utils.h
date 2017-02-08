/** join_utils.h                                                   -*- C++ -*-
    Jeremy Barnes, 24 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "sql_expression.h"
#include "mldb/sql/table_expression_operations.h" //for join qualification


namespace MLDB {

 enum JoinSide {
    JOIN_SIDE_LEFT = 0,
    JOIN_SIDE_RIGHT,
    JOIN_SIDE_MAX
};

/** Fix up an expression, by looking for all column references and removing
    the table name in the set of aliases from each one.
*/
std::shared_ptr<SqlExpression>
removeTableNameFromExpression(const SqlExpression & expr, const Utf8String & tableName);


/*****************************************************************************/
/* ANNOTATED CLAUSE                                                          */
/*****************************************************************************/

/** This structure annotates an ON clause with information about its role
    in the join and which variables it refers to.
*/

struct AnnotatedClause {
    AnnotatedClause()
        : role(CONSTANT), pivot(NOT_PIVOT)
    {
    }

    /// Annotate the clause.  The tables referred to on the left of the join
    /// are in leftTables, and those on the right in rightTables.
    AnnotatedClause(std::shared_ptr<SqlExpression> c,
                    const std::set<Utf8String> & leftTables,
                    const std::set<Utf8String> & rightTables,
                    bool debug = false);

    /// Original expression for the clause
    std::shared_ptr<SqlExpression> expr;

    /// Lists of variables on the left, the right, and satisfied by neither side
    std::vector<ColumnPath> leftVars, rightVars, externalVars;

    /// Lists of functions on the left, the right, and satisfied by neither side
    /// We still need to keep the scope, as table1.rowName() is not the same as
    /// rowName() for a joined table.
    std::vector<ScopedName> leftFuncs, rightFuncs, externalFuncs;
    
    /// Role that this clause plays in the overall join logic
    enum Role {
        CONSTANT,  ///< Expression does not depend on either table
        LEFT,      ///< Expression depends on left table only
        RIGHT,     ///< Expression depends on right table only
        CROSS      ///< Expression depends on both left and right tables
    } role;

    /// For a cross term, which role does it play in the pivoting?
    enum Pivot {
        UNKNOWN,        ///< Pivot status not analysed
        NOT_PIVOT,      ///< Not a pivot term
        CHOSEN_PIVOT,   ///< Chosen as the main pivot term
        POSSIBLE_PIVOT  ///< Possible pivot but not chosen
    } pivot;
};

DECLARE_ENUM_DESCRIPTION_NAMED(AnnotatedClauseRoleDescription, AnnotatedClause::Role);
DECLARE_ENUM_DESCRIPTION_NAMED(AnnotatedClausePivotDescription, AnnotatedClause::Pivot);
DECLARE_STRUCTURE_DESCRIPTION(AnnotatedClause);

std::shared_ptr<SqlExpression>
generateWhereExpression(const std::vector<AnnotatedClause> & clause,
                        const Utf8String & tableName);


/*****************************************************************************/
/* ANNOTATED JOIN CONDITION                                                  */
/*****************************************************************************/

/** A join condition, broken down and annotated with the role of each
    clause within it.

    The on and where clauses are combined to generate the overall
    condition for the join.
*/

struct AnnotatedJoinCondition {

    AnnotatedJoinCondition();

    AnnotatedJoinCondition(std::shared_ptr<TableExpression> left,
                           std::shared_ptr<TableExpression> right,
                           std::shared_ptr<SqlExpression> on,
                           std::shared_ptr<SqlExpression> where,
                           JoinQualification joinQualification,
                           bool debug = false);

    bool debug;

    /// Tell us what the style of the join is
    enum Style {
        EMPTY,        ///< Returns nothing
        CROSS_JOIN,   ///< Cross-join
        EQUIJOIN,     ///< Join on f(leftrow) = f(rightrow)
        UNKNOWN       ///< Unknown join style; naive implementation
    };

    /// Original ON clause
    std::shared_ptr<SqlExpression> on;

    /// Original WHERE clause as in 
    /// SELECT * FROM t1 JOIN t2 ON t1.c = t2.c WHERE c > 0
    std::shared_ptr<SqlExpression> where;

    /// Holds all the AND clauses in the ON and the WHERE statements
    std::vector<std::shared_ptr<SqlExpression> > andClauses;

    /// Style of join
    Style style;
    
    /// Side of a join
    struct Side {
        Side()
        {
        }

        Side(std::shared_ptr<TableExpression> table)
            : table(table)
        {
        }

        /// Original table of the join
        std::shared_ptr<TableExpression> table;

        /** Clause for the select expression
            This is used to evaluated each side of an EQUIJOIN.
        */
        std::shared_ptr<SqlExpression> selectExpression;

        /// Expression to select from the table on this side
        SelectExpression select;

        // left/right side WHEN condition as an SQL expression
        WhenExpression when;

        // left/right side WHERE condition as an SQL expression
        std::shared_ptr<SqlExpression> where;

        /// Order by clause for the table on this side
        OrderByExpression orderBy;
    };

    /// Left side of the join
    Side left;

    /// Right side of the join
    Side right;

    // ON condition, that both rows must satisfy
    std::vector<AnnotatedClause> crossConditions;

    // Constant conditions, that don't depend on either row
    std::vector<AnnotatedClause> constantConditions;

    /// Filter expression to apply AFTER the rows have been joined
    std::shared_ptr<SqlExpression> crossWhere;

    /// Filter expression to apply BEFORE any rows; it's constant
    /// with respect to the left and right tables.
    std::shared_ptr<SqlExpression> constantWhere;


    /** Recursive function to analyze the given clause and extract all of the
        AND clauses within it.
    */
    void analyze(const std::shared_ptr<SqlExpression> & expr);
};

DECLARE_STRUCTURE_DESCRIPTION(AnnotatedJoinCondition);

DECLARE_STRUCTURE_DESCRIPTION_NAMED(AnnotatedJoinConditionSideDescription,
                                    AnnotatedJoinCondition::Side);

DECLARE_ENUM_DESCRIPTION_NAMED(AnnotatedJoinConditionStyleDescription,
                              AnnotatedJoinCondition::Style);

} // namespace MLDB


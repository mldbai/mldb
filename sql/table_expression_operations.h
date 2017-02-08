/** table_expression_operations.h                                  -*- C++ -*-
    Jeremy Barnes, 27 July 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Operations on tables (implementations of a table expression).
*/

#pragma once

#include "mldb/sql/sql_expression.h"


namespace MLDB {

enum JoinQualification {
    JOIN_INNER,
    JOIN_LEFT,
    JOIN_RIGHT,
    JOIN_FULL
};

DECLARE_ENUM_DESCRIPTION(JoinQualification);

/*****************************************************************************/
/* NAMED DATASET EXPRESSION                                                  */
/*****************************************************************************/

struct NamedDatasetExpression : public TableExpression {

    NamedDatasetExpression(const Utf8String& asName);

    void setDatasetAlias(const Utf8String& newAlias) { asName = newAlias; }

    virtual Utf8String getAs() const { return asName; }

    Utf8String asName;
};

/*****************************************************************************/
/* DATASET EXPRESSION                                                        */
/*****************************************************************************/

/** Used when selecting directly from a dataset. */

struct DatasetExpression: public NamedDatasetExpression {
    DatasetExpression(Utf8String datasetName, Utf8String asName);
    DatasetExpression(Any config, Utf8String asName);

    virtual ~DatasetExpression();

    virtual BoundTableExpression
    bind(SqlBindingScope & context, const ProgressFunc & onProgress) const;
    
    virtual Utf8String print() const;

    virtual void printJson(JsonPrintingContext & context);

    virtual std::string getType() const;

    virtual Utf8String getOperation() const;
  
    virtual std::set<Utf8String> getTableNames() const;

    virtual UnboundEntities getUnbound() const;

    Any config;
    Utf8String datasetName;
};


/*****************************************************************************/
/* JOIN EXPRESSION                                                           */
/*****************************************************************************/

/** Used when joining datasets */

struct JoinExpression: public TableExpression {
    JoinExpression(std::shared_ptr<TableExpression> left,
                   std::shared_ptr<TableExpression> right,
                   std::shared_ptr<SqlExpression> on,
                   JoinQualification qualification);

    virtual ~JoinExpression();

    virtual BoundTableExpression
    bind(SqlBindingScope & context, const ProgressFunc & onProgress) const;
    
    virtual Utf8String print() const;

    virtual std::string getType() const;

    virtual Utf8String getOperation() const;

    virtual std::set<Utf8String> getTableNames() const;

    virtual UnboundEntities getUnbound() const;

    std::shared_ptr<TableExpression> left;
    std::shared_ptr<TableExpression> right;
    std::shared_ptr<SqlExpression> on;
    JoinQualification qualification;
};

/*****************************************************************************/
/* NO TABLE                                                                  */
/*****************************************************************************/

/** Used when there is no dataset */

struct NoTable: public TableExpression {
    virtual ~NoTable();

    virtual BoundTableExpression
    bind(SqlBindingScope & context, const ProgressFunc & onProgress) const;
    
    virtual Utf8String print() const;

    virtual void printJson(JsonPrintingContext & context);

    virtual std::string getType() const;

    virtual Utf8String getOperation() const;

    virtual std::set<Utf8String> getTableNames() const;

    virtual UnboundEntities getUnbound() const;
};


/*****************************************************************************/
/* SELECT SUBTABLE EXPRESSION                                                */
/*****************************************************************************/

/** Used when doing a select inside a FROM clause **/

struct SelectSubtableExpression: public NamedDatasetExpression {

    SelectSubtableExpression(SelectStatement statement,
                             Utf8String asName);

    virtual ~SelectSubtableExpression();

    virtual BoundTableExpression
    bind(SqlBindingScope & context, const ProgressFunc & onProgress) const;
    
    virtual Utf8String print() const;

    virtual std::string getType() const;

    virtual Utf8String getOperation() const;

    virtual std::set<Utf8String> getTableNames() const;

    virtual UnboundEntities getUnbound() const;

    SelectStatement statement;
};

/*****************************************************************************/
/* DATASET FUNCTION EXPRESSION                                               */
/*****************************************************************************/

/** Used when doing a select inside a FROM clause **/

struct DatasetFunctionExpression: public NamedDatasetExpression {

    DatasetFunctionExpression(Utf8String functionName,
                              std::vector<std::shared_ptr<TableExpression>>& args,
                              std::shared_ptr<SqlExpression> options);

    virtual ~DatasetFunctionExpression();

    virtual BoundTableExpression
    bind(SqlBindingScope & context, const ProgressFunc & onProgress) const;

    virtual Utf8String print() const;

    virtual std::string getType() const;

    virtual Utf8String getOperation() const;

    virtual std::set<Utf8String> getTableNames() const;

    virtual UnboundEntities getUnbound() const;

    Utf8String functionName;
    std::vector<std::shared_ptr<TableExpression> > args;
    std::shared_ptr<SqlExpression> options;
};

/*****************************************************************************/
/* ROW / ATOM DATASET                                                        */
/*****************************************************************************/

/** Used when we want to treat a row expression as a table:

    select * from row table ({x: 1})

    will return a table

    rowName value
          x     1

    There are two variants: ATOMS creates one row per atom, where as
    COLUMNS creates one row per column.
*/

struct RowTableExpression: public TableExpression {
    enum Style {
        ATOMS,
        COLUMNS
    };

    RowTableExpression(std::shared_ptr<SqlExpression> expr,
                       Utf8String asName,
                       Style style);

    virtual ~RowTableExpression();

    virtual BoundTableExpression
    bind(SqlBindingScope & context, const ProgressFunc & onProgress) const;
    
    virtual Utf8String print() const;

    virtual void printJson(JsonPrintingContext & context);

    virtual std::string getType() const;

    virtual Utf8String getOperation() const;

    virtual std::set<Utf8String> getTableNames() const;

    virtual UnboundEntities getUnbound() const;

    virtual Utf8String getAs() const { return asName; }

    std::shared_ptr<SqlExpression> expr;
    Utf8String asName;
    Style style;
};

} // namespace MLDB


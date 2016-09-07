#include "sql_config_validator.h"

using namespace std;

namespace Datacratic {
namespace MLDB {

void
QueryValidator::
operator()(const Optional<InputQuery> & query, const std::string & name) const
{
   if (query) operator()(*query, name);
}

void
NoGroupByHaving::
operator()(const InputQuery & query, const std::string & name) const
{
    if (query.stm) {
        if (!query.stm->groupBy.empty()) {
            throw ML::Exception(name + " does not support groupBy clause");
        }
        else if (!query.stm->having->isConstantTrue()) {
            throw ML::Exception(name + " does not support having clause");
        }
    }
}

void
NoWhere::
operator()(const InputQuery & query, const std::string & name) const
{
    if (query.stm) {
        if (!query.stm->where->isConstantTrue()) {
            throw ML::Exception(name + " does not support where");
        }
    }
}

void
NoLimit::
operator()(const InputQuery & query, const std::string & name) const
{
    if (query.stm) {
        if (query.stm->limit != -1) {
            throw ML::Exception(name + " does not support limit");
        }
    }
}

void
NoOffset::
operator()(const InputQuery & query, const std::string & name) const
{
    if (query.stm) {
        if (query.stm->offset > 0) {
            throw ML::Exception(name + " does not support offset");
        }
    }
}

void
MustContainFrom::
operator()(const InputQuery & query, const std::string & name) const
{
    if (!query.stm || !query.stm->from || query.stm->from->surface.empty())
        throw ML::Exception(name + " must contain a FROM clause");
}

void validatePlainColumnSelect(const std::shared_ptr<const SqlRowExpression> expr, 
                               const std::string & name) {

    auto getWildcard = [] (const std::shared_ptr<const SqlExpression> expression)
        -> std::shared_ptr<const WildcardExpression>
        {
            return std::dynamic_pointer_cast<const WildcardExpression>(expression);
        };

    auto getColumnExpression = [] (const std::shared_ptr<SqlRowExpression> expression)
        -> std::shared_ptr<const SelectColumnExpression>
        {
            return std::dynamic_pointer_cast<const SelectColumnExpression>(expression);
        };

    auto getNamedColumnExpression = [] (const std::shared_ptr<const SqlRowExpression> expression)
        -> std::shared_ptr<const NamedColumnExpression>
        {
            return std::dynamic_pointer_cast<const NamedColumnExpression>(expression);
        };

    auto getReadVariable = [] (const std::shared_ptr<SqlExpression> expression)
        -> std::shared_ptr<const ReadColumnExpression>
        {
            return std::dynamic_pointer_cast<const ReadColumnExpression>(expression);
        };

    auto getWithinExpression = [] (const std::shared_ptr<const SqlExpression> expression)
        -> std::shared_ptr<const SelectWithinExpression>
        {
            return std::dynamic_pointer_cast<const SelectWithinExpression>(expression);
        };

    auto getIsTypeExpression = [] (const std::shared_ptr<SqlExpression> expression)
        -> std::shared_ptr<const IsTypeExpression>
        {
            return std::dynamic_pointer_cast<const IsTypeExpression>(expression);
        };

    auto getComparisonExpression = [] (const std::shared_ptr<SqlExpression> expression)
        -> std::shared_ptr<const ComparisonExpression>
        {
            return std::dynamic_pointer_cast<const ComparisonExpression>(expression);
        };

    auto getBooleanExpression = [] (const std::shared_ptr<SqlExpression> expression)
        -> std::shared_ptr<const BooleanOperatorExpression>
        {
            return std::dynamic_pointer_cast<const BooleanOperatorExpression>(expression);
        };

    auto getConstantExpression = [] (const std::shared_ptr<SqlExpression> expression)
        -> std::shared_ptr<const ConstantExpression>
        {
            return std::dynamic_pointer_cast<const ConstantExpression>(expression);
        };

    auto getSelectExpression = [] (const std::shared_ptr<SqlRowExpression> expression)
        -> std::shared_ptr<const SelectExpression>
        {
            return std::dynamic_pointer_cast<const SelectExpression>(expression);
        };
             
    std::cerr << "type " << typeid(*expr).name() << std::endl;
    // {x, y}
    auto withinExpression = getWithinExpression(expr);
    if (withinExpression) {
        std::cerr << "within " << withinExpression->surface << std::endl;
        auto selectExpression = getSelectExpression(withinExpression->select);

        if (selectExpression) {
            std::cerr << "select " << std::endl;
            for (const auto & clause : selectExpression->clauses) {
                std::cerr << "clause " << clause->surface << std::endl;
                std::cerr << "typeif " << typeid(*clause).name() << std::endl;
                auto readVariable = getReadVariable(clause);
                if (readVariable)
                    continue;

                auto wildcard = getWildcard(clause);
                if (wildcard)
                    continue;

                auto namedColumn = getNamedColumnExpression(clause);
                if (namedColumn) {
                    std::cerr << "named column ... " << std::endl;
                    continue;
                }

                throw ML::Exception(name +
                                    " only accepts wildcard and column names at " +
                                    clause->surface.rawString());
            }
            return;
        }

        auto namedColumn = getNamedColumnExpression(withinExpression->select);
        if (namedColumn) {
            std::cerr << "named column ... " << std::endl;
            return;
        }
                
        auto wildcard = getWildcard(withinExpression->select);
        if (wildcard) {
          std::cerr << "wildcard ... " << std::endl;
          return;
        }

        throw ML::Exception(name +
                            " sdlfhdlsfhsdhjk only accepts wildcard and column names at " +
                            "");//clause->surface.rawString());
    }

    auto wildcard = getWildcard(expr);
    if (wildcard)
        return;

    auto namedColumn = getNamedColumnExpression(expr);
    if (namedColumn) {
        std::cerr << "named column ... " << std::endl;
        return;
    }

    //     // x is not null
    //     auto isTypeExpression = getIsTypeExpression(namedColumn->expression);
    //     if (isTypeExpression) {
    //         std::cerr << "type" << std::endl;
    //         continue;
    //     }
    //     // x = 'true'
    //     auto comparisonExpression = getComparisonExpression(namedColumn->expression);
    //     if (comparisonExpression) {
    //         std::cerr << "comparison" << std::endl;
    //         continue;
    //     }
    //     // NOT x
    //     auto booleanExpression = getBooleanExpression(namedColumn->expression);
    //     if (booleanExpression) {
    //         std::cerr << "boolean" << std::endl;
    //         continue;
    //     }
    //     // 1.0
    //     auto constantExpression = getConstantExpression(namedColumn->expression);
    //     if (constantExpression) {
    //         std::cerr << "constant" << std::endl;
    //         continue;
    //     }
    // }

    throw ML::Exception(name +
                        " only accepts wildcard and column names at " +
                        "" ); //clause->surface.rawString());
}

void
PlainColumnSelect::
operator()(const InputQuery & query, const std::string & name) const
{
    if (!query.stm) {
        throw ML::Exception(name + " must contain a SELECT clause");
    }

    auto & select = query.stm->select;
    for (const auto & clause : select.clauses) {
        validatePlainColumnSelect(clause, name);
    }
}

void
PlainColumnSubSelect::
operator()(const InputQuery & query, const std::string & name) const
{
    if (!query.stm) {
        throw ML::Exception(name + " must contain a SELECT clause");
    }

    auto getNamedColumnExpression = [] (const std::shared_ptr<SqlRowExpression> expression)
        -> std::shared_ptr<const NamedColumnExpression>
        {
            return std::dynamic_pointer_cast<const NamedColumnExpression>(expression);
        };
    
    auto & select = query.stm->select;
    for (const auto & clause : select.clauses) {
        auto namedColumn = getNamedColumnExpression(clause);
        
        if (namedColumn) {
            validatePlainColumnSelect(namedColumn, name);
        }
    }
}

void
FeaturesLabelSelect::
operator()(const InputQuery & query, const std::string & name) const
{
    if (!containsNamedSubSelect(query, "features") ||
        !containsNamedSubSelect(query, "label") )
        throw ML::Exception(name + " expects a row named 'features' and a scalar named 'label'");
}

void
ScoreLabelSelect::
operator()(const InputQuery & query, const std::string & name) const
{
    if (!containsNamedSubSelect(query, "score") ||
        !containsNamedSubSelect(query, "label") )
        throw ML::Exception(name + " expects a scalar named 'score' and a scalar named 'label'");
}

} // namespace MLDB
} // namespace Datacratic

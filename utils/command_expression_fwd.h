/* command_expression.h                                            -*- C++ -*-
   Jeremy Barnes, 27 August 2013
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Template for a command that can be reconstituted with calculated
   arguments.
*/

#pragma once

#include <functional>
#include "mldb/types/value_description_fwd.h"
#include <iostream>

namespace MLDB {

namespace PluginCommand {

struct CommandExpressionVariables;

typedef std::function<Json::Value (const std::vector<Json::Value> &)>
    CommandExpressionFunction;

/** Renders the given JSON expression as a string. */
std::string stringRender(const Json::Value & val);

struct CommandExpressionContext;

struct CommandExpression;

struct ConcatExpression;
struct ArrayExpression;
struct ObjectExpression;
struct LiteralExpression;
struct JsonLiteralExpression;
struct InlineArrayExpression;
struct ParenthesisExpression;
struct MapExpression;
struct FunctionExpression;
struct VariableExpression;
struct ExtractFieldExpression;
struct ExtractElementExpression;
struct ArithmeticExpression;
struct BinaryArithmeticExpression;
struct PlusExpression;
struct MinusExpression;
struct DivideExpression;
struct ModulusExpression;
struct TimesExpression;

PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<CommandExpression>);
PREDECLARE_VALUE_DESCRIPTION(std::shared_ptr<const CommandExpression>);

/** Renders the given JSON expression as a shell command. */
std::vector<std::string> commandRender(const Json::Value & val);


struct StringTemplate;
PREDECLARE_VALUE_DESCRIPTION(StringTemplate);

std::ostream & operator << (std::ostream & stream, const StringTemplate & tmpl);


struct CommandTemplate;

DECLARE_STRUCTURE_DESCRIPTION(CommandTemplate);

} // namespace PluginCommand

using PluginCommand::CommandExpression;
using PluginCommand::CommandExpressionContext;
using PluginCommand::CommandExpressionVariables;

} // namespace MLDB


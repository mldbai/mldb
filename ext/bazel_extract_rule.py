# bazel_extract_rule.py
# Jeremy Barnes, 14 March 2016
# This file is part of MLDB.  Copyright (c) 2016 Datacratic Inc.
#
# Script to parse the Bazel BUILD file in TensorFlow Python, and extract
# the 'name' and 'hidden' arguments of calls to 'tf_gen_op_wrapper_py',
# turning them into a Makefile.  This is possible because the Bazel
# syntax is a strict subset of Python's syntax, so we use Python's
# abstract syntax tree manipulation to create it.

import ast
import sys

buildfile=open(sys.argv[1]).read()

tree = ast.parse(buildfile, sys.argv[1])

for node in ast.iter_child_nodes(tree):
    if isinstance(node, ast.Expr):
        if node.value.func.id == 'tf_gen_op_wrapper_py':
#            print ast.dump(node.value)
            name = ''
            vals = ''
            for field in node.value.keywords:
                if field.arg == 'name':
                    name = ast.literal_eval(field.value)
                elif field.arg == 'hidden':
                    vals = ast.literal_eval(field.value)
            print "TENSORFLOW_PYTHON_OP_{name}_HIDDEN := {valstr}".format(name=name,valstr=",".join(vals))


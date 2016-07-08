#
# generate_procedure.py
# Mich, 2016-07-07
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import argparse
import subprocess
import datetime
import os
from string import capwords

template_cc = """/**                                                                 -*- C++ -*-
 * {filename}.cc
 * {author}, {date}
 *
 * This file is part of MLDB. Copyright {year} Datacratic. All rights reserved.
 **/

#include "{filename}.h"
#include "mldb/server/mldb_server.h"
#include "mldb/server/bound_queries.h"
#include "mldb/types/any_impl.h"

// TODO - Common includes, use the one you need, clean up the others
//#include "mldb/sql/sql_expression.h"
//#include "mldb/server/dataset_context.h"
//#include "mldb/types/basic_value_descriptions.h"
//#include "mldb/base/parallel.h"
//#include "mldb/sql/table_expression_operations.h"
//#include "mldb/sql/execution_pipeline.h"
//#include "mldb/server/per_thread_accumulator.h"
//#include "mldb/types/date.h"
//#include "mldb/sql/sql_expression.h"
//#include "mldb/plugins/sql_config_validator.h"
//#include "mldb/utils/log.h"
//#include <memory>

using namespace std;


namespace Datacratic {{
namespace MLDB {{

{cc_proc_name}ProcedureConfig::
{cc_proc_name}ProcedureConfig()
{{
    outputDataset.withType("sparse.mutable");
}}

DEFINE_STRUCTURE_DESCRIPTION({cc_proc_name}ProcedureConfig);

{cc_proc_name}ProcedureConfigDescription::
{cc_proc_name}ProcedureConfigDescription()
{{
    addField("outputDataset", &{cc_proc_name}ProcedureConfig::outputDataset,
             "Output dataset configuration. This may refer either to an "
             "existing dataset, or a fully specified but non-existing dataset "
             "which will be created by the procedure.",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addParent<ProcedureConfig>();

    onPostValidate = [&] ({cc_proc_name}ProcedureConfig * cfg,
                          JsonParsingContext & context)
    {{
        // MustContainFrom(), GroupBy, etc. go here with additional manual
        // validation
    }};
}}

{cc_proc_name}Procedure::
{cc_proc_name}Procedure(MldbServer * owner,
                 PolyConfig config,
                 const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{{
    procedureConfig = config.params.convert<{cc_proc_name}ProcedureConfig>();
}}

RunOutput
{cc_proc_name}Procedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{{
    auto config = applyRunConfOverProcConf(procedureConfig, run);
    auto output = createDataset(server, config.outputDataset,
                                nullptr, true /*overwrite*/);
    output->commit();
    return output->getStatus();
}}

Any
{cc_proc_name}Procedure::
getStatus() const
{{
    return Any();
}}

static RegisterProcedureType<{cc_proc_name}Procedure, {cc_proc_name}ProcedureConfig>
reg{cc_proc_name}Procedure(
    builtinPackage(),
    "<PROCEDURE DESCRIPTION>", // TODO
    "procedures/{cc_proc_name}Procedure.md.html",
    nullptr /* static route */,
    {{ MldbEntity::INTERNAL_ENTITY }});


}} // namespace MLDB
}} // namespace Datacratic
"""

template_h = """/**                                                                 -*- C++ -*-
 * {filename}.h
 * {author}, {date}
 *
 * This file is part of MLDB. Copyright {year} Datacratic. All rights reserved.
 **/

#pragma once
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/types/value_description.h"

// TODO - Common includes, use the one you need, clean up the others
//#include "mldb/core/function.h"
//#include "mldb/sql/sql_expression.h"

namespace Datacratic {{
namespace MLDB {{

struct {cc_proc_name}ProcedureConfig : ProcedureConfig {{
    static constexpr const char * name = "{proc_name}";

    {cc_proc_name}ProcedureConfig();

    PolyConfigT<Dataset> outputDataset;
}};

DECLARE_STRUCTURE_DESCRIPTION({cc_proc_name}ProcedureConfig);

struct {cc_proc_name}Procedure: public Procedure {{

    {cc_proc_name}Procedure(
        MldbServer * owner,
        PolyConfig config,
        const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(
        const ProcedureRunConfig & run,
        const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    {cc_proc_name}ProcedureConfig procedureConfig;
}};

}} // namespace MLDB
}} // namespace Datacratic
"""

template_doc ="""
# {human_proc_title} Procedure

/**DESCRIPTION GOES HERE**/

## Configuration

![](%%config procedure {proc_name})
"""

def get_proc_from_args():
    parser = argparse.ArgumentParser(description=
        "Create a procedure file skeleton .cc, .h, .html.md and create the "
        "entry in the makefile.")
    parser.add_argument('procedure', help="The procedure to create.")
    args = parser.parse_args()
    return args.procedure


def update_makefile(filename):
    mk_file = 'plugins.mk'
    if os.path.isfile(mk_file):
        f = open(mk_file, 'rt')
    else:
        raise Exception("*.mk not found")

    makefile = f.read()
    f.close()

    needle = 'LIBMLDB_BUILTIN_PLUGIN_SOURCES:= \\\n'
    idx = makefile.find(needle)
    if idx == -1:
        raise Exception("Failed to find {} in makefile".format(needle))
    new_makefile = '{top}\t{new_file} \\\n{bottom}'.format(
        top=makefile[:idx + len(needle)],
        new_file=filename,
        bottom=makefile[idx + len(needle):])
    f = open(mk_file, 'wt')
    f.write(new_makefile)
    f.close()

def do_it():
    proc = get_proc_from_args()
    base_filename = proc.replace('.', '_') + '_procedure'
    author = \
        subprocess.check_output(['git', 'config', '--get', 'user.name'])[:-1]
    now = datetime.datetime.now().isoformat()
    camel_case_proc_name = capwords(proc, '.').replace('.', '')

    update_makefile(base_filename + '.cc')

    f = open(base_filename + '.cc', 'wt')
    f.write(template_cc.format(filename=base_filename,
                               author=author,
                               date=now[:10],
                               year=now[:4],
                               cc_proc_name=camel_case_proc_name))
    f.close()

    f = open(base_filename + '.h', 'wt')
    f.write(template_h.format(filename=base_filename,
                              author=author,
                              date=now[:10],
                              year=now[:4],
                              cc_proc_name=camel_case_proc_name,
                              proc_name=proc))
    f.close()

    f = open('../container_files/public_html/doc/builtin/procedures/'
             + camel_case_proc_name + 'Procedure.md', 'wt')
    f.write(template_doc.format(
        human_proc_title=capwords(proc, '.').replace('.', ' '),
        proc_name=proc))
    f.close()

if __name__ == '__main__':
    do_it()

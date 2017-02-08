// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** permutation_procedure.cc
    Francois Maillet, 16 septembre 2015
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    Script procedure
*/

#include "permuter_procedure.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/rest/in_process_rest_connection.h"
#include "mldb/types/any_impl.h"
#include "jml/utils/string_functions.h"
#include "mldb/http/http_exception.h"
#include "mldb/utils/log.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* PERMUTATION PROCEDURE CONFIG                                              */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(PermutationProcedureConfig);

PermutationProcedureConfigDescription::
PermutationProcedureConfigDescription()
{
    addField("procedure", &PermutationProcedureConfig::procedure,
            "Base configuration of the procedure to run the permutations over.");
    addField("permutations", &PermutationProcedureConfig::permutations,
            "Object with a key structure that follows the procedure's configuration. "
            "Values can either by objects or lists. Lists need to contain the "
             "elements to permute.");
    addParent<ProcedureConfig>();
}



/*****************************************************************************/
/* PERMUTATION PROCEDURE                                                     */
/*****************************************************************************/

PermutationProcedure::
PermutationProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procConf = config.params.convert<PermutationProcedureConfig>();
}

Any
PermutationProcedure::
getStatus() const
{
    return Any();
}

void PermutationProcedure::
forEachPermutation(
        Json::Value baseConfig,
        const Json::Value & permutations,
        ForEachPermutationFunc doForPermutation)
{
    // flatten json into path -> value pairs
    vector<pair<vector<string>, Json::Value>> lists;

    typedef std::function<void (const Json::Value&, const vector<string>&)> FlattenFunc;
    FlattenFunc flatten = [&] (const Json::Value & js, const vector<string> & path)
    {
        for (const auto & key : js.getMemberNames()) {
            vector<string> newPath(path);
            newPath.push_back(key);

            if(js[key].isArray() || js[key].isString()) {
                lists.push_back(make_pair(newPath, js[key]));
            }
            else if(js[key].isObject()) {
                flatten(js[key], newPath);
            }
            else {
               throw MLDB::Exception("unsupported type!");
            }
        }
    };
    flatten(permutations, vector<string>{});



    // recursivelly modify each key-val pair and call our callback with
    // the modified configuration
    typedef std::function<void (size_t)> RecurJsModifFunc;
    RecurJsModifFunc modifyJsRecur = [&] (size_t list_idx)
    {
        bool lastList = list_idx == lists.size() - 1;

        if(lists[list_idx].second.isArray()) {
            // navigate the config json to get to the correct key-val
            // using the path that we flattened before
            Json::Value * jsLocation = &baseConfig;

            auto & path = lists[list_idx].first;
            for(int i=0; i<path.size()-1; i++) {
                auto & key = path[i];
                if(!jsLocation->isMember(key)) {
                    (*jsLocation)[key] = Json::Value();
                }

                jsLocation = &(*jsLocation)[key];
            }

            // we have a pointer to the right element
            // iterate through all values
            auto & vals = lists[list_idx].second;
            for(int i=0; i<vals.size(); i++) {
                // update val
                (*jsLocation)[path.back()] = vals[i];

                // engage warp engines!!
                if(lastList) {
                    doForPermutation(baseConfig);
                }
                // go deeper
                else {
                    modifyJsRecur(list_idx + 1);
                }
            }
        }
        else {
            throw MLDB::Exception("Invalid data type for permutation");
        }
    };

    modifyJsRecur(0);
}

RunOutput
PermutationProcedure::
run(const ProcedureRunConfig & run,
    const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(procConf, run);


    Json::Value permutations = jsonEncode(runProcConf.permutations);
    Json::Value baseConfig = jsonEncode(runProcConf.procedure.params);

    Json::Value rtn_results(Json::ValueType::arrayValue);
    int permutation_num = 0;
    std::shared_ptr<Procedure> procedure;
    auto doForPermutation = [&] (const Json::Value & permutedConfTmp)
    {
        permutation_num++;


        // TODO the next couple lines are horrible. we're going from json config
        // to string to do a replace_all to catch all the $permutation to replace
        // and then parsing that back to json...
        string strConf = permutedConfTmp.toString();
        ML::replace_all(strConf, "$permutation", MLDB::format("permutation_%d", permutation_num));

        Json::Value permutedConf;
        Json::Reader reader;
        if(!reader.parse(strConf, permutedConf))
            throw MLDB::Exception("unable to reparse!");



        // create the procedure
        if(permutation_num == 1) {
            PolyConfig procPC = runProcConf.procedure;
            procPC.params = permutedConf;

            INFO_MSG(logger) << " >>>>> Creating procedure";
            procedure = obtainProcedure(server, procPC, onProgress);
        }

        // create run configuration
        ProcedureRunConfig procRunConf;
        procRunConf.id = "run_"+to_string(permutation_num);
        procRunConf.params = permutedConf;
        RunOutput output = procedure->run(procRunConf, onProgress);

        // Add results
        Json::Value rez;
        rez["configuration"] = permutedConf;
        rez["results"] = jsonEncode(output.results);
        rtn_results.append(rez);

    };


    PermutationProcedure::forEachPermutation(baseConfig, permutations, doForPermutation);

    return RunOutput(rtn_results);
}


namespace {

RegisterProcedureType<PermutationProcedure, PermutationProcedureConfig>
regScript(builtinPackage(),
          "Run a child procedure with permutations of its configuration",
          "procedures/PermuterProcedure.md.html");

} // file scope

} // namespace MLDB


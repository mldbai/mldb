// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* json_format.cc
   Jeremy Barnes, 31 January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.

   Format JSON.
*/

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/value_description.h"
#include "command_expression.h"

using namespace std;
using namespace MLDB;
using namespace MLDB::PluginCommand;

int main(int argc, char ** argv)
{
    using namespace boost::program_options;

    options_description configuration_options("Configuration options");

    std::string expression;
    std::vector<std::string> inputFiles;
    std::string outputFile;
    bool outputJson = false;

    configuration_options.add_options()
        ("expresion,e", value(&expression),
         "Expression to parse with")
        ("input-file,i", value(&inputFiles),
         "File to load (can be multiple)")
        ("output-file,o", value(&outputFile),
         "File to write")
        ("output-json", value(&outputJson)->default_value(outputJson),
         "Output JSON instead of a string");
    
    options_description all_opt;
    all_opt
        .add(configuration_options);
    all_opt.add_options()
        ("help,h", "print this message");
   
    positional_options_description pos;
    pos.add("expression", -1);
    pos.add("input-file", -1);
    variables_map vm;
    bool showHelp = false;

    try{
        parsed_options parsed = command_line_parser(argc, argv)
            .options(all_opt)
            .positional(pos)
            .run();
        store(parsed, vm);
        notify(vm);
    }catch(const std::exception & exc){
        //invalid command line param
        cerr << "command line parsing error: " << exc.what() << endl;
        showHelp = true;
    }

    //If one of the options is set to 'help'...
    if (showHelp || vm.count("help")){
        //Display the options_description
        cout << all_opt << "\n";
        return showHelp ? 0 : 1;
    }

    if (inputFiles.empty()) {
        inputFiles.push_back("-");
    }

    if (outputFile.empty()) {
        outputFile = '-';
    }

    StringTemplate tmpl(expression);
    CommandExpressionContext context;

    filter_ostream out(outputFile);

    for (auto & f: inputFiles) {
        //cerr << "doing file " << f << endl;
        filter_istream stream(f);
        //cerr << "stream = " << (bool)stream << endl;
        ParseContext pcontext(f, stream);
        //cerr << "stream = " << (bool)stream << endl;

        while (pcontext) {
            //cerr << "stream = " << (bool)stream << endl;
            skipJsonWhitespace(pcontext);
            if (!pcontext)
                break;

            Json::Value val = expectJson(pcontext);

            CommandExpressionContext c(context);
            c.setValue("_", val);
        
            Json::Value output = tmpl.expr->apply(c);

            if (outputJson)
                out << output.toString();
            else out << stringRender(output) << endl;
        }

        //cerr << "stream = " << (bool)stream << endl;
    }
}

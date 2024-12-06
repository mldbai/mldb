#!/usr/bin/env python3
from openai import OpenAI
import os
import sys
import re
import argparse



MODEL="gpt-4o"
client = OpenAI(api_key=os.environ.get("OPENAI_SECRET_KEY", "<your OpenAI API key if not set as an env var>"))

SYSTEM_PROMPT = """
You are an expert in building software using Makefiles and CMake.
You know about the mldb machine learning database, its codebase and its build system.
"""

USER_PROMPT = """
I will give you a Makefile snippet from mldb. This is part of a build system that uses Makefiles.
You are to convert it to CMake, and output the equivalent CMake code in a Markdown code block.

The Cmake code is not stand-alone. It is meant to be part of a larger CMakeLists.txt file.

Please ensure that the original Makefile comments are added in.
The first output line must be a copyright, like the following:
# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.
Only add aditional comments if they are very necessary to understand the code. Otherwise, leave them out.
If and only if the translation is highly complex, then include
the original Makefile snippet in a comment in the output delimited by `#input: [input_line]`.

You may encounter the following macros in the makefile:

- include_sub_make: include a sub-makefile in a subdirectory. Should be converted to a single-argument `add_subdirectory` command in CMake.
  - $(1): name of the submakefile. Used as a namespace for the Makefile variables. It can be ignored unless it's the only argument.
  - $(2): directory containing the submakefile. This is optional and defaults to $(1) if not given
  - $(3): the name of the submakefile in the directory. This is optional and defaults to $(2)/$(1).mk if not given
- test: add a test file to the test suite. Should be converted to a `add_test` command in CMake, or to use the add_mldb_test function.
  - $(1): test name. This is also the filename used to build the test. If there is no extension, then .cc is assumed.
  - $(2): libraries. These libraries should be linked in to the executable.
  - $(3): test options (there may be multiple). These can include:
    - `valgrind`: run the test within valgrind
    - `manual`: don't run the test automatically
    - `boost`: run the test using the boost test framework, including linking the boost_unit_test_framework library
    - `timed`: use the system "time" command to time the test
- program: add a program to the build. Should be converted to a `add_executable` command in CMake.
  - $(1): program name. This is also the filename used to build the program. If there is no extension, then .cc is assumed.
  - $(2): libraries. These libraries should be linked in to the executable.
  
You may encounter the following Makefile variables:
- CWD is the current working directory, equivalent to CMAKE_CURRENT_SOURCE_DIR in CMake.
- OS is the operating system, which will be Linux or Darwin.
- ARCH is the architecture, which will be x86_64 or arm64 (on Linux) or aarch64 (on Darwin/OSX).
- STD_FILESYSTEM_LIBNAME is the name of the standard filesystem library.

Example lines and their translations:
$(eval $(call include_sub_make,base_testing,testing)) -->
  add_subdirectory(testing)

$(eval $(call test,sse2_math_test,arch,boost manual)) -->
  add_mldb_test(sse2_math_test "arch" "boost;manual")

There are a set of utility functions that can be used to help with the conversion. These are:

```cmake
{utility_functions}
```

Here is the input Makefile:

```makefile
{input_makefile}
```

Here is the equivalent CMake code:
"""

#For each section, first echo the input line in a comment in the format `#input: [input_line]`. Ensure that #input: occurs on
#every line of a multi-line echoed input, except where the echoed input is a comment. Never ever output '#input: #' at the beginning of a line.


def main():
    parser = argparse.ArgumentParser(description="Convert Makefile snippets to CMake.")
    parser.add_argument('--input-dir', '-i', required=True, help="Input directory we're scanning")
    parser.add_argument('--utility-dir', '-u', default="jml-build", help="Directory that contains utility functions for cmake")
    parser.add_argument('--overwrite', '-o', action='store_true', help="Overwrite existing CMake files")
    parser.add_argument('--dry-run', '-d', action='store_true', help="Perform a dry run without making any changes")
    parser.add_argument('--verbose', '-v', action='store_true', help="Print verbose output")

    args = parser.parse_args()
    
    input_dir = args.input_dir
    utility_dir = args.utility_dir
    overwrite = args.overwrite
    verbose = args.verbose

    if args.dry_run:
        print("Dry run mode enabled. No changes will be made.")

    # Scan the input directory, and find any .mk files
    all_dirs = {}
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".mk"):
                fullpath = os.path.join(root, file)
                path = os.path.dirname(fullpath)
                print("file ", file, " path", path)
                all_dirs.setdefault(path, list()).append(file)

    if not all_dirs:
        print("Error: No .mk files found in the input directory.")
        sys.exit(1)

    print("all_dirs ", all_dirs)

    utility_functions = ""
    if utility_dir is not None:
        utility_files = [f for f in os.listdir(utility_dir) if f.endswith(".cmake")]
        for utility_file in utility_files:
            with open(os.path.join(utility_dir, utility_file), 'r') as f:
                utility_functions += f.read() + "\n\n"

    for dir, mk_files in all_dirs.items():
        for mk_file in mk_files:
            mk_path = os.path.join(dir, mk_file)
            print("processing ", mk_path)
            with open(mk_path, 'r') as f:
                input_makefile = f.read()
            
            messages = [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": USER_PROMPT.format(input_makefile=input_makefile, utility_functions=utility_functions),
                }
            ]

            if verbose:
                print(f"prompt\n{messages}",file=sys.stderr)
                
            completion = client.chat.completions.create(
                model=MODEL,
                messages=messages,
            )
            
            if verbose:
                sys.stderr.write("completion\n{completion}")
                print(f"completion\n{completion}",file=sys.stderr)


            if len(mk_files) == 1:
                cmake_file = os.path.join(dir, "CMakeLists.txt")
            else:
                cmake_file = os.path.join(dir, mk_file.replace(".mk", ".cmake"))

            cmake_output = completion.choices[0].message.content
            #print("output of llm")
            #print(cmake_output)
            #print("end output of llm")
            #print()

            # Extract the markdown block from the completion
            if not overwrite and os.path.exists(cmake_file):
                print(f"Error: {cmake_file} already exists. Use --overwrite to overwrite it.")
                continue
            markdown_block = re.search(r'```cmake(.*?)```', cmake_output, re.DOTALL)
            if markdown_block:
                cmake_output = markdown_block.group(1).strip()
            else:
                print("Error: No markdown block found in the completion.")
                sys.exit(1)
            


            with open(cmake_file, 'w') as f:
                f.write(cmake_output)
                f.write("\n")
            #print(cmake_output)
            print(f"Output written to {cmake_file}")

if __name__ == "__main__":
    main()

if False:
    # set input_makefile to the contents of the first command line argument
    input_makefile = open(sys.argv[1]).read()
    utility_functions = open(sys.argv[2]).read()

    completion = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": USER_PROMPT.format(input_makefile=input_makefile, utility_functions=utility_functions),
            }
        ]
    )

    print(completion.choices[0].message.content)

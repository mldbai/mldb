#!/usr/bin/env python3
from openai import OpenAI
import os
import sys

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
Only add aditional comments if they are very necessary to understand the code. Otherwise, leave them out.
If and only if the translation is not mechanical and there is significant extra complexity, then include
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
"""

#For each section, first echo the input line in a comment in the format `#input: [input_line]`. Ensure that #input: occurs on
#every line of a multi-line echoed input, except where the echoed input is a comment. Never ever output '#input: #' at the beginning of a line.


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

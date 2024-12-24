# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

# Source files for the Hoedown library
set(HOEDOWN_SRC
    autolink.c
    buffer.c
    document.c
    escape.c
    html.c
    html_blocks.c
    html_smartypants.c
    stack.c
    version.c
)

list(TRANSFORM HOEDOWN_SRC PREPEND "hoedown/src/")

#set_source_files_properties(${HOEDOWN_SRC} PROPERTIES COMPILE_FLAGS "${HOEDOWN_WARNING_FLAGS}")

# Define the hoedown library
add_library(hoedown ${HOEDOWN_SRC})

## Perfect hashing
add_custom_command(
  OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/src/html_blocks.c
  COMMAND gperf
          -L ANSI-C
          -N hoedown_find_block_tag
          -c -C -E -S 1 --ignore-case -m100
          mldb/ext/hoedown/html_block_names.gperf > ${CMAKE_CURRENT_SOURCE_DIR}/src/html_blocks.c~
  COMMAND mv ${CMAKE_CURRENT_SOURCE_DIR}/src/html_blocks.c~ ${CMAKE_CURRENT_SOURCE_DIR}/src/html_blocks.c
  DEPENDS mldb/ext/hoedown/html_block_names.gperf
  COMMENT "Generating src/html_blocks.c from html_block_names.gperf"
)

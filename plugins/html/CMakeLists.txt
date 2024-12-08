# This file is part of MLDB. Copyright 2024 Jeremy Barnes. All rights reserved.

add_subdirectory(ext)

# Html plugins
set(LIBMLDB_HTML_PLUGIN_SOURCES
    html_plugin.cc
    parse_html.cc
    css.cc
    useragent_function.cc
)

# Setting compile options for HTML plugin sources
set_source_files_properties(${LIBMLDB_HTML_PLUGIN_SOURCES}
                            PROPERTIES COMPILE_OPTIONS "-Imldb/plugins/html/ext")

# Define the MLDB HTML plugin library
add_library(mldb_html_plugin
    ${LIBMLDB_HTML_PLUGIN_SOURCES}
)

# Link libraries to the MLDB HTML plugin
target_link_libraries(mldb_html_plugin hubbub tinyxpath uap)

# Mark this library as a builtin plugin
set_property(TARGET mldb_html_plugin PROPERTY MLDB_BUILTIN_PLUGIN "html")

add_subdirectory(testing)

# makefile for google URL module

# We depend on ICU for unicode support
find_package(ICU REQUIRED COMPONENTS data i18n uc)


# Source files for the google_url module
set(LIBGOOGLE_URL_SOURCES
    src/gurl.cc
    src/url_canon_etc.cc
    src/url_parse_file.cc
    src/url_canon_mailtourl.cc
    src/url_canon_icu.cc
    src/url_canon_fileurl.cc
    src/url_canon_pathurl.cc
    src/url_parse.cc
    src/url_canon_host.cc
    src/url_canon_relative.cc
    src/url_canon_ip.cc
    src/url_util.cc
    src/url_canon_filesystemurl.cc
    src/url_canon_internal.cc
    src/url_canon_stdurl.cc
    src/url_canon_path.cc
    src/url_canon_query.cc
    base/string16.cc
)

# Set compile options for the google_url sources
set_source_files_properties(${LIBGOOGLE_URL_SOURCES} PROPERTIES
    INCLUDE_DIRECTORIES
        "${ICU_INCLUDE_DIRS};${MLDB_EXT_DIR};${CMAKE_CURRENT_SOURCE_DIR}"
    COMPILE_FLAGS
        "-Duint64=uint64_t -Wno-deprecated-declarations"
)

#set_source_files_properties(${LIBGOOGLE_URL_SOURCES} PROPERTIES INCLUDE_DIRECTORIES ${ICU_INCLUDE_DIRS} ${MLDB_EXT_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..)

#set_include_paths(src/gurl.cc /usr/bonusman/bonus;/usr/bonusman2/bonus2 ${MLDB_EXT_DIR} bonusman ${ICU_INCLUDE_DIRS} ${MLDB_EXT_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..)
#set_include_paths("${LIBGOOGLE_URL_SOURCES}" "bonusman;bonusman2;${ICU_INCLUDE_DIRS};${MLDB_EXT_DIR}")
#set_compile_options("${LIBGOOGLE_URL_SOURCES}" "-Duint64=uint64_t")
#set_compile_options("${LIBGOOGLE_URL_SOURCES}" "-Duint64=uint64_t -Wno-array-bounds -Imldb/ext -Imldb/ext/googleurl -I${ICU_INCLUDE_PATH}")

# Libraries for linking
set(LIBGOOGLE_URL_LINK
        ICU::i18n
        ICU::data
        ICU::uc
        m)

# Create a library from the google_url sources
add_library(googleurl ${LIBGOOGLE_URL_SOURCES})
target_link_libraries(googleurl PRIVATE ${LIBGOOGLE_URL_LINK})

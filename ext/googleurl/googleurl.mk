# makefile for google URL module

# Make rules to build the google_url files

LIBGOOGLE_URL_SOURCES := \
	src/gurl.cc \
	src/url_canon_etc.cc \
	src/url_parse_file.cc \
	src/url_canon_mailtourl.cc \
	src/url_canon_icu.cc \
	src/url_canon_fileurl.cc \
	src/url_canon_pathurl.cc \
	src/url_parse.cc \
	src/url_canon_host.cc \
	src/url_canon_relative.cc \
	src/url_canon_ip.cc \
	src/url_util.cc \
	src/url_canon_filesystemurl.cc \
	src/url_canon_internal.cc \
	src/url_canon_stdurl.cc \
	src/url_canon_path.cc \
	src/url_canon_query.cc


#	src/gurl_test_main.cc \
#	src/gurl_unittest.cc \
#	src/url_util_unittest.cc \
#	src/url_parse_unittest.cc \
#	src/url_canon_unittest.cc \

ICU_INCLUDE_PATH?=/usr/include

$(eval $(call set_compile_option,$(LIBGOOGLE_URL_SOURCES),-Duint64=uint64_t -Wno-array-bounds -Imldb/ext -I$(ICU_INCLUDE_PATH)))

LIBGOOGLE_URL_LINK := icui18n icuuc icudata m

$(eval $(call library,googleurl,$(LIBGOOGLE_URL_SOURCES),$(LIBGOOGLE_URL_LINK)))

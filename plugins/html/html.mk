# Makefile for html plugin for MLDB

$(eval $(call include_sub_make,html_ext,ext,html_ext.mk))


# Html plugins
LIBMLDB_HTML_PLUGIN_SOURCES:= \
	html_plugin.cc \
	parse_html.cc \
	css.cc \
	useragent_function.cc

$(eval $(call set_compile_option,$(LIBMLDB_HTML_PLUGIN_SOURCES),-Imldb/plugins/html/ext))

$(eval $(call mldb_plugin_library,html,mldb_html_plugin,$(LIBMLDB_HTML_PLUGIN_SOURCES),hubbub tinyxpath uap))

$(eval $(call mldb_builtin_plugin,html,mldb_html_plugin,doc))

$(eval $(call include_sub_make,html_testing,testing,html_testing.mk))

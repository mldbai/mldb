# Makefile for textual plugin for MLDB

#$(eval $(call include_sub_make,textual_ext,ext,textual_ext.mk))


# Textual plugins
LIBMLDB_TEXTUAL_PLUGIN_SOURCES:= \
	textual_plugin.cc \
	csv_export_procedure.cc \
	csv_writer.cc \
	json_importer.cc \
	importtext_procedure.cc \
	sql_csv_scope.cc \
	tokensplit.cc \

LIBMLDB_TEXTUAL_PLUGIN_LINK:= \
	block \

$(eval $(call library,mldb_textual_plugin,$(LIBMLDB_TEXTUAL_PLUGIN_SOURCES),$(LIBMLDB_TEXTUAL_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_TEXTUAL_PLUGIN_SOURCES),-Imldb/textual/ext))

#$(eval $(call mldb_plugin_library,textual,mldb_textual_plugin,$(LIBMLDB_TEXTUAL_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,textual,mldb_textual_plugin,doc))

#$(eval $(call include_sub_make,textual_testing,testing,textual_testing.mk))

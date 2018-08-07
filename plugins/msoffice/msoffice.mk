# Makefile for msoffice plugin for MLDB

#$(eval $(call include_sub_make,msoffice_ext,ext,msoffice_ext.mk))


# Msoffice plugins
LIBMLDB_MSOFFICE_PLUGIN_SOURCES:= \
	msoffice_plugin.cc \
	xlsx_importer.cc \


LIBMLDB_MSOFFICE_PLUGIN_LINK:= \

$(eval $(call library,mldb_msoffice_plugin,$(LIBMLDB_MSOFFICE_PLUGIN_SOURCES),$(LIBMLDB_MSOFFICE_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_MSOFFICE_PLUGIN_SOURCES),-Imldb/msoffice/ext))

#$(eval $(call mldb_plugin_library,msoffice,mldb_msoffice_plugin,$(LIBMLDB_MSOFFICE_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,msoffice,mldb_msoffice_plugin,doc))

#$(eval $(call include_sub_make,msoffice_testing,testing,msoffice_testing.mk))

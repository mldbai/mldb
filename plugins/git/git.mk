# Makefile for git plugin for MLDB

#$(eval $(call include_sub_make,git_ext,ext,git_ext.mk))


# Git plugins
LIBMLDB_GIT_PLUGIN_SOURCES:= \
	git_plugin.cc \
	git.cc \


LIBMLDB_GIT_PLUGIN_LINK:= \
	mldb_core \
	mldb_engine \
	arch \
	types \
	utils \
	sql_expression \
	value_description \
	base \
	progress \
	rest \
	db \
	vfs \
	log \
	link \
	rest \
	any \
	watch \
	rest_entity \
	mldb_builtin_base \
	mldb_builtin \
	sql_types \
	git2 \

$(eval $(call library,mldb_git_plugin,$(LIBMLDB_GIT_PLUGIN_SOURCES),$(LIBMLDB_GIT_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_GIT_PLUGIN_SOURCES),-Imldb/git/ext))

#$(eval $(call mldb_plugin_library,git,mldb_git_plugin,$(LIBMLDB_GIT_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,git,mldb_git_plugin,doc))

#$(eval $(call include_sub_make,git_testing,testing,git_testing.mk))

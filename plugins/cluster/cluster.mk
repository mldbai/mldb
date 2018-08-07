# Makefile for cluster plugin for MLDB

#$(eval $(call include_sub_make,cluster_ext,ext,cluster_ext.mk))


# Cluster plugins
LIBMLDB_CLUSTER_PLUGIN_SOURCES:= \
	cluster_plugin.cc \
	kmeans.cc \
	em.cc \
	tsne.cc \


LIBMLDB_CLUSTER_PLUGIN_LINK:= \
	ml

$(eval $(call library,mldb_cluster_plugin,$(LIBMLDB_CLUSTER_PLUGIN_SOURCES),$(LIBMLDB_CLUSTER_PLUGIN_LINK)))

#$(eval $(call set_compile_option,$(LIBMLDB_CLUSTER_PLUGIN_SOURCES),-Imldb/cluster/ext))

#$(eval $(call mldb_plugin_library,cluster,mldb_cluster_plugin,$(LIBMLDB_CLUSTER_PLUGIN_SOURCES),hubbub tinyxpath))

#$(eval $(call mldb_builtin_plugin,cluster,mldb_cluster_plugin,doc))

#$(eval $(call include_sub_make,cluster_testing,testing,cluster_testing.mk))

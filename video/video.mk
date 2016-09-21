# Video plugin for MLDB
# Jeremy Barnes, 21 September 2016

# External repos required
$(eval $(call include_sub_make,video_ext,ext,video_ext.mk))

# Video plugin
LIBMLDB_VIDEO_PLUGIN_SOURCES:= \
	video_plugin.cc

$(eval $(call mldb_plugin_library,video,mldb_video_plugin,$(LIBMLDB_VIDEO_PLUGIN_SOURCES),avdevice))

$(eval $(call mldb_builtin_plugin,video,mldb_video_plugin,doc))
$(eval $(call include_sub_make,testing))

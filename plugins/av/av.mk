# Av plugin for MLDB
# Jeremy Barnes, 21 September 2016

# External repos required
$(eval $(call include_sub_make,av_ext,ext,av_ext.mk))

# Av plugin
LIBMLDB_AV_PLUGIN_SOURCES:= \
	av_plugin.cc \
	audio_sensor.cc \
	video_sensor.cc

$(eval $(call mldb_plugin_library,av,mldb_av_plugin,$(LIBMLDB_AV_PLUGIN_SOURCES),avdevice))

$(eval $(call mldb_builtin_plugin,av,mldb_av_plugin,doc))
$(eval $(call include_sub_make,testing))

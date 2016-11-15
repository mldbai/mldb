# Av plugin for MLDB
# Jeremy Barnes, 21 September 2016

LIBI2C_SOURCES:= \
	i2c.cc \
	I2Cdev.cc \

$(eval $(call mldb_plugin_library,sensors,i2c,$(LIBI2C_SOURCES)))


# External repos required
$(eval $(call include_sub_make,sensors_ext,ext,sensors_ext.mk))

# Sensors plugin
LIBMLDB_SENSORS_PLUGIN_SOURCES:= \
	sensors_plugin.cc \
	audio_sensor.cc \
	video_sensor.cc \
	memory_sensor.cc

$(eval $(call mldb_plugin_library,sensors,mldb_sensors_plugin,$(LIBMLDB_SENSORS_PLUGIN_SOURCES),avdevice i2c))

$(eval $(call mldb_builtin_plugin,sensors,mldb_sensors_plugin,doc))
$(eval $(call include_sub_make,testing))

I2CDEVLIB_SOURCE_FILES:=$(filter-out %/I2Cdev.cpp %/_Stub.cpp %/AD7746.cpp %/IAQ2000.cpp %/MS5803.cpp,$(wildcard $(CWD)/i2cdevlib/Arduino/*/*.cpp))
I2CDEVLIB_SOURCE_BUILD:=$(I2CDEVLIB_SOURCE_FILES:$(CWD)/%=%)

$(eval $(call set_compile_option,$(I2CDEVLIB_SOURCE_BUILD),-Imldb/sensors))
$(eval $(call mldb_plugin_library,sensors,i2cdevlib,$(I2CDEVLIB_SOURCE_BUILD),i2c))


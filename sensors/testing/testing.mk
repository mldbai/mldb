# Testing for the MLDB video capture plugin

$(eval $(call mldb_unit_test,MLDB-1956-audio-capture.js,sensors))
$(eval $(call mldb_unit_test,MLDB-1956-sensor-logging.js,sensors))
$(eval $(call test,video_device_test,avcodec avdevice avutil,plain))
$(eval $(call test,i2c_test,i2c types,boost))

$(eval $(call set_compile_option,mpu6050_test.cc ads1115_test.cc hmc5883l_test.cc,-Imldb/sensors))
$(eval $(call test,mpu6050_test,i2c types i2cdevlib,boost manual))
$(eval $(call test,ads1115_test,i2c types i2cdevlib,boost manual))
$(eval $(call test,hmc5883l_test,i2c types i2cdevlib,boost manual))

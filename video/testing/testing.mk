# Testing for the MLDB video capture plugin

$(eval $(call test,video_device_test,avcodec avdevice avutil,plain))

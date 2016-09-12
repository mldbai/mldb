MANUAL_IF_NO_MONGO:= $(if $(shell which mongod),,manual)

$(eval $(call mldb_unit_test,mongo_plugin_test.py,mongodb mongo_temp_server_wrapping))
$(eval $(call test,mongo_temporary_server_test,services boost_filesystem mongo_tmp_server,boost mongo_temp_server_wrapping,$(MANUAL_IF_NO_MONGO)))

$(eval $(call python_test,mongo_temp_server_wrapping_test,mongo_temp_server_wrapping,$(MANUAL_IF_NO_MONGO)))

mongo_plugin_test.py: \
    $(LIB)/libmongo_tmp_server.so \
    $(BIN)/python_mongo_temp_server_wrapping.so \
    $(PLUGINS)/mongodb/lib/libmldb_mongodb_plugin.so \

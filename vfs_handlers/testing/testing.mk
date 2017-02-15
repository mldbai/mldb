#$(eval $(call test,hdfs_test,vfs_handlers,boost manual))
$(eval $(call test,fs_utils_test,vfs_handlers test_utils,boost manual))
$(eval $(call test,azure_blob_storage_test,vfs vfs_handlers boost_filesystem boost_system,boost))

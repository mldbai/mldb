# VFS Handlers

$(eval $(call include_sub_make,aws))

LIBVFS_HANDLERS_SOURCES := \
	exception_ptr.cc \
	sftp.cc \
	azure_blob_storage.cc \
	archive.cc \
	docker.cc

#	hdfs.cc

LIBVFS_HANDLERS_LINK := hash $(LIBARCHIVE_LIB_NAME) ssh2 azure_storage_cpp aws_vfs_handlers

AZURE_BLOB_STORAGE_OPTIONS := \
    -Imldb/ext/casablanca/Release/include \
    -Imldb/ext/azure-storage-cpp/Microsoft.WindowsAzure.Storage/includes \
    -Wno-overloaded-virtual \
    -Wno-reorder \
    -Wno-unused-value \
    -Wno-unknown-pragmas

$(eval $(call set_compile_option,azure_blob_storage.cc,$(AZURE_BLOB_STORAGE_OPTIONS)))

$(eval $(call library,vfs_handlers,$(LIBVFS_HANDLERS_SOURCES),$(LIBVFS_HANDLERS_LINK)))


$(eval $(call include_sub_make,testing))

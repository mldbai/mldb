# VFS Handlers

LIBVFS_HANDLERS_SOURCES := \
	exception_ptr.cc \
	sftp.cc \
	s3_handlers.cc \
	azure_blob_storage.cc \
	archive.cc \
	docker.cc

#	hdfs.cc

LIBVFS_HANDLERS_LINK := aws hash archive tinyxml2 ssh2  #hdfs3


$(eval $(call library,vfs_handlers,$(LIBVFS_HANDLERS_SOURCES),$(LIBVFS_HANDLERS_LINK)))

# gcc 4.7
$(eval $(call set_compile_option,aws.cc,-fpermissive))

$(eval $(call program,s3tee,vfs_handlers boost_program_options utils))
$(eval $(call program,s3cp,vfs_handlers boost_program_options utils))
$(eval $(call program,s3_multipart_cmd,vfs_handlers boost_program_options utils))
$(eval $(call program,s3cat,vfs_handlers boost_program_options utils))

$(eval $(call include_sub_make,testing))

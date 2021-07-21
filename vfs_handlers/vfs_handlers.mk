# VFS Handlers

$(eval $(call include_sub_make,aws))

LIBVFS_HANDLERS_SOURCES := \
	sftp.cc \
	archive.cc \
	docker.cc \

LIBVFS_HANDLERS_LINK := \
	hash \
	$(LIBARCHIVE_LIB_NAME) \
	ssh2 \
	aws_vfs_handlers \
	arch \
	value_description \
	vfs \
	http \
	types \
	credentials \
	utils \

$(eval $(call library,vfs_handlers,$(LIBVFS_HANDLERS_SOURCES),$(LIBVFS_HANDLERS_LINK)))


$(eval $(call include_sub_make,testing))

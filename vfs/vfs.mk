# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBVFS_SOURCES := \
	fs_utils.cc \
        filter_streams.cc \
	http_streambuf.cc \
	compressor.cc \
	zstandard.cc \
	snappy.cc \
	exception_ptr.cc \
	libdb_initialization.cc \

LIBVFS_LINK := \
	arch \
	boost_iostreams \
	lzmapp \
	types \
	$(STD_FILESYSTEM_LIBNAME) \
	http \
	lz4 \
	xxhash \
	zstd \
	snappy \
	db

$(eval $(call library,vfs,$(LIBVFS_SOURCES),$(LIBVFS_LINK)))

$(eval $(call include_sub_make,vfs_testing,testing))


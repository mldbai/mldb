# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

LIBVFS_SOURCES := \
	fs_utils.cc \
        filter_streams.cc \
	http_streambuf.cc \
	compressor.cc \
	zstandard.cc

LIBVFS_LINK := arch boost_iostreams lzmapp types boost_filesystem http lz4 xxhash zstd

$(eval $(call library,vfs,$(LIBVFS_SOURCES),$(LIBVFS_LINK)))

$(eval $(call include_sub_make,vfs_testing,testing))


# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,filter_streams_test,vfs $(STD_FILESYSTEM_LIBNAME) boost_system value_description arch types base,boost))

$(TESTS)/filter_streams_test:	$(BIN)/lz4cli $(BIN)/zstd

$(eval $(call test,compressor_test,vfs $(STD_FILESYSTEM_LIBNAME) boost_system value_description arch types base,boost))

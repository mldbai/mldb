# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,filter_streams_test,vfs boost_filesystem boost_system,boost))

$(TESTS)/filter_streams_test:	$(BIN)/lz4cli $(BIN)/zstd

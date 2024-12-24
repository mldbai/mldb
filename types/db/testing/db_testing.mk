# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,compact_size_type_test,arch base db,boost))
$(eval $(call test,serialize_reconstitute_test,arch base db,boost))

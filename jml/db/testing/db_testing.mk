# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

$(eval $(call test,compact_size_type_test,utils arch db,boost))
$(eval $(call test,serialize_reconstitute_test,utils arch db,boost))

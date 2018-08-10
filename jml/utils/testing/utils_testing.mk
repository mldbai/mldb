# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,environment_test,utils arch,boost))
$(eval $(call test,string_functions_test,arch utils,boost))

$(eval $(call test,csv_parsing_test,arch utils,boost))

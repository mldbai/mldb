# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,aws_test,aws,boost))
$(eval $(call test,sns_mock_test,aws,boost))


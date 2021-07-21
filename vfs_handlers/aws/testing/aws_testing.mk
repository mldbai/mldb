# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,aws_test,aws arch types value_description,boost))
$(eval $(call test,sns_mock_test,aws arch types value_description http,boost))


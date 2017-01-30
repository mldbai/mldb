# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

$(eval $(call test,statsd_connector_test,opstats,boost  manual))

$(eval $(call test,aws_test,aws,boost))
$(eval $(call test,sns_mock_test,aws services,boost))


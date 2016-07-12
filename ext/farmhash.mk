FARMHASH_SOURCE = src/farmhash.cc

$(FARMHASH_SOURCE):	$(CWD)/src/farmhash.h

FARMHASH_INCLUDE_FILES:=$(CWD)/src/farmhash.h

$(eval $(call library,farmhash,$(FARMHASH_SOURCE)))

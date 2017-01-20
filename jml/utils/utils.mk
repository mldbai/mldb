# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


LIBUTILS_SOURCES := \
        environment.cc \
        file_functions.cc \
        string_functions.cc \
	configuration.cc \
	csv.cc \
	hex_dump.cc \
	floating_point.cc \
	rng.cc

LIBUTILS_LINK :=	arch vfs

$(eval $(call library,utils,$(LIBUTILS_SOURCES),$(LIBUTILS_LINK)))

# gcc 4.7
$(eval $(call set_compile_option,hash.cc,-fpermissive))

$(eval $(call include_sub_make,utils_testing,testing))



# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


LIBBASE_SOURCES := \
        parse_context.cc \
	thread_pool.cc \
	parallel.cc \
	optimized_path.cc \
	hex_dump.cc \
    ../utils/string_functions.cc \
	../utils/csv.cc \


LIBBASE_LINK :=	\
	arch \
	gc

$(eval $(call library,base,$(LIBBASE_SOURCES),$(LIBBASE_LINK)))

# gcc 4.7
$(eval $(call set_compile_option,hash.cc,-fpermissive))

$(eval $(call set_compile_option,hash.cc,-I $(CRYPTOPP_INCLUDE_DIR)))
$(eval $(call library,hash,hash.cc,arch cryptopp))

$(eval $(call include_sub_make,base_testing,testing))


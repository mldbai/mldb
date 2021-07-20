# AWS

LIBAWS_SOURCES := \
	xml_helpers.cc \
	s3.cc \
	sns.cc \
	aws.cc \
	sqs.cc \

LIBAWS_LINK := credentials hash cryptopp tinyxml2 http rest

$(eval $(call library,aws,$(LIBAWS_SOURCES),$(LIBAWS_LINK)))

$(eval $(call library,aws_vfs_handlers,s3_handlers.cc,aws vfs tinyxml2 http))


# gcc 4.7
$(eval $(call set_compile_option,aws.cc,-fpermissive -I $(CRYPTOPP_INCLUDE_DIR)))

$(eval $(call program,s3tee,aws_vfs_handlers boost_program_options utils))
$(eval $(call program,s3cp,aws_vfs_handlers boost_program_options utils))
$(eval $(call program,s3_multipart_cmd,aws_vfs_handlers boost_program_options utils))
$(eval $(call program,s3cat,aws_vfs_handlers boost_program_options utils))


$(eval $(call program,sns_send,aws boost_program_options utils))

$(eval $(call include_sub_make,aws_testing,testing,aws_testing.mk))

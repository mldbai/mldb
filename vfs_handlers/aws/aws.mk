# AWS

LIBAWS_SOURCES := \
	xml_helpers.cc \
	s3.cc \
	sns.cc \
	aws.cc \
	sqs.cc \

LIBAWS_INDIRECT_LINK := arch base value_description types
LIBAWS_LINK := credentials hash cryptopp tinyxml2 http rest io_base utils $(LIBAWS_INDIRECT_LINK)

$(eval $(call library,aws,$(LIBAWS_SOURCES),$(LIBAWS_LINK)))

$(eval $(call library,aws_vfs_handlers,s3_handlers.cc,aws vfs tinyxml2 http $(LIBAWS_INDIRECT_LINK)))


# gcc 4.7
$(eval $(call set_compile_option,aws.cc,-fpermissive -I $(CRYPTOPP_INCLUDE_DIR)))

$(eval $(call program,s3tee,vfs aws arch base aws_vfs_handlers boost_program_options utils))
$(eval $(call program,s3cp,vfs aws arch base types aws_vfs_handlers boost_program_options utils))
$(eval $(call program,s3_multipart_cmd,vfs aws arch base aws_vfs_handlers boost_program_options utils))
$(eval $(call program,s3cat,vfs aws arch base aws_vfs_handlers boost_program_options utils))


$(eval $(call program,sns_send,arch base http vfs aws boost_program_options utils))

$(eval $(call include_sub_make,aws_testing,testing,aws_testing.mk))

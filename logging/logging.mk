$(eval $(call library,logging,logging.cc,types arch))

$(eval $(call include_sub_make,testing))

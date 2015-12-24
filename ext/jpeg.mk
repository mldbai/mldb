# Makefile for the jpeg library

JPEG_SOURCE = jaricom.c jcapimin.c jcapistd.c jcarith.c \
	jccoefct.c jccolor.c jcdctmgr.c jchuff.c jcinit.c \
	jcmainct.c jcmarker.c jcmaster.c jcomapi.c jcparam.c \
	jcprepct.c jcsample.c jctrans.c jdapimin.c jdapistd.c \
	jdarith.c jdatadst.c jdatasrc.c jdcoefct.c jdcolor.c \
	jddctmgr.c jdhuff.c jdinput.c jdmainct.c jdmarker.c \
	jdmaster.c jdmerge.c jdpostct.c jdsample.c jdtrans.c \
	jerror.c jfdctflt.c jfdctfst.c jfdctint.c jidctflt.c \
	jidctfst.c jidctint.c jquant1.c jquant2.c jutils.c \
	jmemmgr.c jmemnobs.c

ifneq ($(PREMAKE),1)
$(CWD)/jconfig.h:
	cd mldb/ext/jpeg && ./configure
endif

$(JPEG_SOURCE):	$(CWD)/jconfig.h

JPEG_INCLUDE_FILES:=$(CWD)/jconfig.h

$(eval $(call library,jpeg,$(JPEG_SOURCE)))

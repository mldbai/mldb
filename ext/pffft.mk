PFFFT_SOURCE = pffft.c fftpack.c

HAS_FFTW3:=$(if $(call seq,$(ARCH),$(HOSTARCH)),$(wildcard /usr/include/fftw3.*))
$(eval $(call set_compile_option,test_pffft.c,$(if $(HAS_FFTW3),-DHAVE_FFTW)))

$(eval $(call library,pffft,$(PFFFT_SOURCE)))
$(eval $(call test,test_pffft,pffft $(if $(HAS_FFTW3),fftw3f),standard,,test_pffft.c))

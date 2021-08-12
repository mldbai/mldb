CRYPTOPP_EXCLUDE_aarch64:=chacha_avx.cpp donna_sse.cpp sse_simd.cpp
CRYPTOPP_EXCLUDE_x86_64:=neon_simd.cpp

CRYPTOPP_SOURCE := cryptlib.cpp cpu.cpp integer.cpp $(filter-out $(CRYPTOPP_EXCLUDE_$(ARCH)) cryptlib.cpp cpu.cpp integer.cpp pch.cpp simple.cpp adhoc.cpp test.cpp bench1.cpp bench2.cpp bench3.cpp datatest.cpp dlltest.cpp fipsalgt.cpp validat0.cpp validat1.cpp validat2.cpp validat3.cpp validat4.cpp validat5.cpp validat6.cpp validat7.cpp validat8.cpp validat9.cpp validat10.cpp regtest1.cpp regtest2.cpp regtest3.cpp regtest4.cpp,$(notdir $(sort $(wildcard $(CWD)/*.cpp))))

CRYPTOPP_ARCH:=$(ARCH)
ifeq ($(ARCH),amd64)
CRYPTOPP_ARCH:=aarch64
endif

ifeq ($(ARCH),x86_64)
$(eval $(call set_compile_option,aria_simd.cpp,-msse3))
$(eval $(call set_compile_option,blake2b_simd.cpp,-msse4.1))
$(eval $(call set_compile_option,blake2s_simd.cpp,-msse4.1))
$(eval $(call set_compile_option,chacha_avx.cpp,-mavx2))
$(eval $(call set_compile_option,chacha_simd.cpp,-msse2))
$(eval $(call set_compile_option,cham_simd.cpp,-msse3))
$(eval $(call set_compile_option,crc_simd.cpp,-msse4.2))
$(eval $(call set_compile_option,donna_sse.cpp,-msse2))
$(eval $(call set_compile_option,gcm_simd.cpp,-msse3 -mpclmul))
$(eval $(call set_compile_option,gf2n_simd.cpp,-mpclmul))
$(eval $(call set_compile_option,keccak_simd.cpp,-msse3))
$(eval $(call set_compile_option,lea_simd.cpp,-msse3))
$(eval $(call set_compile_option,rijndael_simd.cpp,-msse4.1 -maes))
$(eval $(call set_compile_option,sha_simd.cpp,-msse4.2 -msha))
$(eval $(call set_compile_option,shacal2_simd.cpp,-msse4.2 -msha))
$(eval $(call set_compile_option,simon128_simd.cpp,-msse3))
$(eval $(call set_compile_option,sm4_simd.cpp,-msse3 -maes))
$(eval $(call set_compile_option,speck128_simd.cpp,-msse3))
$(eval $(call set_compile_option,sse_simd.cpp,-msse3))
endif

ifeq ($(ARCH),aarch64)
$(eval $(call set_compile_option,aria_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,blake2b_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,blake2s_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,chacha_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,cham_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,crc_simd.cpp,-march=armv8-a+crc))
$(eval $(call set_compile_option,gcm_simd.cpp,-march=armv8-a+crypto))
$(eval $(call set_compile_option,gf2n_simd.cpp,-march=armv8-a+crypto))
$(eval $(call set_compile_option,keccak_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,lea_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,rijndael_simd.cpp,-march=armv8-a+crypto))
$(eval $(call set_compile_option,neon_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,sha_simd.cpp,-march=armv8-a+crypto))
$(eval $(call set_compile_option,shacal2_simd.cpp,-march=armv8-a+crypto))
$(eval $(call set_compile_option,simon128_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,sm4_simd.cpp,-march=armv8-a))
$(eval $(call set_compile_option,speck128_simd.cpp,-march=armv8-a))
endif




$(eval $(call library,cryptopp,$(CRYPTOPP_SOURCE)))
CRYPTOPP_INCLUDE_DIR:=mldb/ext

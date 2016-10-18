DEFAULTGXX:=arm-linux-gnueabihf-g++
DEFAULTGCC:=arm-linux-gnueabihf-gcc
# To explain the extra COMPARE_AND_SWAP arguments, they fix pure
# virtual method and std::thread problems on the Pi.  See here:
# https://groups.google.com/forum/#!msg/automatak-dnp3/Jisp_zGhd5I/ck_Cj6nO8joJ
ARCHFLAGS:=-fPIC -funwind-tables -fno-omit-frame-pointer -funsafe-math-optimizations -mfpu=neon-vfpv4 -ftree-vectorize -Os -I$(BUILD)/$(ARCH)/osdeps/usr/include -I$(BUILD)/$(ARCH)/osdeps/usr/include/arm-linux-gnueabihf $(foreach width,1 2 8,-U__GCC_HAVE_SYNC_COMPARE_AND_SWAP_$(width)) 
PORT_LIBRARY_DIRS := \
	$(BUILD)/$(ARCH)/osdeps/usr/lib/arm-linux-gnueabihf \
	$(BUILD)/$(ARCH)/osdeps/usr/lib \
	$(BUILD)/$(ARCH)/osdeps/lib/arm-linux-gnueabihf \
	$(BUILD)/$(ARCH)/osdeps/usr/lib/lapack \
	$(BUILD)/$(ARCH)/osdeps/usr/lib/libblas

PORT_LINK_FLAGS:=$(foreach dir,$(PORT_LIBRARY_DIRS), -L$(dir) -Wl,--rpath-link,$(dir))

TCMALLOC_ENABLED:=0
port:=ubuntu1404


DEFAULTGXX:=arm-linux-gnueabihf-g++
DEFAULTGCC:=arm-linux-gnueabihf-gcc
ARCHFLAGS:=-fPIC -fno-omit-frame-pointer -I$(BUILD)/$(ARCH)/osdeps/usr/include -I$(BUILD)/$(ARCH)/osdeps/usr/include/arm-linux-gnueabihf
PORT_LIBRARY_DIRS := \
	$(BUILD)/$(ARCH)/osdeps/usr/lib/arm-linux-gnueabihf \
	$(BUILD)/$(ARCH)/osdeps/usr/lib \
	$(BUILD)/$(ARCH)/osdeps/lib/arm-linux-gnueabihf \
	$(BUILD)/$(ARCH)/osdeps/usr/lib/lapack \
	$(BUILD)/$(ARCH)/osdeps/usr/lib/libblas

PORT_LINK_FLAGS:=$(foreach dir,$(PORT_LIBRARY_DIRS), -L$(dir) -Wl,--rpath,$(dir))

TCMALLOC_ENABLED:=0
port:=ubuntu1404


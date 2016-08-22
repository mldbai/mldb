DEFAULTGXX:=aarch64-linux-gnu-g++
DEFAULTGCC:=aarch64-linux-gnu-gcc
ARCHFLAGS:=-fPIC -fno-omit-frame-pointer -I$(BUILD)/$(ARCH)/osdeps/usr/include -I$(BUILD)/$(ARCH)/osdeps/usr/include/aarch64-linux-gnu
PORT_LINK_FLAGS:= -L$(BUILD)/$(ARCH)/osdeps/usr/lib/aarch64-linux-gnu -L$(BUILD)/$(ARCH)/osdeps/usr/lib 
TCMALLOC_ENABLED:=0

PORT_DEV_PACKAGES:=libicu52 libicu-dev libpython2.7-dev openssl openssl-dev libboost-program-options1.54 libboost-filesystem1.54 libboost-program-options1.54-dev libboost-filesystem1.54-dev




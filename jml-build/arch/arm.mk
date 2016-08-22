DEFAULTGXX:=arm-linux-gnueabihf-g++
DEFAULTGCC:=arm-linux-gnueabihf-gcc
ARCHFLAGS:=-fPIC -fno-omit-frame-pointer -I$(BUILD)/$(ARCH)/osdeps/usr/include -I$(BUILD)/$(ARCH)/osdeps/usr/include/arm-linux-gnueabihf
PORT_LINK_FLAGS:= -L$(BUILD)/$(ARCH)/osdeps/usr/include/arm-linux-gnueabihf -L$(BUILD)/$(ARCH)/osdeps/usr/lib 
TCMALLOC_ENABLED:=0


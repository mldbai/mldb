HIGHWAYHASH_SOURCE = highwayhash/scalar_highway_tree_hash.cc highwayhash/sip_hash.cc ../highwayhash.cc

ifeq ($(ARCH),x86_64)
HIGHWAYHASH_SOURCE += highwayhash/sse41_highway_tree_hash.cc highwayhash/highway_tree_hash.cc 
$(eval $(call set_compile_option,highwayhash/sse41_highway_tree_hash.cc,-msse4.1 -I$(CWD)))
$(eval $(call set_compile_option,highwayhash/highway_tree_hash.cc,-mavx2 -I$(CWD)))
endif

$(eval $(call set_compile_option,highwayhash/scalar_highway_tree_hash.cc,-I$(CWD)))
$(eval $(call set_compile_option,highwayhash/sip_hash.cc,-I$(CWD)))
$(eval $(call set_compile_option,../highwayhash.cc,-I$(CWD)))

$(eval $(call library,highwayhash,$(HIGHWAYHASH_SOURCE)))

HIGHWAYHASH_SOURCE = highwayhash/highway_tree_hash.cc highwayhash/sse41_highway_tree_hash.cc highwayhash/scalar_highway_tree_hash.cc highwayhash/sip_hash.cc ../highwayhash.cc

$(eval $(call set_compile_option,highwayhash/highway_tree_hash.cc,-mavx2 -I$(CWD)))
$(eval $(call set_compile_option,highwayhash/sse41_highway_tree_hash.cc,-msse4.1 -I$(CWD)))
$(eval $(call set_compile_option,highwayhash/scalar_highway_tree_hash.cc,-I$(CWD)))
$(eval $(call set_compile_option,highwayhash/sip_hash.cc,-I$(CWD)))
$(eval $(call set_compile_option,../highwayhash.cc,-I$(CWD)))

$(eval $(call library,highwayhash,$(HIGHWAYHASH_SOURCE)))

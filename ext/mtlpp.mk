# Makefile for the mtlpp objc-to-c++ bridge library for Metal (OSX/IOS only)

MTLPP_SOURCE = \
	src/argument.mm \
	src/blit_command_encoder.mm \
	src/buffer.mm \
	src/command_buffer.mm \
	src/command_encoder.mm \
	src/command_queue.mm \
	src/compute_command_encoder.mm \
	src/compute_pipeline.mm \
	src/depth_stencil.mm \
	src/device.mm \
	src/drawable.mm \
	src/fence.mm \
	src/function_constant_values.mm \
	src/heap.mm \
	src/library.mm \
	src/ns.mm \
	src/parallel_render_command_encoder.mm \
	src/render_command_encoder.mm \
	src/render_pass.mm \
	src/render_pipeline.mm \
	src/resource.mm \
	src/sampler.mm \
	src/stage_input_output_descriptor.mm \
	src/texture.mm \
	src/vertex_descriptor.mm \

MTLPP_LINK = \
	Metal \
	CoreFoundation \
	Cocoa \

$(eval $(call library,mtlpp,$(MTLPP_SOURCE),$(MTLPP_LINK)))

LIB_mtlpp_LINKER_OPTIONS+= -fobjc-link-runtime
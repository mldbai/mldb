# NOTE: testing should always be at the end of this list
MLDB_SUBDIRS := \
	ext \
	arch \
	base \
	types \
	watch \
	logging \
	io \
	http \
	vfs \
	block \
	utils \
	credentials \
	rest \
	vfs_handlers \
	sql \
	core \
	engine \
	builtin \
	plugins \
	server \
	sdk \
	testing

$(eval $(call include_sub_makes,$(MLDB_SUBDIRS)))

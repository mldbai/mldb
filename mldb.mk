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
	jml \
	ml \
	utils \
	credentials \
	vfs_handlers \
	rest \
	sql \
	core \
	builtin \
	plugins \
	server \
	sdk \
	html \
	testing

#	postgresql \
#	mongodb \
#	tensorflow \

$(eval $(call include_sub_makes,$(MLDB_SUBDIRS)))

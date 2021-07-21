# Build instructions for svdlibc, the Lancosz solver

SVDLIBC_SOURCES := \
	svdlib.cc \
	svdutil.cc \
	las2.cc

SVDLIBC_LINK :=	m arch base

$(eval $(call library,svdlibc,$(SVDLIBC_SOURCES),$(SVDLIBC_LINK)))


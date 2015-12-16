
MLDB_SUBDIRS := ext arch base types watch http vfs jml soa ml utils rest credentials sql core builtin plugins server sdk tensorflow testing

$(eval $(call include_sub_makes,$(MLDB_SUBDIRS)))

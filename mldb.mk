MLDB_SUBDIRS := ext arch base types watch logging io http vfs jml soa ml vfs_handlers utils rest sql core builtin plugins server sdk tensorflow testing postgresql

$(eval $(call include_sub_makes,$(MLDB_SUBDIRS)))

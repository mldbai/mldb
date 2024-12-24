OSNAME:=$(call exec-shell, uname -s)

include mldb/jml-build/os/$(OSNAME).mk

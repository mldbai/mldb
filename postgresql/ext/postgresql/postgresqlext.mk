# Makefile for postgresql library for MLDB

# postgresql ext
LIBMLDB_POSTGRESQL_EXT_SOURCES:= \
   src/interfaces/libpq/fe-connect.c \
   src/interfaces/libpq/fe-exec.c \
   src/interfaces/libpq/fe-protocol2.c \
   src/interfaces/libpq/fe-protocol3.c \
   src/interfaces/libpq/fe-misc.c \
   src/interfaces/libpq/pqexpbuffer.c \
   src/interfaces/libpq/fe-auth.c \
   src/backend/libpq/ip.c \
   src/backend/utils/mb/encnames.c \
   src/port/strlcpy.c \
   src/port/chklocale.c \
   src/port/noblock.c \
   src/port/pgstrcasecmp.c \
   src/port/pqsignal.c \
   src/port/thread.c \
   src/interfaces/libpq/fe-secure.c \
   src/backend/utils/mb/wchar.c \
   src/backend/libpq/md5.c \

$(eval $(call library,postgresqlext,$(LIBMLDB_POSTGRESQL_EXT_SOURCES)))

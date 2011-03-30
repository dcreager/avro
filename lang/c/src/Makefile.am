# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/jansson/src
AM_CFLAGS=-Wall -Wextra -Wunused-parameter
ACLOCAL_AMFLAGS=-I m4

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = avro-c.pc

nobase_include_HEADERS = avro.h \
avro/allocation.h \
avro/consumer.h \
avro/data.h \
avro/errors.h \
avro/refcount.h \
avro/specific.h \
avro/specific.h.in \
avro/specific.c.in \
avro/specific/array.c.in \
avro/specific/comparison.c.in \
avro/specific/consume.c.in \
avro/specific/done.c.in \
avro/specific/init.c.in \
avro/specific/map.c.in \
avro/specific/resolver.c.in \
avro/specific/union_set.c.in \
avro/specific/accessors.h.in \
avro/specific/comparison.h.in \
avro/specific/consume.h.in \
avro/specific/lifecycle.h.in \
avro/specific/resolver.h.in \
avro/specific/typedef.h.in

lib_LTLIBRARIES = libavro.la
libavro_la_SOURCES = st.c st.h schema.c schema.h schema_equal.c \
schema_specific.c \
datum.c datum_equal.c datum_validate.c datum_read.c datum_skip.c datum_write.c datum_size.c datum.h \
datum_json.c \
array.c map.c memoize.c string.c \
consumer.c resolver.c \
raw_compare.c raw_consume.c raw_resolvers.c \
io.c dump.c dump.h encoding_binary.c \
allocation.c \
avro_private.h encoding.h datafile.c \
errors.c
libavro_la_LIBADD = $(top_builddir)/jansson/src/.libs/libjansson.a
libavro_la_LDFLAGS = \
        -version-info $(LIBAVRO_VERSION) \
        -release $(VERSION) \
        -export-dynamic

bin_PROGRAMS = avrocc
avrocc_SOURCES = avrocc.c
avrocc_LDADD = libavro.la

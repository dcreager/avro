# -pedantic
AM_CPPFLAGS=-I$(top_srcdir)/src
AM_CFLAGS=-Wall -Wextra -Wunused-parameter
ACLOCAL_AMFLAGS=-I m4

BUILT_SOURCES=
CLEANFILES=
EXTRA_DIST=schema_tests test_valgrind

check_PROGRAMS=test_avro_schema test_avro_schema_names test_avro_data test_cpp \
test_data_structures \
test_specific

noinst_PROGRAMS=generate_interop_data test_interop_data performance

test_LDADD=$(top_builddir)/src/libavro.la

test_data_structures_SOURCES=test_data_structures.c
test_data_structures_LDADD=$(test_LDADD)

test_avro_schema_SOURCES=test_avro_schema.c
test_avro_schema_LDADD=$(test_LDADD)

test_avro_schema_names_SOURCES=test_avro_schema_names.c
test_avro_schema_names_LDADD=$(test_LDADD)

test_avro_data_SOURCES=test_avro_data.c
test_avro_data_LDADD=$(test_LDADD)

test_cpp_SOURCES=test_cpp.cpp
test_cpp_LDADD=$(test_LDADD)

generate_interop_data_SOURCES=generate_interop_data.c
generate_interop_data_LDADD=$(test_LDADD)

performance_SOURCES=performance.c
performance_LDADD=$(test_LDADD)

test_interop_data_SOURCES=test_interop_data.c
test_interop_data_LDADD=$(test_LDADD)

test_specific_SOURCES=test_specific.c
nodist_test_specific_SOURCES= \
    specific_list.h specific_list.c specific_list.def
BUILT_SOURCES += $(nodist_test_specific_SOURCES)
CLEANFILES += $(nodist_test_specific_SOURCES)
test_specific_LDADD=$(test_LDADD)
EXTRA_DIST += list.avsc

specific_list.h specific_list.c specific_list.def: list.avsc point.avsc ../src/avrocc
	../src/avrocc -f specific_list -O . -t specific point.avsc list.avsc

# Skip the valgrind test for now; we'll turn this back on once we commit
# a fix for AVRO-766.
#TESTS=$(check_PROGRAMS) test_valgrind

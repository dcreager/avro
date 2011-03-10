/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "avro.h"
#include "specific_list.h"

typedef int (*avro_test) (void);

static void
init_rand(void)
{
	srand(time(NULL));
}

static double
rand_number(double from, double to)
{
	double range = to - from;
	return from + ((double)rand() / (RAND_MAX + 1.0)) * range;
}

static int64_t
rand_long(void)
{
	return (int64_t) rand_number(LONG_MIN, LONG_MAX);
}

static int32_t
rand_int(void)
{
	return (int32_t) rand_number(INT_MIN, INT_MAX);
}

static char buf[4096];

#define write_read_test(datum, val, expected, basename) \
{ \
	avro_reader_t  reader = avro_reader_memory(buf, sizeof(buf)); \
	avro_writer_t  writer = avro_writer_memory(buf, sizeof(buf)); \
	\
	avro_schema_t  wschema = avro_datum_get_schema(datum); \
	\
	if (avro_write_data(writer, NULL, datum)) { \
		fprintf(stderr, "Unable to write: %s\n", \
			avro_strerror()); \
		return EXIT_FAILURE; \
	} \
	\
	avro_consumer_t  *consumer = basename##_resolver_new(wschema); \
	if (!consumer) { \
		fprintf(stderr, "Couldn't create consumer: %s\n", \
			avro_strerror()); \
		return EXIT_FAILURE; \
	} \
	\
	if (avro_consume_binary(reader, consumer, &val)) { \
		fprintf(stderr, "Unable to read: %s\n", \
			avro_strerror()); \
		return EXIT_FAILURE; \
	} \
	\
	if (!basename##_equals(&val, &expected)) { \
		fprintf(stderr, "Roundtrip value doesn't match\n"); \
		return EXIT_FAILURE; \
	} \
	\
	avro_consumer_free(consumer); \
	avro_reader_free(reader); \
	avro_writer_free(writer); \
}

static int
test_lifecycle(void)
{
	specific_list_t  list;
	specific_list_init(&list);

        list.point.x = 5;
        list.point.y = 2;

        memcpy(list.ip.contents, "\xc0\xa8\x00\x01", sizeof(list.ip.contents));

	specific_null_list_set_list(&list.next);
        list.next.branch.list->point.x = 1;
        list.next.branch.list->point.y = 1;

	specific_null_list_set_null(&list.next.branch.list->next);

	specific_list_done(&list);
        return EXIT_SUCCESS;
}

static int
test_raw_boolean(void)
{
	int  i;
	for (i = 0; i < 2; i++) {
		int  value1 = i;
		int  value2 = i;
		if (!avro_raw_boolean_equals(&value1, &value2)) {
			fprintf(stderr, "Values should be equal.\n");
			return EXIT_FAILURE;
		}
		avro_datum_t  datum = avro_boolean(i);
		write_read_test(datum, value1, value2, avro_raw_boolean);
		avro_datum_decref(datum);
	}

	return EXIT_SUCCESS;
}

static int
test_raw_bytes(void)
{
	avro_raw_string_t  str1;
	avro_raw_string_init(&str1);
	avro_raw_string_set_length(&str1, "\xde\xad\xbe\xef", 4);

	avro_raw_string_t  str2;
	avro_raw_string_init(&str2);
	avro_raw_string_set_length(&str2, "\xde\xad\xbe\xef", 4);

	if (!avro_raw_bytes_equals(&str1, &str2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	avro_datum_t  datum = avro_bytes("\xde\xad\xbe\xef", 4);
	write_read_test(datum, str1, str2, avro_raw_bytes);
	avro_datum_decref(datum);

	avro_raw_string_done(&str1);
	avro_raw_string_done(&str2);
	return EXIT_SUCCESS;
}

static int
test_raw_double(void)
{
	int  i;
	for (i = 0; i < 100; i++) {
		double  value1 = rand_number(-1e10, 1e10);
		double  value2 = value1;
		if (!avro_raw_double_equals(&value1, &value2)) {
			fprintf(stderr, "Values should be equal.\n");
			return EXIT_FAILURE;
		}
		avro_datum_t  datum = avro_double(value1);
		write_read_test(datum, value1, value2, avro_raw_double);
		avro_datum_decref(datum);
	}

	return EXIT_SUCCESS;
}

static int
test_raw_float(void)
{
	int  i;
	for (i = 0; i < 100; i++) {
		float  value1 = rand_number(-1e10, 1e10);
		float  value2 = value1;
		if (!avro_raw_float_equals(&value1, &value2)) {
			fprintf(stderr, "Values should be equal.\n");
			return EXIT_FAILURE;
		}
		avro_datum_t  datum = avro_float(value1);
		write_read_test(datum, value1, value2, avro_raw_float);
		avro_datum_decref(datum);
	}

	return EXIT_SUCCESS;
}

static int
test_raw_int(void)
{
	int  i;
	for (i = 0; i < 100; i++) {
		int32_t  value1 = rand_int();
		int32_t  value2 = value1;
		if (!avro_raw_int_equals(&value1, &value2)) {
			fprintf(stderr, "Values should be equal.\n");
			return EXIT_FAILURE;
		}
		avro_datum_t  datum = avro_int32(value1);
		write_read_test(datum, value1, value2, avro_raw_int);
		avro_datum_decref(datum);
	}

	return EXIT_SUCCESS;
}

static int
test_raw_long(void)
{
	int  i;
	for (i = 0; i < 100; i++) {
		int64_t  value1 = rand_long();
		int64_t  value2 = value1;
		if (!avro_raw_long_equals(&value1, &value2)) {
			fprintf(stderr, "Values should be equal.\n");
			return EXIT_FAILURE;
		}
		avro_datum_t  datum = avro_int64(value1);
		write_read_test(datum, value1, value2, avro_raw_long);
		avro_datum_decref(datum);
	}

	return EXIT_SUCCESS;
}

static int
test_raw_null(void)
{
	int  value1 = 0;
	int  value2 = 0;
	if (!avro_raw_null_equals(&value1, &value2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	avro_datum_t  datum = avro_null();
	write_read_test(datum, value1, value2, avro_raw_null);
	avro_datum_decref(datum);

	return EXIT_SUCCESS;
}

static int
test_raw_string(void)
{
	const char *strings[] = { "Four score and seven years ago",
		"our father brought forth on this continent",
		"a new nation", "conceived in Liberty",
		"and dedicated to the proposition that all men are created equal."
	};

	unsigned int  i;
	for (i = 0; i < sizeof(strings) / sizeof(strings[0]); i++) {
		avro_raw_string_t  str1;
		avro_raw_string_init(&str1);
		avro_raw_string_set(&str1, strings[i]);

		avro_raw_string_t  str2;
		avro_raw_string_init(&str2);
		avro_raw_string_set(&str2, strings[i]);

		if (!avro_raw_string_equals(&str1, &str2)) {
			fprintf(stderr, "Values should be equal.\n");
			return EXIT_FAILURE;
		}

		avro_datum_t  datum = avro_string(strings[i]);
		write_read_test(datum, str1, str2, avro_raw_string);
		avro_datum_decref(datum);

		avro_raw_string_done(&str1);
		avro_raw_string_done(&str2);
	}

	return EXIT_SUCCESS;
}

static int
test_array(void)
{
	specific_array_double_t  array;
	specific_array_double_init(&array);

	if (specific_array_double_size(&array) != 0) {
		fprintf(stderr, "Array should start empty.\n");
		return EXIT_FAILURE;
	}

	double  *element = specific_array_double_append(&array);
	if (element == NULL) {
		fprintf(stderr, "Cannot append array element.\n");
		return EXIT_FAILURE;
	}

	*element = 42.0;
	double  *element2 = specific_array_double_get(&array, 0);
	if (element2 == NULL) {
		fprintf(stderr, "Cannot retrieve array element 0.\n");
		return EXIT_FAILURE;
	}

	if (*element != *element2) {
		fprintf(stderr, "Unexpected value for array element 0.\n");
		return EXIT_FAILURE;
	}

	if (specific_array_double_size(&array) != 1) {
		fprintf(stderr, "Array shouldn't be empty after appending.\n");
		return EXIT_FAILURE;
	}

	specific_array_double_t  array2;
	specific_array_double_init(&array2);
	element = specific_array_double_append(&array2);
	*element = 42.0;

	if (!specific_array_double_equals(&array, &array2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	avro_schema_t  schema = specific_array_double_schema();
	avro_datum_t  datum = avro_array(schema);
	avro_datum_t  delement = avro_double(42.0);
	avro_array_append_datum(datum, delement);
	avro_datum_decref(delement);

	write_read_test(datum, array, array2, specific_array_double);

	specific_array_double_clear(&array);
	if (specific_array_double_size(&array) != 0) {
		fprintf(stderr, "Array should be empty after clearing.\n");
		return EXIT_FAILURE;
	}

	avro_schema_decref(schema);
	avro_datum_decref(datum);
	specific_array_double_done(&array);
	specific_array_double_done(&array2);
	return EXIT_SUCCESS;
}

static int
test_enum(void)
{
	specific_scheme_t  value1 = SPECIFIC_SCHEME_RECTANGULAR;
	specific_scheme_t  value2 = SPECIFIC_SCHEME_RECTANGULAR;

	if (!specific_scheme_equals(&value1, &value2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	avro_schema_t  schema = specific_scheme_schema();
	avro_datum_t  datum = avro_enum(schema, 1);

	write_read_test(datum, value1, value2, specific_scheme);

	avro_schema_decref(schema);
	avro_datum_decref(datum);
	return EXIT_SUCCESS;
}

static int
test_fixed(void)
{
	specific_ipv4_t  value1 = {{ 0xDE, 0xAD, 0xBE, 0xEF }};
	specific_ipv4_t  value2 = {{ 0xDE, 0xAD, 0xBE, 0xEF }};

	if (!specific_ipv4_equals(&value1, &value2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	avro_schema_t  schema = specific_ipv4_schema();
	avro_datum_t  datum = avro_fixed(schema, "\xde\xad\xbe\xef", 4);

	write_read_test(datum, value1, value2, specific_ipv4);

	avro_schema_decref(schema);
	avro_datum_decref(datum);
	return EXIT_SUCCESS;
}

static int
test_map(void)
{
	specific_map_string_t  map;
	specific_map_string_init(&map);

	if (specific_map_string_size(&map) != 0) {
		fprintf(stderr, "map should start empty.\n");
		return EXIT_FAILURE;
	}

	avro_raw_string_t  *element = NULL;
	unsigned int  index = 0;

	if (!specific_map_string_get_or_create(&map, "a", &element, &index)) {
		fprintf(stderr, "Cannot append map element.\n");
		return EXIT_FAILURE;
	}

	if (index != 0) {
		fprintf(stderr, "Unexpected index for first map element.\n");
		return EXIT_FAILURE;
	}

	avro_raw_string_set(element, "value");

	avro_raw_string_t  *element2 =
		specific_map_string_get_by_index(&map, 0);
	if (element2 == NULL) {
		fprintf(stderr, "Cannot retrieve map element 0.\n");
		return EXIT_FAILURE;
	}

	if (!avro_raw_string_equals(element, element2)) {
		fprintf(stderr, "Unexpected value for map element 0.\n");
		return EXIT_FAILURE;
	}

	element2 = specific_map_string_get(&map, "a", NULL);
	if (element2 == NULL) {
		fprintf(stderr, "Cannot retrieve map element \"a\".\n");
		return EXIT_FAILURE;
	}

	if (!avro_raw_string_equals(element, element2)) {
		fprintf(stderr, "Unexpected value for map element \"a\".\n");
		return EXIT_FAILURE;
	}

	if (specific_map_string_size(&map) != 1) {
		fprintf(stderr, "map shouldn't be empty after appending.\n");
		return EXIT_FAILURE;
	}

	specific_map_string_t  map2;
	specific_map_string_init(&map2);
	specific_map_string_get_or_create(&map2, "a", &element, NULL);
	avro_raw_string_set(element, "value");

	if (!specific_map_string_equals(&map, &map2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	avro_schema_t  schema = specific_map_string_schema();
	avro_datum_t  datum = avro_map(schema);
	avro_datum_t  delement = avro_string("value");
	avro_map_set(datum, "a", delement);
	avro_datum_decref(delement);

	write_read_test(datum, map, map2, specific_map_string);

	specific_map_string_clear(&map);
	if (specific_map_string_size(&map) != 0) {
		fprintf(stderr, "map should be empty after clearing.\n");
		return EXIT_FAILURE;
	}

	avro_schema_decref(schema);
	avro_datum_decref(datum);
	specific_map_string_done(&map);
	specific_map_string_done(&map2);
	return EXIT_SUCCESS;
}

static int
test_union(void)
{
	specific_null_long_t  nl1;
	specific_null_long_init(&nl1);
	specific_null_long_set_long(&nl1);
	nl1.branch.long_val = 506;

	specific_null_long_t  nl2;
	specific_null_long_init(&nl2);
	specific_null_long_set_long(&nl2);
	nl2.branch.long_val = 506;

	if (!specific_null_long_equals(&nl1, &nl2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	avro_datum_t  datum1 = avro_int64(506);
	write_read_test(datum1, nl1, nl2, specific_null_long);
	avro_datum_decref(datum1);

	specific_null_long_set_null(&nl1);
	specific_null_long_set_null(&nl2);

	avro_datum_t  datum2 = avro_null();
	write_read_test(datum2, nl1, nl2, specific_null_long);
	avro_datum_decref(datum2);

	specific_null_long_done(&nl1);
	specific_null_long_done(&nl2);
	return EXIT_SUCCESS;
}

static int
test_record(void)
{
	specific_point_t  point1;
	specific_point_init(&point1);
	point1.x = 5;
	point1.y = 2;

	specific_point_t  point2;
	specific_point_init(&point2);
	point2.x = 5;
	point2.y = 2;

	if (!specific_point_equals(&point1, &point2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	avro_schema_t  schema = specific_point_schema();
	avro_datum_t  datum = avro_record(schema);
	avro_record_set(datum, "x", avro_int32(5));
	avro_record_set(datum, "y", avro_int32(2));

	write_read_test(datum, point1, point2, specific_point);

	avro_schema_decref(schema);
	avro_datum_decref(datum);
	specific_point_done(&point1);
	specific_point_done(&point2);
	return EXIT_SUCCESS;
}

static int
test_nested(void)
{
	specific_list_t  list1;
	specific_list_init(&list1);
	list1.point.x = 5;
	list1.point.y = 2;
	specific_null_long_set_null(&list1.size);
	memcpy(list1.ip.contents,
	       "\xc0\xa8\x00\x01", sizeof(list1.ip.contents));
	specific_null_list_set_list(&list1.next);
	list1.next.branch.list->point.x = 1;
	list1.next.branch.list->point.y = 1;
	specific_null_long_set_null(&list1.next.branch.list->size);
	specific_null_list_set_null(&list1.next.branch.list->next);

	specific_list_t  list2;
	specific_list_init(&list2);
	list2.point.x = 5;
	list2.point.y = 2;
	specific_null_long_set_null(&list2.size);
	memcpy(list2.ip.contents,
	       "\xc0\xa8\x00\x01", sizeof(list2.ip.contents));
	specific_null_list_set_list(&list2.next);
	list2.next.branch.list->point.x = 1;
	list2.next.branch.list->point.y = 1;
	specific_null_long_set_null(&list2.next.branch.list->size);
	specific_null_list_set_null(&list2.next.branch.list->next);

	if (!specific_list_equals(&list1, &list2)) {
		fprintf(stderr, "Values should be equal.\n");
		return EXIT_FAILURE;
	}

	specific_list_done(&list1);
	specific_list_done(&list2);
	return EXIT_SUCCESS;
}

int main(void)
{
	unsigned int i;
	struct avro_tests {
		char *name;
		avro_test func;
	} tests[] = {
		{ "lifecycle", test_lifecycle },
		{ "raw_boolean", test_raw_boolean },
		{ "raw_bytes", test_raw_bytes },
		{ "raw_double", test_raw_double },
		{ "raw_float", test_raw_float },
		{ "raw_int", test_raw_int },
		{ "raw_long", test_raw_long },
		{ "raw_null", test_raw_null },
		{ "raw_string", test_raw_string },
		{ "array", test_array },
		{ "enum", test_enum },
		{ "fixed", test_fixed },
		{ "map", test_map },
		{ "union", test_union },
		{ "record", test_record },
		{ "nested", test_nested }
	};

	init_rand();

	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		struct avro_tests *test = tests + i;
		fprintf(stderr, "**** Running %s tests ****\n", test->name);
		if (test->func() != 0) {
			return EXIT_FAILURE;
		}
	}
	return EXIT_SUCCESS;
}

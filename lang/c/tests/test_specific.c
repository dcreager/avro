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

static int
test_lifecycle(void)
{
	specific_list_t  list;
	specific_list_init(&list);

        list.point.x = 5;
        list.point.y = 2;

        memcpy(list.ip, "\xc0\xa8\x00\x01", sizeof(list.ip));

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

	specific_array_double_clear(&array);
	if (specific_array_double_size(&array) != 0) {
		fprintf(stderr, "Array should be empty after clearing.\n");
		return EXIT_FAILURE;
	}

	specific_array_double_done(&array);
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

	specific_map_string_clear(&map);
	if (specific_map_string_size(&map) != 0) {
		fprintf(stderr, "map should be empty after clearing.\n");
		return EXIT_FAILURE;
	}

	specific_map_string_done(&map);
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
		{ "map", test_map }
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

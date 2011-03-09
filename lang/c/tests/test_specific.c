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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "specific_list.h"

typedef int (*avro_test) (void);

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

	if (!avro_raw_string_equal(element, element2)) {
		fprintf(stderr, "Unexpected value for map element 0.\n");
		return EXIT_FAILURE;
	}

	element2 = specific_map_string_get(&map, "a", NULL);
	if (element2 == NULL) {
		fprintf(stderr, "Cannot retrieve map element \"a\".\n");
		return EXIT_FAILURE;
	}

	if (!avro_raw_string_equal(element, element2)) {
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
		{ "array", test_array },
		{ "map", test_map }
	};

	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		struct avro_tests *test = tests + i;
		fprintf(stderr, "**** Running %s tests ****\n", test->name);
		if (test->func() != 0) {
			return EXIT_FAILURE;
		}
	}
	return EXIT_SUCCESS;
}

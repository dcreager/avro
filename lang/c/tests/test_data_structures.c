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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro_private.h"
#include "avro/data.h"

static int  result = EXIT_SUCCESS;

typedef int (*avro_test) (void);

static int
test_array(void)
{
	avro_raw_array_t  array;
	long  *element;

	avro_raw_array_init(&array, sizeof(long));
	element = avro_raw_array_append(&array);
	*element = 1;
	element = avro_raw_array_append(&array);
	*element = 3;

	if (avro_raw_array_size(&array) != 2) {
		fprintf(stderr, "Incorrect array size: got %zu, expected %zu.\n",
			(size_t) avro_raw_array_size(&array),
			(size_t) 2);
		return EXIT_FAILURE;
	}

	if (avro_raw_array_get(&array, long, 0) != 1) {
		fprintf(stderr, "Unexpected array element %u: got %ld, expected %ld.\n",
			(unsigned int) 0, avro_raw_array_get(&array, long, 0),
			(long) 1);
		return EXIT_FAILURE;
	}

	avro_raw_array_done(&array);
	return EXIT_SUCCESS;
}


int main(int argc, char *argv[])
{
	AVRO_UNUSED(argc);
	AVRO_UNUSED(argv);

	unsigned int i;
	struct avro_tests {
		char *name;
		avro_test func;
	} tests[] = {
		{ "array", test_array }
	};

	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		struct avro_tests *test = tests + i;
		fprintf(stderr, "**** Running %s tests ****\n", test->name);
		if (test->func() != 0) {
			result = EXIT_FAILURE;
		}
	}
	return result;
}

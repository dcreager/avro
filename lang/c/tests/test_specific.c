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

        list.next.discriminant = 1;
        list.next.branch.list = specific_list_new();

        list.next.branch.list->point.x = 1;
        list.next.branch.list->point.y = 1;

        list.next.branch.list->next.discriminant = 0;

	specific_list_done(&list);
        return EXIT_SUCCESS;
}

int main(void)
{
	unsigned int i;
	struct avro_tests {
		char *name;
		avro_test func;
	} tests[] = {
		{ "lifecycle", test_lifecycle }
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

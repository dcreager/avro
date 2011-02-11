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
#include <time.h>

#include "avro.h"
#include "avro_private.h"

/*
 * A series of performance tests.
 */

typedef void
(*test_func_t)(void);


/**
 * Tests the single-threaded performance of our reference counting
 * mechanism.  We create a single datum, and then reference and
 * deference it many many times.
 */

static void
test_refcount(void)
{
	const unsigned long  NUM_TESTS = 100000000;
	unsigned long  i;

	avro_datum_t  datum = avro_int32(42);
	for (i = 0; i < NUM_TESTS; i++) {
		avro_datum_incref(datum);
		avro_datum_decref(datum);
	}
	avro_datum_decref(datum);
}


#define NUM_RUNS  3

int
main(int argc, char **argv)
{
	AVRO_UNUSED(argc);
	AVRO_UNUSED(argv);

	unsigned int  i;
	struct avro_tests {
		const char  *name;
		test_func_t  func;
	} tests[] = {
		{ "refcount", test_refcount }
	};

	for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
		fprintf(stderr, "**** Running %s ****\n", tests[i].name);
		unsigned int  run;

		double  sum = 0.0;

		for (run = 1; run <= NUM_RUNS; run++) {
			fprintf(stderr, "  Run %u\n", run);

			clock_t  before = clock();
			tests[i].func();
			clock_t  after = clock();
			double  secs = ((double) after-before) / CLOCKS_PER_SEC;
			sum += secs;
		}

		fprintf(stderr, "  Average time: %.03lf seconds\n", sum / NUM_RUNS);
	}

	return EXIT_SUCCESS;
}

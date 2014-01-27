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

#include "avro.h"

#define try(call, msg) \
	do { \
		if (call) { \
			fprintf(stderr, msg ":\n  %s\n", avro_strerror()); \
			return EXIT_FAILURE; \
		} \
	} while (0)

int
main(void)
{
	const char  *filename = "empty.avro";
	avro_schema_t  schema = avro_schema_long();
	avro_file_writer_t  writer;
	avro_file_reader_t  reader;

	/* Delete the file if it exists. */
	remove(filename);

	/* Create an empty data file. */
	try(avro_file_writer_create(filename, schema, &writer),
	    "Error opening file for writing");
	try(avro_file_writer_close(writer),
	    "Error closing file for writing");

	/* Try reading from the file. */
	try(avro_file_reader(filename, &reader),
	    "Error opening file for reading");
	try(avro_file_reader_close(reader),
	    "Error closing file for writing");

	return 0;
}

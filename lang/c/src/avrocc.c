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

#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"

static struct option longopts[] = {
	{ "output-path", required_argument, NULL, 'O' },
	{ "filename-prefix", required_argument, NULL, 'f' },
	{ "type-prefix", required_argument, NULL, 't' },
	{ NULL, 0, NULL, 0 }
};

static void usage(void)
{
	fprintf(stderr,
		"Usage: avrocc [--output-path=<output path>]\n"
		"              [--filename-prefix=<filename>]\n"
		"              [--type-prefix=<prefix>]\n"
		"              <avsc files>\n");
}

int main(int argc, char **argv)
{
	char  *output_path = ".";
	char  *filename = "avro-specific-";
	char  *type_prefix = "avro_specific";

	int  ch;
	while ((ch = getopt_long(argc, argv, "O:f:t:", longopts, NULL)) != -1) {
		switch (ch) {
			case 'O':
				output_path = optarg;
				break;

			case 'f':
				filename = optarg;
				break;

			case 't':
				type_prefix = optarg;
				break;

			default:
				usage();
				exit(1);
		}
	}

	argc -= optind;
	argv += optind;

	if (argc <= 0) {
		fprintf(stderr, "avrocc: Must specify at least one schema file.\n");
		usage();
		exit(1);
	}

	int  i;
	for (i = 0; i < argc; i++) {
		int  rval;
		char  *schema_file = argv[i];

		FILE  *fp = fopen(schema_file, "r");
		if (!fp) {
			fprintf(stderr, "Error processing schema %s:\n  %s\n",
				schema_file, strerror(errno));
			exit(1);
		}

		fseek(fp, 0, SEEK_END);
		long  length = ftell(fp);
		fseek(fp, 0, SEEK_SET);

		char  *json = malloc(length+1);
		if (!json) {
			fprintf(stderr, "Could not read schema JSON:\n  %s\n",
				strerror(ENOMEM));
			fclose(fp);
			exit(1);
		}

		size_t  num_read = fread(json, length, 1, fp);
		if (num_read != 1) {
			fprintf(stderr, "Could not read schema JSON:\n  %s\n",
				strerror(errno));
			free(json);
			fclose(fp);
			exit(1);
		}

		json[length] = '\0';
		fclose(fp);

		avro_schema_t  schema = NULL;
		avro_schema_error_t  error = NULL;
		rval = avro_schema_from_json(json, length, &schema, &error);
		free(json);
		if (rval) {
			fprintf(stderr, "Error parsing schema JSON:\n  %s\n",
				avro_strerror());
			avro_schema_decref(schema);
			exit(1);
		}

		rval = avro_schema_to_specific
		    (schema, output_path, filename, type_prefix);
		if (rval) {
			fprintf(stderr, "Error write schema definition:\n  %s\n",
				avro_strerror());
			avro_schema_decref(schema);
			exit(1);
		}

		avro_schema_decref(schema);
	}
	return 0;
}

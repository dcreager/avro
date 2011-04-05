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

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "avro.h"
#include "avro/allocation.h"
#include "avro/data.h"
#include "avro/errors.h"
#include "avro/specific.h"
#include "avro_private.h"
#include "schema.h"
#include "st.h"


static void
strupcase(char *str)
{
	char  *c;
	for (c = str; *c != '\0'; c++) {
		*c = toupper(*c);
	}
}


#define MAX_RECURSION_DEPTH  64

struct avro_specific_gen_t {
	const char  *filename_prefix;
	const char  *type_prefix;
	char  *upper_type_prefix;

	size_t  def_length;
	char  *def_filename;
	char  *h_filename;
	char  *c_filename;

	avro_writer_t  writer;

	st_table  *started_schemas;

	unsigned int  stack_size;
	avro_schema_t  schema_stack[MAX_RECURSION_DEPTH];
};

static int
write(avro_writer_t writer, const char *fmt, ...)
{
	int  rval;
	char  buf[4096];
	va_list  args;
	unsigned int  len;

	va_start(args, fmt);
	len = vsnprintf(buf, sizeof(buf), fmt, args);
	va_end(args);

	if (len >= sizeof(buf)) {
		avro_set_error("Buffer overflow");
		return EINVAL;
	}

	check(rval, avro_write(writer, buf, len));
	return 0;
}

static void
get_union_name(avro_raw_string_t *dest, avro_schema_t schema)
{
	size_t  num_branches = avro_schema_union_size(schema);
	size_t  i;
	int  first = 1;

	for (i = 0; i < num_branches; i++) {
		if (first) {
			first = 0;
		} else {
			avro_raw_string_append(dest, "_");
		}

		avro_schema_t  branch = avro_schema_union_branch(schema, i);
		const char  *branch_name = avro_schema_type_name(branch);
		avro_raw_string_append(dest, branch_name);
	}
}

static int
write_union_name(avro_specific_gen_t *ctx, avro_schema_t schema)
{
	int  rval;
	avro_raw_string_t  name;
	avro_raw_string_init(&name);
	get_union_name(&name, schema);
	rval = write(ctx->writer, "%s", avro_raw_string_get(&name));
	avro_raw_string_done(&name);
	return rval;
}

static void
get_schema_name(avro_raw_string_t *dest, avro_schema_t schema)
{
	switch (avro_typeof(schema)) {
		case AVRO_ARRAY:
			avro_raw_string_append(dest, "array_");
			get_schema_name(dest, avro_schema_array_items(schema));
			return;

		case AVRO_MAP:
			avro_raw_string_append(dest, "map_");
			get_schema_name(dest, avro_schema_map_values(schema));
			return;

		case AVRO_UNION:
			get_union_name(dest, schema);
			return;

		default:
			avro_raw_string_append
			    (dest, avro_schema_type_name(schema));
			return;
	}
}

static int
write_array_map_name(avro_specific_gen_t *ctx, avro_schema_t schema)
{
	int  rval;
	avro_raw_string_t  name;
	avro_raw_string_init(&name);
	get_schema_name(&name, schema);
	rval = write(ctx->writer, "%s", avro_raw_string_get(&name));
	avro_raw_string_done(&name);
	return rval;
}

/**
 * Outputs a reference to the definition of an Avro schema.
 */

static int
avro_schema_type_ref(avro_specific_gen_t *ctx, avro_schema_t schema)
{
	int  rval;
	switch (avro_typeof(schema)) {
		case AVRO_ARRAY:
			{
				avro_schema_t  items =
				    avro_schema_array_items(schema);
				check(rval, write(ctx->writer, "array, "));
				check(rval, write_array_map_name(ctx, items));
				return 0;
			}

		case AVRO_ENUM:
			{
				const char  *type_name = avro_schema_name(schema);
				check(rval, write(ctx->writer, "enum, %s",
						  type_name));
				return 0;
			}

		case AVRO_FIXED:
			{
				const char  *type_name = avro_schema_name(schema);
				check(rval, write(ctx->writer, "fixed, %s",
						  type_name));
				return 0;
			}

		case AVRO_MAP:
			{
				avro_schema_t  items =
				    avro_schema_map_values(schema);
				check(rval, write(ctx->writer, "map, "));
				check(rval, write_array_map_name(ctx, items));
				return 0;
			}

		case AVRO_RECORD:
			{
				/*
				 * If the schema that's being referred
				 * to is on the current schema stack,
				 * then we have a recursive reference.
				 */

				const char  *type_name = avro_schema_name(schema);
				const char  *reference_type = "record";
				unsigned int  i;

				for (i = 0; i < ctx->stack_size; i++) {
					if (ctx->schema_stack[i] == schema) {
						reference_type = "recursive";
					}
				}

				check(rval, write(ctx->writer, "%s, %s",
						  reference_type, type_name));
				return 0;
			}

		case AVRO_UNION:
			check(rval, write(ctx->writer, "union, "));
			check(rval, write_union_name(ctx, schema));
			return 0;

		case AVRO_LINK:
			{
				avro_schema_t  target =
				    avro_schema_link_target(schema);
				return avro_schema_type_ref(ctx, target);
			}

		default:
			check(rval, write(ctx->writer, "%s, _",
					  avro_schema_type_name(schema)));
			return 0;
	}
}


/**
 * Outputs a definition header file for an Avro schema.  We make a
 * recursive call to ensure that the definitions for any child schemas
 * will be written before they're referred to.
 */

static int
avro_schema_write_def(avro_specific_gen_t *ctx, avro_schema_t schema)
{
	int  rval;

	/*
	 * If this is a linked schema, just immediately process the
	 * link's target.
	 */

	if (is_avro_link(schema)) {
		avro_schema_t  target = avro_schema_link_target(schema);
		return avro_schema_write_def(ctx, target);
	}

	/*
	 * If we've already started processing this schema, just return.
	 * (We might be in the middle of processing the schema, if the
	 * schema is recursive.)
	 */

	avro_raw_string_t  name;
	avro_raw_string_init(&name);
	get_schema_name(&name, schema);
	const char  *schema_name = avro_raw_string_get(&name);

	if (st_lookup(ctx->started_schemas, (st_data_t) schema_name, NULL)) {
		avro_raw_string_done(&name);
		return 0;
	}

	/*
	 * Add this schema to the started set, so that we don't try to
	 * process it twice.
	 */

	if (ctx->stack_size == MAX_RECURSION_DEPTH) {
		avro_raw_string_done(&name);
		avro_set_error("Exceeded schema recursion depth");
		return EINVAL;
	}

	schema_name = avro_strdup(avro_raw_string_get(&name));
	avro_raw_string_done(&name);
	fprintf(stderr, "Outputting definitions for %s...\n", schema_name);

	ctx->schema_stack[ctx->stack_size++] = schema;
	st_insert(ctx->started_schemas,
		  (st_data_t) schema_name, (st_data_t) NULL);

	/*
	 * Output forward declarations before recursing.
	 */

	switch (avro_typeof(schema)) {
		case AVRO_ARRAY:
			{
				avro_schema_t  items =
				    avro_schema_array_items(schema);
				check(rval, write(ctx->writer,
				      "  \\\n  AVRO_FORWARD(%s, array, ",
				      ctx->type_prefix));
				check(rval, write_array_map_name(ctx, items));
				check(rval, write(ctx->writer, ") \\\n"));
				break;
			}

		case AVRO_ENUM:
			check(rval, write(ctx->writer,
			      "  \\\n  AVRO_FORWARD(%s, enum, %s) \\\n",
			      ctx->type_prefix, avro_schema_name(schema)));
			break;

		case AVRO_FIXED:
			check(rval, write(ctx->writer,
			      "  \\\n  AVRO_FORWARD(%s, fixed, %s) \\\n",
			      ctx->type_prefix, avro_schema_name(schema)));
			break;

		case AVRO_MAP:
			{
				avro_schema_t  items =
				    avro_schema_map_values(schema);
				check(rval, write(ctx->writer,
				      "  \\\n  AVRO_FORWARD(%s, map, ",
				      ctx->type_prefix));
				check(rval, write_array_map_name(ctx, items));
				check(rval, write(ctx->writer, ") \\\n"));
				break;
			}

		case AVRO_RECORD:
			check(rval, write(ctx->writer,
			      "  \\\n  AVRO_FORWARD(%s, record, %s) \\\n",
			      ctx->type_prefix, avro_schema_name(schema)));
			break;

		case AVRO_UNION:
			check(rval, write(ctx->writer,
			      "  \\\n  AVRO_FORWARD(%s, union, ",
			      ctx->type_prefix));
			check(rval, write_union_name(ctx, schema));
			check(rval, write(ctx->writer, ") \\\n"));
			break;

		default:
			break;
	}

	/*
	 * Recurse through the children of this schema.
	 */

	switch (avro_typeof(schema)) {
		case AVRO_ARRAY:
			{
				avro_schema_t  child =
				    avro_schema_array_items(schema);
				check(rval, avro_schema_write_def(ctx, child));
				break;
			}

		case AVRO_MAP:
			{
				avro_schema_t  child =
				    avro_schema_map_values(schema);
				check(rval, avro_schema_write_def(ctx, child));
				break;
			}

		case AVRO_RECORD:
			{
				size_t  num_fields =
				    avro_schema_record_size(schema);
				unsigned int  i;

				for (i = 0; i < num_fields; i++) {
					avro_schema_t  child =
					    avro_schema_record_field_get_by_index
					    (schema, i);
					check(rval, avro_schema_write_def(ctx, child));
				}
				break;
			}

		case AVRO_UNION:
			{
				size_t  num_branches =
				    avro_schema_union_size(schema);
				unsigned int  i;

				for (i = 0; i < num_branches; i++) {
					avro_schema_t  child =
					    avro_schema_union_branch(schema, i);
					check(rval, avro_schema_write_def(ctx, child));
				}
				break;
			}

		default:
			break;
	}

	/*
	 * Output the definition of this schema once all of its children
	 * have been written.
	 */

	switch (avro_typeof(schema)) {
		case AVRO_ARRAY:
			{
				avro_schema_t  items =
				    avro_schema_array_items(schema);

				check(rval, write(ctx->writer,
				      "  \\\n  AVRO_ARRAY(%s, ",
				      ctx->type_prefix));
				check(rval, write_array_map_name(ctx, items));
				check(rval, write(ctx->writer, ", "));
				check(rval, avro_schema_type_ref(ctx, items));
				check(rval, write(ctx->writer, ") \\\n"));
				break;
			}

		case AVRO_ENUM:
			{
				const char  *enum_name = avro_schema_name(schema);

				check(rval, write(ctx->writer,
				      "  \\\n  AVRO_ENUM_START(%s, %s) \\\n",
				      ctx->type_prefix, enum_name));

				char  *upper_enum_name = strdup(enum_name);
				strupcase(upper_enum_name);

				size_t  num_symbols =
				    avro_schema_enum_size(schema);
				unsigned int  i;
				for (i = 0; i < num_symbols; i++) {
					check(rval, write(ctx->writer,
					      "  AVRO_ENUM_SYMBOL(%s, %s, %s, %u,"
					      " %u, %u) \\\n",
					      ctx->upper_type_prefix,
					      upper_enum_name,
					      avro_schema_enum_get(schema, i),
					      i, (i == 0), (i == num_symbols-1)));
				}

				check(rval, write(ctx->writer,
				      "  AVRO_ENUM_END(%s, %s) \\\n",
				      ctx->type_prefix, enum_name));

				free(upper_enum_name);
				break;
			}

		case AVRO_FIXED:
			{
				check(rval, write(ctx->writer,
				      "  \\\n  AVRO_FIXED(%s, %s, %lu) \\\n",
				      ctx->type_prefix,
				      avro_schema_name(schema),
				      (unsigned long) avro_schema_fixed_size(schema)));
				break;
			}

		case AVRO_MAP:
			{
				avro_schema_t  items =
				    avro_schema_map_values(schema);

				check(rval, write(ctx->writer,
				      "  \\\n  AVRO_MAP(%s, ",
				      ctx->type_prefix));
				check(rval, write_array_map_name(ctx, items));
				check(rval, write(ctx->writer, ", "));
				check(rval, avro_schema_type_ref(ctx, items));
				check(rval, write(ctx->writer, ") \\\n"));
				break;
			}

		case AVRO_RECORD:
			{
				const char  *record_name = avro_schema_name(schema);

				check(rval, write(ctx->writer,
				      "  \\\n  AVRO_RECORD_START(%s, %s) \\\n",
				      ctx->type_prefix, record_name));

				size_t  num_fields =
				    avro_schema_record_size(schema);
				unsigned int  i;
				for (i = 0; i < num_fields; i++) {
					const char  *field_name =
					    avro_schema_record_field_name
					    (schema, i);
					avro_schema_t  field =
					    avro_schema_record_field_get_by_index
					    (schema, i);

					check(rval, write(ctx->writer,
					      "  AVRO_RECORD_FIELD(%s, %s, %u, %s, ",
					      ctx->type_prefix, record_name,
					      i, field_name));
					check(rval, avro_schema_type_ref(ctx, field));
					check(rval, write(ctx->writer,
					      ", %u, %u) \\\n",
					      (i == 0), (i == num_fields-1)));
				}

				check(rval, write(ctx->writer,
				      "  AVRO_RECORD_END(%s, %s) \\\n",
				      ctx->type_prefix, record_name));
				break;
			}

		case AVRO_UNION:
			{
				check(rval, write(ctx->writer,
				      "  \\\n  AVRO_UNION_START(%s, ",
				      ctx->type_prefix));
				check(rval, write_union_name(ctx, schema));
				check(rval, write(ctx->writer, ") \\\n"));

				size_t  num_branches =
				    avro_schema_union_size(schema);
				unsigned int  i;
				for (i = 0; i < num_branches; i++) {
					avro_schema_t  branch =
					    avro_schema_union_branch(schema, i);

					check(rval, write(ctx->writer,
					      "  AVRO_UNION_BRANCH(%s, ",
					      ctx->type_prefix));
					check(rval, write_union_name(ctx, schema));
					check(rval, write(ctx->writer, ", %u, ", i));
					check(rval, avro_schema_type_ref(ctx, branch));
					check(rval, write(ctx->writer,
					      ", %u, %u) \\\n",
					      i, (i == 0), (i == num_branches-1)));
				}

				check(rval, write(ctx->writer,
				      "  AVRO_UNION_END(%s, ",
				      ctx->type_prefix));
				check(rval, write_union_name(ctx, schema));
				check(rval, write(ctx->writer, ") \\\n"));
				break;
			}

		default:
			break;
	}

	/*
	 * Pop this schema off the stack before returning.
	 */

	ctx->stack_size--;
	return 0;
}


#define check_out(rval, call) { rval = call; if (rval) goto error; }

avro_specific_gen_t *
avro_specific_gen_open(const char *output_path,
		       const char *filename_prefix,
		       const char *type_prefix)
{
	int  rval;
	FILE  *fp;

	avro_specific_gen_t  *ctx = avro_new(avro_specific_gen_t);
	if (!ctx) {
		return NULL;
	}
	memset(ctx, 0, sizeof(avro_specific_gen_t));

	ctx->type_prefix = type_prefix;
	ctx->upper_type_prefix = avro_strdup(ctx->type_prefix);
	if (!ctx->upper_type_prefix) {
		goto error;
	}
	strupcase(ctx->upper_type_prefix);

	/* Create filename strings */

	ctx->filename_prefix = filename_prefix;
	ctx->def_length =
	    snprintf(NULL, 0, "%s/%s.def",
		     output_path, filename_prefix);

	ctx->def_filename = avro_malloc(ctx->def_length+1);
	if (!ctx->def_filename) {
		goto error;
	}

	ctx->h_filename = avro_malloc(ctx->def_length+1-2);
	if (!ctx->h_filename) {
		goto error;
	}

	ctx->c_filename = avro_malloc(ctx->def_length+1-2);
	if (!ctx->c_filename) {
		goto error;
	}

	snprintf(ctx->def_filename, ctx->def_length+1,
		 "%s/%s.def",
		 output_path, filename_prefix);
	snprintf(ctx->h_filename, ctx->def_length+1-2,
		 "%s/%s.h",
		 output_path, filename_prefix);
	snprintf(ctx->c_filename, ctx->def_length+1-2,
		 "%s/%s.c",
		 output_path, filename_prefix);

	/* Definition file */

	fp = fopen(ctx->def_filename, "w");
	if (!fp) {
		avro_set_error(strerror(errno));
		goto error;
	}

	ctx->writer = avro_writer_file(fp);
	check_out(rval, write(ctx->writer,
		  "/* Autogenerated file.  Do not edit! */\n\n"
		  "#define SCHEMA_DEFINITION() \\\n"));
	ctx->started_schemas = st_init_strtable();
	ctx->stack_size = 0;

	return ctx;

error:
	if (ctx) {
		if (ctx->def_filename) {
			avro_free(ctx->def_filename, ctx->def_length+1);
		}
		if (ctx->h_filename) {
			avro_free(ctx->h_filename, ctx->def_length+1-2);
		}
		if (ctx->c_filename) {
			avro_free(ctx->c_filename, ctx->def_length+1-2);
		}
		if (ctx->upper_type_prefix) {
			avro_str_free(ctx->upper_type_prefix);
		}
		avro_freet(avro_specific_gen_t, ctx);
	}

	return NULL;
}


int
avro_specific_gen_output_schema(avro_specific_gen_t *ctx,
				avro_schema_t schema)
{
	check_param(EINVAL, is_avro_schema(schema), "schema");
	return avro_schema_write_def(ctx, schema);
}


static int
started_schemas_free_foreach(char *key, void *value, void *arg)
{
	AVRO_UNUSED(arg);
	AVRO_UNUSED(value);
	avro_str_free(key);
	return ST_DELETE;
}


int
avro_specific_gen_close(avro_specific_gen_t *ctx)
{
	int  rval = 0;
	FILE  *fp;

	st_foreach(ctx->started_schemas, started_schemas_free_foreach, 0);
	st_free_table(ctx->started_schemas);

	check_out(rval,
		  write(ctx->writer, "  /* end of schema definition */\n"));
	avro_writer_free(ctx->writer);

	/* Header file */

	fp = fopen(ctx->h_filename, "w");
	if (!fp) {
		avro_set_error(strerror(errno));
		rval = errno;
		goto error;
	}

	ctx->writer = avro_writer_file(fp);
	check_out(rval, write(ctx->writer,
		  "/* Autogenerated file.  Do not edit! */\n\n"
		  "#include \"%s.def\"\n"
		  "#include <avro/specific.h.in>\n"
		  "#undef SCHEMA_DEFINITION\n",
		  ctx->filename_prefix));
	avro_writer_free(ctx->writer);

	/* Source file */

	fp = fopen(ctx->c_filename, "w");
	if (!fp) {
		avro_set_error(strerror(errno));
		rval = errno;
		goto error;
	}

	ctx->writer = avro_writer_file(fp);
	check_out(rval, write(ctx->writer,
		  "/* Autogenerated file.  Do not edit! */\n\n"
		  "#include \"%s.h\"\n"
		  "#include \"%s.def\"\n"
		  "#include <avro/specific.c.in>\n"
		  "#undef SCHEMA_DEFINITION\n",
		  ctx->filename_prefix,
		  ctx->filename_prefix));

	rval = 0;

error:
	if (ctx->writer) {
		avro_writer_free(ctx->writer);
	}
	if (ctx->def_filename) {
		avro_free(ctx->def_filename, ctx->def_length+1);
	}
	if (ctx->h_filename) {
		avro_free(ctx->h_filename, ctx->def_length+1-2);
	}
	if (ctx->c_filename) {
		avro_free(ctx->c_filename, ctx->def_length+1-2);
	}
	if (ctx->upper_type_prefix) {
		avro_str_free(ctx->upper_type_prefix);
	}
	avro_freet(avro_specific_gen_t, ctx);
	return rval;
}


int avro_schema_to_specific(avro_schema_t schema,
			    const char *output_path,
			    const char *filename_prefix,
			    const char *type_prefix)
{
	check_param(EINVAL, is_avro_schema(schema), "schema");

	int  rval;

	char  real_prefix[1024];
	snprintf(real_prefix, sizeof(real_prefix),
		 "%s%s", filename_prefix, avro_schema_type_name(schema));

	avro_specific_gen_t  *ctx =
	    avro_specific_gen_open(output_path, real_prefix, type_prefix);
	if (!ctx) {
		return EINVAL;
	}

	check_out(rval, avro_specific_gen_output_schema(ctx, schema));
	return avro_specific_gen_close(ctx);

error:
	avro_specific_gen_close(ctx);
	return rval;
}
